package io.examon.trino.kairosdb_connector;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.airlift.units.Duration;
import io.trino.spi.connector.ColumnMetadata;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.PullPolicy;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end integration test against a real KairosDB container.
 *
 * <p>Boots {@code examonhpc/kairosdb:1.2.2} in the in-process H2 datastore mode
 * (same flag the dev-loop docker-compose uses), writes a small known dataset
 * via the raw HTTP API, then drives every public {@link KairosdbClient}
 * surface to verify the connector talks to KairosDB correctly.
 *
 * <p>Opt in with {@code mvn -Pintegration verify} (the surefire config excludes
 * the {@code integration} JUnit tag by default).  Inside the docker-based
 * builder, run it via {@code scripts/test-integration.sh}, which mounts the
 * docker socket so Testcontainers can spawn containers on the host engine.
 */
@Tag("integration")
final class ITKairosdbClient
{
    private static final String IMAGE = "examonhpc/kairosdb:1.2.2";
    private static final int KAIROS_PORT = 8080;

    // A self-contained tiny dataset.  All timestamps are absolute ms from a
    // fixed anchor so the assertions are deterministic.  We pick "now" so
    // the tag-discovery scan (which looks back from now in 30-day windows)
    // finds the metric on its very first probe.
    private static final long BASE_MILLIS = System.currentTimeMillis() - 60_000L;
    private static final String METRIC_LOAD = "sys.load";
    // A second, mixed-case metric to verify we preserve casing.
    private static final String METRIC_MEM = "Sys.Mem";

    private static GenericContainer<?> kairos;
    private static KairosdbClient client;
    private static OkHttpClient http;
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final MediaType JSON_TYPE = MediaType.get("application/json; charset=utf-8");

    @BeforeAll
    static void startKairosdbAndSeed() throws Exception
    {
        // Flip the active datastore module to H2 the same way docker-compose
        // does, so the container is fully self-contained (no Cassandra).
        // The image's entrypoint runs /usr/bin/config-kairos.sh; we wrap it
        // with an in-place sed.
        String bootCommand =
                "sed -i " +
                        "-e 's|^kairosdb.service.datastore=org.kairosdb.datastore.cassandra.CassandraModule|#&|' " +
                        "-e 's|^#kairosdb.service.datastore=org.kairosdb.datastore.h2.H2Module|kairosdb.service.datastore=org.kairosdb.datastore.h2.H2Module|' " +
                        "/opt/kairosdb/conf/kairosdb.properties && exec /usr/bin/config-kairos.sh";

        kairos = new GenericContainer<>(IMAGE)
                .withImagePullPolicy(PullPolicy.defaultPolicy())
                .withEnv("KAIROS_JETTY_PORT", String.valueOf(KAIROS_PORT))
                .withCreateContainerCmdModifier(cmd -> cmd
                        .withEntrypoint("/bin/sh", "-c")
                        .withCmd(bootCommand))
                .withExposedPorts(KAIROS_PORT)
                // Wait for Jetty to be ready to accept TCP connections.
                // The standard /api/v1/health/check endpoint returns 204
                // (not 200) on some image variants, which trips
                // Wait.forHttp().forStatusCode(200) - using TCP+poll instead
                // sidesteps the response-code ambiguity, and seedDatapoints()
                // below does a real HTTP poll right after for full readiness.
                .waitingFor(Wait.forListeningPort()
                        .withStartupTimeout(java.time.Duration.ofMinutes(3)));
        kairos.start();

        URI uri = URI.create(String.format("http://%s:%d", kairos.getHost(), kairos.getMappedPort(KAIROS_PORT)));

        http = new OkHttpClient.Builder()
                .connectTimeout(5, TimeUnit.SECONDS)
                .readTimeout(10, TimeUnit.SECONDS)
                .build();

        seedDatapoints(uri);

        KairosdbConfig config = new KairosdbConfig();
        config.setKairosdbUri(uri);
        config.setMetadataCacheTtl(new Duration(1, MINUTES));
        config.setReadTimeout(new Duration(10, java.util.concurrent.TimeUnit.SECONDS));
        client = new KairosdbClient(config);
    }

    @AfterAll
    static void stopKairos()
    {
        if (kairos != null) {
            kairos.stop();
        }
    }

    @Test
    void listsBothMetricsCasePreserving()
    {
        List<String> names = client.listMetricNames();
        // KairosDB may surface internal bookkeeping metrics; we only assert
        // ours are present in their original casing.
        assertThat(names).contains(METRIC_LOAD, METRIC_MEM);
    }

    @Test
    void resolveTableNamePrefersExactCaseThenFallsBackInsensitively()
    {
        // The connector's documented default is to try exact-case first and
        // then fall back to case-insensitive matching (the long-running
        // production behaviour, configurable via
        // kairosdb.case-insensitive-name-matching).  Both queries below
        // therefore resolve to the original casing recorded by KairosDB.
        assertThat(client.resolveTableName(METRIC_LOAD)).contains(METRIC_LOAD);
        assertThat(client.resolveTableName(METRIC_MEM)).contains(METRIC_MEM);
        assertThat(client.resolveTableName("sys.mem")).contains(METRIC_MEM);
        assertThat(client.resolveTableName("does.not.exist")).isEmpty();
    }

    @Test
    void columnsExposeLowercaseTagsPlusSyntheticAndHidden()
    {
        List<ColumnMetadata> columns = client.getColumns(METRIC_LOAD);
        List<String> names = columns.stream().map(ColumnMetadata::getName).toList();

        // Trino's SPI lowercases ColumnMetadata names; we keep the original
        // casing internally through getOriginalTagKeys().
        assertThat(names).contains("host", "zone", "timestamp", "value", "sampling_aggregator");

        ColumnMetadata samplingAggregator = columns.stream()
                .filter(c -> c.getName().equals("sampling_aggregator"))
                .findFirst()
                .orElseThrow();
        assertThat(samplingAggregator.isHidden()).isTrue();
    }

    @Test
    void originalTagKeysAreCasePreserving()
    {
        // The seed data uses lowercase tag keys so we just assert they're
        // both present; the casing-preservation logic is exercised in
        // unit tests.
        assertThat(client.getOriginalTagKeys(METRIC_LOAD))
                .containsExactlyInAnyOrder("host", "zone");
    }

    @Test
    void queryReturnsAllDatapointsAndTagsInWindow()
    {
        List<KairosdbResponses.QueryDatapointsResponse.DataResult> results = client.queryDatapoints(
                METRIC_LOAD,
                BASE_MILLIS - 1,
                BASE_MILLIS + 1_000_000,
                Map.of(),
                Optional.empty(),
                List.of());

        int totalDatapoints = results.stream().mapToInt(r -> r.values.size()).sum();
        assertThat(totalDatapoints).isEqualTo(5);

        // Every result must carry the group_by tags so per-row tag values
        // are correct (this is the bug we fixed earlier by adding group_by
        // for all original tag keys).
        for (KairosdbResponses.QueryDatapointsResponse.DataResult r : results) {
            assertThat(r.tags).containsKey("host");
            assertThat(r.tags).containsKey("zone");
        }
    }

    @Test
    void queryHonoursTagFilterPushdown()
    {
        List<KairosdbResponses.QueryDatapointsResponse.DataResult> results = client.queryDatapoints(
                METRIC_LOAD,
                BASE_MILLIS - 1,
                BASE_MILLIS + 1_000_000,
                Map.of("host", List.of("h1")),
                Optional.empty(),
                List.of());

        int totalDatapoints = results.stream().mapToInt(r -> r.values.size()).sum();
        assertThat(totalDatapoints).isEqualTo(3);

        for (KairosdbResponses.QueryDatapointsResponse.DataResult r : results) {
            assertThat(r.tags.get("host")).containsExactly("h1");
        }
    }

    @Test
    void queryHonoursLimitPushdown()
    {
        List<KairosdbResponses.QueryDatapointsResponse.DataResult> results = client.queryDatapoints(
                METRIC_LOAD,
                BASE_MILLIS - 1,
                BASE_MILLIS + 1_000_000,
                Map.of(),
                Optional.of(2L),
                List.of());

        int totalDatapoints = results.stream().mapToInt(r -> r.values.size()).sum();
        // KairosDB's "limit" is per-result-group; with two host groups we
        // expect <= 2 per group, but we just assert the cap held.
        assertThat(totalDatapoints).isLessThanOrEqualTo(2 * 2);
        // And of course at least one - the dataset is non-empty.
        assertThat(totalDatapoints).isGreaterThan(0);
    }

    @Test
    void queryWithAggregatorReturnsFewerDatapointsThanRaw()
    {
        // Raw query: 3 datapoints for host=h1.
        List<KairosdbResponses.QueryDatapointsResponse.DataResult> raw = client.queryDatapoints(
                METRIC_LOAD,
                BASE_MILLIS - 1,
                BASE_MILLIS + 1_000_000,
                Map.of("host", List.of("h1")),
                Optional.empty(),
                List.of());
        int rawCount = raw.stream().mapToInt(r -> r.values.size()).sum();

        // Bucket every minute - all three h1 datapoints fall in the same
        // minute window, so the aggregator collapses them to a single row.
        // Spec format is documented in KairosdbSamplingConstants:
        // 'type;<digits>[smhd]?;{start_time|end_time|sampling|none}'.
        List<KairosdbResponses.QueryDatapointsResponse.DataResult> aggregated = client.queryDatapoints(
                METRIC_LOAD,
                BASE_MILLIS - 1,
                BASE_MILLIS + 1_000_000,
                Map.of("host", List.of("h1")),
                Optional.empty(),
                List.of("sum;1m;start_time"));

        int aggregatedCount = aggregated.stream().mapToInt(r -> r.values.size()).sum();
        assertThat(aggregatedCount).isLessThan(rawCount);
        assertThat(aggregatedCount).isGreaterThan(0);
    }

    /**
     * Seeds the deterministic dataset:
     * <pre>
     *   sys.load  host=h1 zone=us-east  t,   t+10s, t+20s   ->  1.0, 2.5, 3.7
     *   sys.load  host=h2 zone=us-east  t,         t+20s    ->  5.0,      6.0
     *   Sys.Mem   host=h1               t                   ->  42
     * </pre>
     */
    private static void seedDatapoints(URI baseUri) throws Exception
    {
        List<Map<String, Object>> payload = List.of(
                Map.of(
                        "name", METRIC_LOAD,
                        "tags", Map.of("host", "h1", "zone", "us-east"),
                        "datapoints", List.of(
                                List.of(BASE_MILLIS, 1.0),
                                List.of(BASE_MILLIS + 10_000, 2.5),
                                List.of(BASE_MILLIS + 20_000, 3.7))),
                Map.of(
                        "name", METRIC_LOAD,
                        "tags", Map.of("host", "h2", "zone", "us-east"),
                        "datapoints", List.of(
                                List.of(BASE_MILLIS, 5.0),
                                List.of(BASE_MILLIS + 20_000, 6.0))),
                Map.of(
                        "name", METRIC_MEM,
                        "tags", Map.of("host", "h1"),
                        "datapoints", List.of(
                                List.of(BASE_MILLIS, 42))));

        String body = JSON.writeValueAsString(payload);
        Request request = new Request.Builder()
                .url(baseUri.resolve("/api/v1/datapoints").toString())
                .post(RequestBody.create(body, JSON_TYPE))
                .build();
        try (Response response = http.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                String resp = response.body() != null ? response.body().string() : "(no body)";
                throw new AssertionError("Failed to seed KairosDB: HTTP " + response.code() + " - " + resp);
            }
        }

        // KairosDB returns 204 No Content as soon as the points are queued.
        // They become queryable a tick later; poll briefly so the metadata
        // probes below find them deterministically.
        long deadline = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < deadline) {
            Request listing = new Request.Builder()
                    .url(baseUri.resolve("/api/v1/metricnames").toString())
                    .get()
                    .build();
            try (Response response = http.newCall(listing).execute()) {
                String text = response.body() != null ? response.body().string() : "";
                if (text.contains(METRIC_LOAD) && text.contains(METRIC_MEM)) {
                    return;
                }
            }
            Thread.sleep(250);
        }
        throw new AssertionError("KairosDB never surfaced the seeded metrics");
    }
}
