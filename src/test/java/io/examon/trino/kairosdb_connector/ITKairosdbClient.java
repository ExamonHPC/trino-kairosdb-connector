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
    // A second, mixed-case metric to verify we preserve casing on the
    // wire and lowercase the SQL identifier.
    private static final String METRIC_MEM = "Sys.Mem";
    // A case-collision pair: the connector must expose BOTH under hash-
    // mangled Trino-side names rather than letting Trino's lowercasing
    // silently shadow one with the other.  Three variants exercise the
    // grouping logic in the unit test; two variants are enough at the
    // integration layer to verify the round-trip from KairosDB through
    // the client.
    private static final String METRIC_COLLIDE_LOWER = "pue";
    private static final String METRIC_COLLIDE_UPPER = "Pue";

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
    void listMetricNamesReturnsTrinoSideLowercaseAndMangledCollisions()
    {
        List<String> names = client.listMetricNames();
        // Singletons appear as their lowercase form (Trino-side).
        assertThat(names).contains(METRIC_LOAD, "sys.mem");

        // Collision group: BOTH variants must appear, each as a distinct
        // mangled name.  Neither the unmangled "pue" nor the mixed-case
        // "Pue" should leak through directly.
        String mangledLower = "pue__" + KairosdbMetricView.hashSuffix(METRIC_COLLIDE_LOWER);
        String mangledUpper = "pue__" + KairosdbMetricView.hashSuffix(METRIC_COLLIDE_UPPER);
        assertThat(names).contains(mangledLower, mangledUpper);
        assertThat(names).doesNotContain("pue");
    }

    @Test
    void resolveTableNameMapsTrinoSideIdentifiersToKairosOriginals()
    {
        // Singleton: lowercase form wins, returns the KairosDB original.
        assertThat(client.resolveTableName(METRIC_LOAD)).contains(METRIC_LOAD);
        assertThat(client.resolveTableName("sys.mem")).contains(METRIC_MEM);

        // Collision group: only the mangled forms resolve; the bare
        // lowercase form does NOT, which is how the user gets a clean
        // "table not found" instead of silent wrong data.
        String mangledLower = "pue__" + KairosdbMetricView.hashSuffix(METRIC_COLLIDE_LOWER);
        String mangledUpper = "pue__" + KairosdbMetricView.hashSuffix(METRIC_COLLIDE_UPPER);
        assertThat(client.resolveTableName(mangledLower)).contains(METRIC_COLLIDE_LOWER);
        assertThat(client.resolveTableName(mangledUpper)).contains(METRIC_COLLIDE_UPPER);
        assertThat(client.resolveTableName("pue")).isEmpty();

        // Unrelated.
        assertThat(client.resolveTableName("does.not.exist")).isEmpty();
    }

    @Test
    void mangledMetricsCarryDescriptiveTableComment()
    {
        // The comment is what surfaces the original case via
        // SHOW CREATE TABLE / system.metadata.table_comments, so it has
        // to mention the original name verbatim and only fire for
        // collision-group members.
        assertThat(client.getTableComment(METRIC_LOAD)).isEmpty();
        assertThat(client.getTableComment(METRIC_MEM)).isEmpty();
        assertThat(client.getTableComment(METRIC_COLLIDE_LOWER))
                .hasValueSatisfying(s -> assertThat(s).contains("\"" + METRIC_COLLIDE_LOWER + "\""));
        assertThat(client.getTableComment(METRIC_COLLIDE_UPPER))
                .hasValueSatisfying(s -> assertThat(s).contains("\"" + METRIC_COLLIDE_UPPER + "\""));
    }

    @Test
    void caseCollidedMetricsReturnTheirOwnDistinctData()
    {
        // The whole point of the rule: each mangled SQL identifier
        // addresses exactly one underlying KairosDB metric and never
        // accidentally returns rows from its case-twin.  Pue has 1
        // datapoint; pue has 2.  If the connector ever conflated them the
        // counts below would not match.
        List<KairosdbResponses.QueryDatapointsResponse.DataResult> lowerResults = client.queryDatapoints(
                METRIC_COLLIDE_LOWER,
                BASE_MILLIS - 1,
                BASE_MILLIS + 1_000_000,
                Map.of(),
                Optional.empty(),
                List.of());
        int lowerCount = lowerResults.stream().mapToInt(r -> r.values.size()).sum();

        List<KairosdbResponses.QueryDatapointsResponse.DataResult> upperResults = client.queryDatapoints(
                METRIC_COLLIDE_UPPER,
                BASE_MILLIS - 1,
                BASE_MILLIS + 1_000_000,
                Map.of(),
                Optional.empty(),
                List.of());
        int upperCount = upperResults.stream().mapToInt(r -> r.values.size()).sum();

        assertThat(lowerCount).isEqualTo(2);
        assertThat(upperCount).isEqualTo(1);
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
     *   pue       host=h1               t,   t+10s          ->  1.0, 2.0   (case-collision: 2 datapoints)
     *   Pue       host=h1               t                   ->  9.0        (case-collision: 1 datapoint)
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
                                List.of(BASE_MILLIS, 42))),
                Map.of(
                        "name", METRIC_COLLIDE_LOWER,
                        "tags", Map.of("host", "h1"),
                        "datapoints", List.of(
                                List.of(BASE_MILLIS, 1.0),
                                List.of(BASE_MILLIS + 10_000, 2.0))),
                Map.of(
                        "name", METRIC_COLLIDE_UPPER,
                        "tags", Map.of("host", "h1"),
                        "datapoints", List.of(
                                List.of(BASE_MILLIS, 9.0))));

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
        // probes below find them deterministically.  We wait until both
        // the singletons AND the colliding pair are visible so the
        // collision tests don't race the listing.
        long deadline = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < deadline) {
            Request listing = new Request.Builder()
                    .url(baseUri.resolve("/api/v1/metricnames").toString())
                    .get()
                    .build();
            try (Response response = http.newCall(listing).execute()) {
                String text = response.body() != null ? response.body().string() : "";
                if (text.contains(METRIC_LOAD)
                        && text.contains(METRIC_MEM)
                        && text.contains(METRIC_COLLIDE_LOWER)
                        && text.contains(METRIC_COLLIDE_UPPER)) {
                    return;
                }
            }
            Thread.sleep(250);
        }
        throw new AssertionError("KairosDB never surfaced the seeded metrics");
    }
}
