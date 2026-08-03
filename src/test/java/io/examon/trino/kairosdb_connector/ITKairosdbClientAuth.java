package io.examon.trino.kairosdb_connector;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.airlift.units.Duration;
import io.trino.spi.TrinoException;
import okhttp3.Credentials;
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
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * End-to-end HTTP Basic Auth test against a real KairosDB container with
 * Jetty basic auth <em>enabled</em> ({@code kairosdb.jetty.basic_auth.user}/
 * {@code .password} uncommented via the same sed trick the no-auth
 * {@link ITKairosdbClient} uses to flip the datastore to H2).
 *
 * <p>Kept separate from {@link ITKairosdbClient} on purpose: that class
 * doubles as the no-auth regression check, and folding auth into its shared
 * container would force credentials into every data-path test.  This class
 * boots one lean auth-enabled container and checks the three things that
 * matter: correct credentials round-trip, wrong credentials get 401, and
 * missing credentials get 401 (which also proves the server really enforces
 * auth, keeping the positive test honest).
 *
 * <p>Opt in with {@code mvn -Pintegration verify}, or via
 * {@code scripts/test-integration.sh} inside the docker-based builder.
 */
@Tag("integration")
final class ITKairosdbClientAuth
{
    private static final String IMAGE = "examonhpc/kairosdb:1.2.2";
    private static final int KAIROS_PORT = 8080;

    private static final String AUTH_USER = "kairos-it-user";
    private static final String AUTH_PASSWORD = "kairos-it-secret";

    private static final long BASE_MILLIS = System.currentTimeMillis() - 60_000L;
    private static final String METRIC = "auth.probe";

    private static GenericContainer<?> kairos;
    private static URI baseUri;
    private static OkHttpClient http;
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final MediaType JSON_TYPE = MediaType.get("application/json; charset=utf-8");

    @BeforeAll
    static void startAuthEnabledKairosdbAndSeed() throws Exception
    {
        // Same in-place patch ITKairosdbClient applies (H2 datastore), plus
        // two expressions uncommenting Jetty's basic-auth credentials.
        String bootCommand =
                "sed -i " +
                        "-e 's|^kairosdb.service.datastore=org.kairosdb.datastore.cassandra.CassandraModule|#&|' " +
                        "-e 's|^#kairosdb.service.datastore=org.kairosdb.datastore.h2.H2Module|kairosdb.service.datastore=org.kairosdb.datastore.h2.H2Module|' " +
                        "-e 's|^#kairosdb.jetty.basic_auth.user=.*|kairosdb.jetty.basic_auth.user=" + AUTH_USER + "|' " +
                        "-e 's|^#kairosdb.jetty.basic_auth.password=.*|kairosdb.jetty.basic_auth.password=" + AUTH_PASSWORD + "|' " +
                        "/opt/kairosdb/conf/kairosdb.properties && exec /usr/bin/config-kairos.sh";

        kairos = new GenericContainer<>(IMAGE)
                .withImagePullPolicy(PullPolicy.defaultPolicy())
                .withEnv("KAIROS_JETTY_PORT", String.valueOf(KAIROS_PORT))
                .withCreateContainerCmdModifier(cmd -> cmd
                        .withEntrypoint("/bin/sh", "-c")
                        .withCmd(bootCommand))
                .withExposedPorts(KAIROS_PORT)
                // TCP wait is auth-agnostic (an HTTP wait would itself need
                // the Authorization header); seedDatapoints() polls over
                // authenticated HTTP right after for full readiness.
                .waitingFor(Wait.forListeningPort()
                        .withStartupTimeout(java.time.Duration.ofMinutes(3)));
        kairos.start();

        baseUri = URI.create(String.format("http://%s:%d", kairos.getHost(), kairos.getMappedPort(KAIROS_PORT)));

        http = new OkHttpClient.Builder()
                .connectTimeout(5, TimeUnit.SECONDS)
                .readTimeout(10, TimeUnit.SECONDS)
                .build();

        seedDatapoints();
    }

    @AfterAll
    static void stopKairos()
    {
        if (kairos != null) {
            kairos.stop();
        }
    }

    @Test
    void authenticatedRoundTripListsAndQueriesMetrics()
    {
        KairosdbClient client = clientFor(AUTH_USER, AUTH_PASSWORD);

        assertThat(client.listMetricNames()).contains(METRIC);

        List<KairosdbResponses.QueryDatapointsResponse.DataResult> results = client.queryDatapoints(
                METRIC,
                BASE_MILLIS - 1,
                BASE_MILLIS + 1_000_000,
                Map.of(),
                Optional.empty(),
                List.of());
        int totalDatapoints = results.stream().mapToInt(r -> r.values.size()).sum();
        assertThat(totalDatapoints).isEqualTo(2);
    }

    @Test
    void wrongPasswordFailsWithHttp401()
    {
        KairosdbClient client = clientFor(AUTH_USER, "definitely-wrong");

        assertThatThrownBy(client::listMetricNames)
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("401");
    }

    /**
     * Also guards the whole class against a silently non-enforcing server:
     * if the sed patch ever stopped taking effect, this test would fail and
     * expose {@link #authenticatedRoundTripListsAndQueriesMetrics} as vacuous.
     */
    @Test
    void missingCredentialsFailWithHttp401()
    {
        KairosdbClient client = clientFor(null, null);

        assertThatThrownBy(client::listMetricNames)
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("401");
    }

    private static KairosdbClient clientFor(String user, String password)
    {
        KairosdbConfig config = new KairosdbConfig()
                .setKairosdbUri(baseUri)
                .setMetadataCacheTtl(new Duration(1, MINUTES))
                .setReadTimeout(new Duration(10, SECONDS))
                .setAuthUser(user)
                .setAuthPassword(password);
        return new KairosdbClient(config);
    }

    /**
     * Seeds one tiny metric (two datapoints) over authenticated raw HTTP -
     * with basic auth enabled, the seeding POST and the readiness poll must
     * both carry the Authorization header or this method would hang until
     * the deadline and fail confusingly.
     */
    private static void seedDatapoints() throws Exception
    {
        List<Map<String, Object>> payload = List.of(
                Map.of(
                        "name", METRIC,
                        "tags", Map.of("host", "h1"),
                        "datapoints", List.of(
                                List.of(BASE_MILLIS, 1.0),
                                List.of(BASE_MILLIS + 10_000, 2.0))));

        String body = JSON.writeValueAsString(payload);
        Request request = authenticated(new Request.Builder()
                .url(baseUri.resolve("/api/v1/datapoints").toString())
                .post(RequestBody.create(body, JSON_TYPE)))
                .build();
        try (Response response = http.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                String resp = response.body() != null ? response.body().string() : "(no body)";
                throw new AssertionError("Failed to seed KairosDB: HTTP " + response.code() + " - " + resp);
            }
        }

        // Points are queued on 204; poll until the metric is listable so the
        // tests below never race ingestion.
        long deadline = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < deadline) {
            Request listing = authenticated(new Request.Builder()
                    .url(baseUri.resolve("/api/v1/metricnames").toString())
                    .get())
                    .build();
            try (Response response = http.newCall(listing).execute()) {
                String text = response.body() != null ? response.body().string() : "";
                if (response.isSuccessful() && text.contains(METRIC)) {
                    return;
                }
            }
            Thread.sleep(250);
        }
        throw new AssertionError("KairosDB never surfaced the seeded metric");
    }

    private static Request.Builder authenticated(Request.Builder builder)
    {
        return builder.header("Authorization", Credentials.basic(AUTH_USER, AUTH_PASSWORD));
    }
}
