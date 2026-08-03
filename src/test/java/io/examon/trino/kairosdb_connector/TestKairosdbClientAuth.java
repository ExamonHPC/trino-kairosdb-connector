package io.examon.trino.kairosdb_connector;

import io.trino.spi.TrinoException;
import okhttp3.Credentials;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verifies the HTTP Basic Auth wiring of {@link KairosdbClient} against an
 * in-process {@link MockWebServer}: the {@code Authorization} header is sent
 * on every request when {@code kairosdb.auth.*} is configured, absent when it
 * is not, and a 401 from the server surfaces as a clean {@link TrinoException}.
 */
final class TestKairosdbClientAuth
{
    private static final String USER = "kairos-user";
    private static final String PASSWORD = "kairos-pass";

    private static final String METRIC_NAMES_BODY = "{\"results\":[\"sys.load\"]}";
    private static final String QUERY_BODY =
            "{\"queries\":[{\"results\":[{\"name\":\"sys.load\",\"values\":[],\"tags\":{}}]}]}";

    private MockWebServer server;

    @BeforeEach
    void startServer() throws IOException
    {
        server = new MockWebServer();
        server.start();
    }

    @AfterEach
    void stopServer() throws IOException
    {
        server.shutdown();
    }

    @Test
    void metricNamesRequestCarriesBasicAuthHeader() throws InterruptedException
    {
        server.enqueue(jsonResponse(METRIC_NAMES_BODY));
        KairosdbClient client = clientWithAuth();

        client.listMetricNames();

        RecordedRequest recorded = server.takeRequest();
        assertThat(recorded.getHeader("Authorization")).isEqualTo(Credentials.basic(USER, PASSWORD));
    }

    @Test
    void queryDatapointsRequestCarriesBasicAuthHeader() throws InterruptedException
    {
        server.enqueue(jsonResponse(QUERY_BODY));
        KairosdbClient client = clientWithAuth();

        client.queryDatapoints("sys.load", 0, 1_000, Map.of(), Optional.empty(), List.of());

        RecordedRequest recorded = server.takeRequest();
        assertThat(recorded.getHeader("Authorization")).isEqualTo(Credentials.basic(USER, PASSWORD));
    }

    @Test
    void noAuthorizationHeaderWhenAuthNotConfigured() throws InterruptedException
    {
        server.enqueue(jsonResponse(METRIC_NAMES_BODY));
        KairosdbClient client = clientFor(new KairosdbConfig().setKairosdbUri(server.url("/").uri()));

        client.listMetricNames();

        RecordedRequest recorded = server.takeRequest();
        assertThat(recorded.getHeader("Authorization")).isNull();
    }

    @Test
    void unauthorizedResponseSurfacesAsTrinoException()
    {
        server.enqueue(new MockResponse().setResponseCode(401));
        KairosdbClient client = clientWithAuth();

        assertThatThrownBy(client::listMetricNames)
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("401");
    }

    private static MockResponse jsonResponse(String body)
    {
        return new MockResponse()
                .setHeader("Content-Type", "application/json")
                .setBody(body);
    }

    private KairosdbClient clientWithAuth()
    {
        return clientFor(new KairosdbConfig()
                .setKairosdbUri(server.url("/").uri())
                .setAuthUser(USER)
                .setAuthPassword(PASSWORD));
    }

    /**
     * Real client against the mock server; tag discovery is stubbed out so
     * {@code queryDatapoints} exercises only the datapoint-query endpoint
     * (the tag-lookback probe loop is out of scope here).
     */
    private static KairosdbClient clientFor(KairosdbConfig config)
    {
        return new KairosdbClient(config) {
            @Override
            public List<String> getOriginalTagKeys(String metricName)
            {
                return List.of();
            }
        };
    }
}
