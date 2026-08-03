package io.examon.trino.kairosdb_connector;

import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.ConfigurationFactory;
import io.airlift.units.Duration;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.airlift.configuration.testing.ConfigAssertions.assertDeprecatedEquivalence;
import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Locks in the {@link KairosdbConfig} property contract: default values,
 * every {@code kairosdb.*} key, and the legacy bare-key aliases accepted
 * for backwards compatibility.
 */
final class TestKairosdbConfig
{
    @Test
    void defaults()
    {
        assertRecordedDefaults(recordDefaults(KairosdbConfig.class)
                .setKairosdbUri(null)
                .setSplitSize(new Duration(1, TimeUnit.DAYS))
                .setTimestampFormat(KairosdbConfig.TimestampFormat.BIGINT)
                .setDefaultStartHours(1)
                .setMetadataCacheTtl(new Duration(30, TimeUnit.SECONDS))
                .setReadTimeout(new Duration(60, TimeUnit.SECONDS))
                .setCaseInsensitiveNameMatching(true)
                .setAuthUser(null)
                .setAuthPassword(null));
    }

    @Test
    void explicitMapping()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("kairosdb.url", "http://kairosdb-test:8080")
                .put("kairosdb.split-size", "15m")
                .put("kairosdb.timestamp.format", "TIMESTAMP_TZ")
                .put("kairosdb.timestamp.default-start-hours", "48")
                .put("kairosdb.metadata.cache-ttl", "5m")
                .put("kairosdb.read-timeout", "30s")
                .put("kairosdb.case-insensitive-name-matching", "false")
                .put("kairosdb.auth.user", "kairos-user")
                .put("kairosdb.auth.password", "kairos-pass")
                .buildOrThrow();

        KairosdbConfig expected = new KairosdbConfig()
                .setKairosdbUri(URI.create("http://kairosdb-test:8080"))
                .setSplitSize(new Duration(15, TimeUnit.MINUTES))
                .setTimestampFormat(KairosdbConfig.TimestampFormat.TIMESTAMP_TZ)
                .setDefaultStartHours(48)
                .setMetadataCacheTtl(new Duration(5, TimeUnit.MINUTES))
                .setReadTimeout(new Duration(30, TimeUnit.SECONDS))
                .setCaseInsensitiveNameMatching(false)
                .setAuthUser("kairos-user")
                .setAuthPassword("kairos-pass");

        assertFullMapping(properties, expected);
    }

    /**
     * Verifies that the legacy bare property names (no {@code kairosdb.}
     * prefix) resolve to the same configuration as their prefixed
     * counterparts via {@code @LegacyConfig}.
     */
    @Test
    void legacyKeysStillResolve()
    {
        Map<String, String> currentProperties = ImmutableMap.<String, String>builder()
                .put("kairosdb.url", "http://kairosdb-test:8080")
                .put("kairosdb.split-size", "1d")
                .put("kairosdb.timestamp.format", "TIMESTAMP_MILLIS")
                .put("kairosdb.timestamp.default-start-hours", "3")
                .buildOrThrow();

        Map<String, String> legacyProperties = ImmutableMap.<String, String>builder()
                .put("url", "http://kairosdb-test:8080")
                .put("split.size.millis", "1d")
                .put("timestamp.format", "TIMESTAMP_MILLIS")
                .put("timestamp.default.start.hours", "3")
                .buildOrThrow();

        assertDeprecatedEquivalence(KairosdbConfig.class, currentProperties, legacyProperties);
    }

    @Test
    void authUserWithoutPasswordIsRejected()
    {
        assertConfigFails(
                ImmutableMap.of(
                        "kairosdb.url", "http://kairosdb-test:8080",
                        "kairosdb.auth.user", "kairos-user"),
                "must be set together");
    }

    @Test
    void authPasswordWithoutUserIsRejected()
    {
        assertConfigFails(
                ImmutableMap.of(
                        "kairosdb.url", "http://kairosdb-test:8080",
                        "kairosdb.auth.password", "kairos-pass"),
                "must be set together");
    }

    @Test
    void colonInAuthUserIsRejected()
    {
        assertConfigFails(
                ImmutableMap.of(
                        "kairosdb.url", "http://kairosdb-test:8080",
                        "kairosdb.auth.user", "kairos:user",
                        "kairosdb.auth.password", "kairos-pass"),
                "must not contain ':'");
    }

    /**
     * Credentials embedded in the URL are silently ignored by OkHttp (no
     * Authorization header is ever derived from them) and would leak the
     * password into logged URLs, so the config rejects them outright and
     * points at the explicit properties.
     */
    @Test
    void userInfoInUrlIsRejected()
    {
        assertConfigFails(
                ImmutableMap.of("kairosdb.url", "http://user:pass@kairosdb-test:8080"),
                "must not embed credentials");
    }

    @Test
    void bothCredentialsTogetherAreAccepted()
    {
        KairosdbConfig config = new ConfigurationFactory(ImmutableMap.of(
                "kairosdb.url", "http://kairosdb-test:8080",
                "kairosdb.auth.user", "kairos-user",
                "kairosdb.auth.password", "kairos-pass"))
                .build(KairosdbConfig.class);

        assertThat(config.getAuthUser()).contains("kairos-user");
        assertThat(config.getAuthPassword()).contains("kairos-pass");
    }

    /**
     * Loads the properties through airlift's real configuration pipeline so
     * the {@code @AssertTrue} bean-validation methods fire exactly as they
     * would at catalog load, and asserts the failure message.
     */
    private static void assertConfigFails(Map<String, String> properties, String expectedMessageFragment)
    {
        ConfigurationFactory factory = new ConfigurationFactory(properties);
        assertThatThrownBy(() -> factory.build(KairosdbConfig.class))
                .hasMessageContaining(expectedMessageFragment);
    }
}
