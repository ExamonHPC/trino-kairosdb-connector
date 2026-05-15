package io.examon.trino.kairosdb_connector;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.airlift.configuration.testing.ConfigAssertions.assertDeprecatedEquivalence;
import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

/**
 * Locks in the {@link KairosdbConfig} property contract: default values,
 * every new {@code kairosdb.*} key, and the legacy bare keys we still
 * accept for backwards compatibility with the original connector's
 * catalog files.
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
                .setCaseInsensitiveNameMatching(true));
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
                .buildOrThrow();

        KairosdbConfig expected = new KairosdbConfig()
                .setKairosdbUri(URI.create("http://kairosdb-test:8080"))
                .setSplitSize(new Duration(15, TimeUnit.MINUTES))
                .setTimestampFormat(KairosdbConfig.TimestampFormat.TIMESTAMP_TZ)
                .setDefaultStartHours(48)
                .setMetadataCacheTtl(new Duration(5, TimeUnit.MINUTES))
                .setReadTimeout(new Duration(30, TimeUnit.SECONDS))
                .setCaseInsensitiveNameMatching(false);

        assertFullMapping(properties, expected);
    }

    /**
     * Catalog files written for earlier releases of this connector used
     * bare property names (no {@code kairosdb.} prefix).  They keep
     * working because the new keys are annotated with {@code @LegacyConfig}.
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
}
