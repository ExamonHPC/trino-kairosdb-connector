package io.examon.trino.kairosdb_connector;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.LegacyConfig;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

import java.net.URI;
import java.util.concurrent.TimeUnit;

/**
 * Connector configuration.
 *
 * <p>Each property accepts both the modern {@code kairosdb.*} key and the
 * unprefixed legacy key that earlier releases of this plugin used.  Legacy
 * aliases are recognised via airlift's {@link LegacyConfig} so existing
 * catalog files keep working without modification.
 */
public class KairosdbConfig
{
    public enum TimestampFormat
    {
        /** Epoch milliseconds rendered as a SQL {@code BIGINT}. */
        BIGINT,
        /** Local timestamp without offset, milliseconds precision. */
        TIMESTAMP_MILLIS,
        /** Timestamp with time zone, milliseconds precision. */
        TIMESTAMP_TZ,
    }

    private URI kairosdbUri;
    private Duration splitSize = new Duration(1, TimeUnit.DAYS);
    private TimestampFormat timestampFormat = TimestampFormat.BIGINT;
    private int defaultStartHours = 1;
    private Duration metadataCacheTtl = new Duration(30, TimeUnit.SECONDS);
    private Duration readTimeout = new Duration(60, TimeUnit.SECONDS);
    private boolean caseInsensitiveNameMatching = true;

    @NotNull
    public URI getKairosdbUri()
    {
        return kairosdbUri;
    }

    @Config("kairosdb.url")
    @LegacyConfig("url")
    @ConfigDescription("Base URL of the KairosDB HTTP API, for example http://kairosdb:8080")
    public KairosdbConfig setKairosdbUri(URI kairosdbUri)
    {
        this.kairosdbUri = kairosdbUri;
        return this;
    }

    @NotNull
    @MinDuration("1ms")
    public Duration getSplitSize()
    {
        return splitSize;
    }

    @Config("kairosdb.split-size")
    @LegacyConfig("split.size.millis")
    @ConfigDescription("Width of one time-range split (KairosDB query chunk)")
    public KairosdbConfig setSplitSize(Duration splitSize)
    {
        this.splitSize = splitSize;
        return this;
    }

    @NotNull
    public TimestampFormat getTimestampFormat()
    {
        return timestampFormat;
    }

    @Config("kairosdb.timestamp.format")
    @LegacyConfig("timestamp.format")
    @ConfigDescription("SQL type used for the synthetic timestamp column: BIGINT (epoch ms), TIMESTAMP_MILLIS (no zone, millisecond precision), or TIMESTAMP_TZ (TIMESTAMP(3) WITH TIME ZONE in UTC)")
    public KairosdbConfig setTimestampFormat(TimestampFormat timestampFormat)
    {
        this.timestampFormat = timestampFormat;
        return this;
    }

    @Min(1)
    public int getDefaultStartHours()
    {
        return defaultStartHours;
    }

    @Config("kairosdb.timestamp.default-start-hours")
    @LegacyConfig("timestamp.default.start.hours")
    @ConfigDescription("Default look-back window (hours) when a query does not constrain the timestamp")
    public KairosdbConfig setDefaultStartHours(int defaultStartHours)
    {
        this.defaultStartHours = defaultStartHours;
        return this;
    }

    @NotNull
    @MinDuration("1s")
    public Duration getMetadataCacheTtl()
    {
        return metadataCacheTtl;
    }

    @Config("kairosdb.metadata.cache-ttl")
    @ConfigDescription("How long metric-name and tag-column metadata is cached on each worker")
    public KairosdbConfig setMetadataCacheTtl(Duration metadataCacheTtl)
    {
        this.metadataCacheTtl = metadataCacheTtl;
        return this;
    }

    @NotNull
    @MinDuration("1s")
    public Duration getReadTimeout()
    {
        return readTimeout;
    }

    @Config("kairosdb.read-timeout")
    @ConfigDescription("HTTP read timeout for calls to KairosDB")
    public KairosdbConfig setReadTimeout(Duration readTimeout)
    {
        this.readTimeout = readTimeout;
        return this;
    }

    public boolean isCaseInsensitiveNameMatching()
    {
        return caseInsensitiveNameMatching;
    }

    /**
     * Controls table-name resolution when Trino's parser hands the connector
     * a lowercased identifier (the usual case for unquoted SQL).  KairosDB
     * is case-sensitive; Trino's SPI is not (see
     * https://github.com/trinodb/trino/issues/17), so the two sides have to
     * be reconciled somewhere.
     *
     * <p>When true (default) the connector first looks for an exact-case
     * match against the KairosDB-side metric list and, failing that, falls
     * back to a case-insensitive match.  This matches the long-running
     * production behaviour: both {@code SELECT * FROM "MyMetric"} and
     * {@code SELECT * FROM mymetric} resolve to the KairosDB metric named
     * {@code MyMetric}.
     *
     * <p>When false only exact-case matches are accepted, which makes
     * sense if two metrics differ only in case (rare) or for a stricter
     * SQL-standard catalog experience.
     *
     * <p>Column (tag) names are <em>not</em> affected by this flag: Trino's
     * SPI forces them to lowercase unconditionally.  The connector always
     * keeps the KairosDB-side original case internally so the data sent to
     * and parsed from KairosDB is always faithful.
     */
    @Config("kairosdb.case-insensitive-name-matching")
    @ConfigDescription("Resolve table names case-insensitively after first trying exact-case match")
    public KairosdbConfig setCaseInsensitiveNameMatching(boolean caseInsensitiveNameMatching)
    {
        this.caseInsensitiveNameMatching = caseInsensitiveNameMatching;
        return this;
    }
}
