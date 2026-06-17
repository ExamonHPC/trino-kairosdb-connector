package io.examon.trino.kairosdb_connector;

import io.trino.spi.type.BigintType;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import org.junit.jupiter.api.Test;

import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DateTimeEncoding.unpackZoneKey;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * How {@link KairosdbRecordCursor#packTimestamp} renders the KairosDB-returned
 * epoch-ms-UTC instant for each {@code timestamp.format}. The key property for
 * {@code TIMESTAMP_TZ}: the stored instant is unchanged, but the display zone is
 * the session zone (so {@code SET TIME ZONE} localizes the result), matching
 * Trino built-ins like {@code from_unixtime}.
 */
final class TestKairosdbTimestampEncoding
{
    /** A fixed UTC instant: 2025-06-15T16:26:40Z. */
    private static final long INSTANT_MS = 1_750_004_800_000L;
    private static final TimeZoneKey ROME = TimeZoneKey.getTimeZoneKey("Europe/Rome");

    @Test
    void bigintReturnsRawEpochMillis()
    {
        assertThat(KairosdbRecordCursor.packTimestamp(BigintType.BIGINT, INSTANT_MS, ROME))
                .isEqualTo(INSTANT_MS);
    }

    @Test
    void timestampMillisIsMicrosAndIgnoresZone()
    {
        // Zone-less TIMESTAMP(3): micros since epoch, session zone irrelevant.
        assertThat(KairosdbRecordCursor.packTimestamp(TimestampType.TIMESTAMP_MILLIS, INSTANT_MS, ROME))
                .isEqualTo(INSTANT_MS * 1_000L);
    }

    @Test
    void timestampTzKeepsInstantAndUsesSessionZone()
    {
        long packed = KairosdbRecordCursor.packTimestamp(
                TimestampWithTimeZoneType.createTimestampWithTimeZoneType(3), INSTANT_MS, ROME);
        // Same instant ...
        assertThat(unpackMillisUtc(packed)).isEqualTo(INSTANT_MS);
        // ... rendered in the session zone (so the client shows local time).
        assertThat(unpackZoneKey(packed)).isEqualTo(ROME);
    }

    @Test
    void timestampTzWithUtcSessionStaysUtc()
    {
        long packed = KairosdbRecordCursor.packTimestamp(
                TimestampWithTimeZoneType.createTimestampWithTimeZoneType(3), INSTANT_MS, UTC_KEY);
        assertThat(unpackMillisUtc(packed)).isEqualTo(INSTANT_MS);
        assertThat(unpackZoneKey(packed)).isEqualTo(UTC_KEY);
    }
}
