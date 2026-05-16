package io.examon.trino.kairosdb_connector;

import com.google.common.collect.ImmutableList;
import io.examon.trino.kairosdb_connector.KairosdbTimestampPushdown.Window;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.SortedRangeSet;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Locks in the predicate -> [startMillis, endMillis] translation for the
 * three supported timestamp column types and the "epoch seconds vs ms"
 * heuristic for BIGINT.
 */
final class TestKairosdbTimestampPushdown
{
    private static final Type BIGINT = BigintType.BIGINT;
    private static final Type TS_MILLIS = TimestampType.TIMESTAMP_MILLIS;
    private static final Type TS_TZ = TimestampWithTimeZoneType.createTimestampWithTimeZoneType(3);

    // Reference moment: 2023-11-14 22:13:20 UTC.
    private static final long T_MS = 1_700_000_000_000L;
    private static final long T_SEC = 1_700_000_000L;

    @Test
    void singlePointBigintIsExactAndClosesBothBounds()
    {
        Domain d = Domain.singleValue(BIGINT, T_MS);

        Optional<Window> w = KairosdbTimestampPushdown.extractWindow(d, BIGINT);

        assertThat(w).isPresent();
        assertThat(w.get().startMillis()).contains(T_MS);
        assertThat(w.get().endMillis()).contains(T_MS);
        assertThat(w.get().exact()).isTrue();
    }

    @Test
    void rangeBigintHonoursBothBounds()
    {
        Domain d = Domain.create(
                ValueSet.ofRanges(Range.range(BIGINT, T_MS, true, T_MS + 60_000, true)),
                false);

        Window w = KairosdbTimestampPushdown.extractWindow(d, BIGINT).orElseThrow();

        assertThat(w.startMillis()).contains(T_MS);
        assertThat(w.endMillis()).contains(T_MS + 60_000);
        assertThat(w.exact()).isTrue();
    }

    @Test
    void multipleRangesProduceApproximateOuterEnvelope()
    {
        SortedRangeSet ranges = (SortedRangeSet) ValueSet.ofRanges(
                Range.equal(BIGINT, T_MS),
                Range.equal(BIGINT, T_MS + 60_000));
        Domain d = Domain.create(ranges, false);

        Window w = KairosdbTimestampPushdown.extractWindow(d, BIGINT).orElseThrow();

        assertThat(w.startMillis()).contains(T_MS);
        assertThat(w.endMillis()).contains(T_MS + 60_000);
        // Multi-range / IN-list -> never claimed as exact, even though the
        // numeric envelope happens to be tight.  Trino keeps its own filter
        // node above us as a backstop.
        assertThat(w.exact()).isFalse();
    }

    @Test
    void bigintEpochSecondsHeuristicRescalesToMillis()
    {
        // User writes WHERE timestamp = 1700000000 (epoch seconds).  Treating
        // it literally would point at 1970-01-15; the heuristic must
        // promote it to milliseconds and land on 2023-11-14.
        Domain d = Domain.singleValue(BIGINT, T_SEC);

        Window w = KairosdbTimestampPushdown.extractWindow(d, BIGINT).orElseThrow();

        assertThat(w.startMillis()).contains(T_MS);
        assertThat(w.endMillis()).contains(T_MS);
    }

    @Test
    void bigintAlreadyMillisIsLeftAlone()
    {
        Domain d = Domain.singleValue(BIGINT, T_MS);

        Window w = KairosdbTimestampPushdown.extractWindow(d, BIGINT).orElseThrow();

        assertThat(w.startMillis()).contains(T_MS);
    }

    @Test
    void timestampMillisDividesMicrosToMillis()
    {
        // Trino stores TIMESTAMP without zone as microseconds since epoch.
        long micros = T_MS * 1_000L;
        Domain d = Domain.singleValue(TS_MILLIS, micros);

        Window w = KairosdbTimestampPushdown.extractWindow(d, TS_MILLIS).orElseThrow();

        assertThat(w.startMillis()).contains(T_MS);
        assertThat(w.endMillis()).contains(T_MS);
    }

    @Test
    void timestampTzUnpacksUtcMillis()
    {
        long packed = packDateTimeWithZone(T_MS, UTC_KEY);
        Domain d = Domain.singleValue(TS_TZ, packed);

        Window w = KairosdbTimestampPushdown.extractWindow(d, TS_TZ).orElseThrow();

        assertThat(w.startMillis()).contains(T_MS);
        assertThat(w.endMillis()).contains(T_MS);
    }

    @Test
    void unboundedLowerBoundLeavesStartOpen()
    {
        Domain d = Domain.create(
                ValueSet.ofRanges(Range.lessThanOrEqual(BIGINT, T_MS)),
                false);

        Window w = KairosdbTimestampPushdown.extractWindow(d, BIGINT).orElseThrow();

        assertThat(w.startMillis()).isEmpty();
        assertThat(w.endMillis()).contains(T_MS);
    }

    @Test
    void allOrNoneDomainsAreNotPushed()
    {
        assertThat(KairosdbTimestampPushdown.extractWindow(Domain.all(BIGINT), BIGINT)).isEmpty();
        assertThat(KairosdbTimestampPushdown.extractWindow(Domain.none(BIGINT), BIGINT)).isEmpty();
    }

    @Test
    void isTimestampColumnIsCaseInsensitive()
    {
        assertThat(KairosdbTimestampPushdown.isTimestampColumn("timestamp")).isTrue();
        assertThat(KairosdbTimestampPushdown.isTimestampColumn("TIMESTAMP")).isTrue();
        assertThat(KairosdbTimestampPushdown.isTimestampColumn("host")).isFalse();
    }

    @Test
    void windowRejectsInvertedBounds()
    {
        // Defensive: the Window record blocks start > end at construction.
        assertThat(ImmutableList.of()).isEmpty();
        // Use reflection-free assertion: just construct via the public ctor.
        try {
            new Window(Optional.of(2L), Optional.of(1L), true);
            org.junit.jupiter.api.Assertions.fail("expected IllegalArgumentException");
        }
        catch (IllegalArgumentException expected) {
            assertThat(expected.getMessage()).contains("startMillis > endMillis");
        }
    }
}
