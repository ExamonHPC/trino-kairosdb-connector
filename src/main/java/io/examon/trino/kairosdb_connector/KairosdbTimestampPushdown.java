package io.examon.trino.kairosdb_connector;

import io.airlift.log.Logger;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;

/**
 * Translates a Trino {@link Domain} on the synthetic {@code timestamp} column
 * into an absolute {@code [startMillis, endMillis]} window that KairosDB can
 * understand.
 *
 * <p>Three input encodings are supported, driven by
 * {@link io.examon.trino.kairosdb_connector.KairosdbConfig.TimestampFormat}:
 * <ul>
 *   <li>{@code BIGINT}: raw epoch milliseconds, but with a "reasonable date"
 *       heuristic that re-interprets values smaller than ~5&times;10<sup>9</sup>
 *       as epoch seconds.</li>
 *   <li>{@code TIMESTAMP_MILLIS}: Trino stores microseconds; divide by 1000.</li>
 *   <li>{@code TIMESTAMP_TZ}: packed via {@code DateTimeEncoding};
 *       {@code unpackMillisUtc} returns epoch ms directly.</li>
 * </ul>
 */
final class KairosdbTimestampPushdown
{
    private static final Logger log = Logger.get(KairosdbTimestampPushdown.class);

    /** 2000-01-01 to 2100-01-01 — anything outside that is considered "unreasonable". */
    private static final long REASONABLE_START_MILLIS = 946_684_800_000L;
    private static final long REASONABLE_END_MILLIS = 4_102_444_800_000L;

    /** Year 2128 in seconds. Below this we may treat a raw value as epoch seconds. */
    private static final long EPOCH_SECONDS_GUARD = 5_000_000_000L;

    private KairosdbTimestampPushdown() {}

    /**
     * Inspects a domain on the timestamp column and returns the smallest
     * absolute window that contains every value the domain admits, or
     * {@link Optional#empty()} if the domain is unrecognised or empty.
     *
     * <p>The returned window is always safe to push to KairosDB as a fetch
     * window: it never excludes a row that the original predicate would
     * accept.  Whether it is also <em>tight</em> (equal to the predicate) is
     * recorded in {@link Window#exact()}.  Only single-range / single-value
     * domains can be claimed as exact; multi-range or multi-value (e.g. an
     * {@code IN} list with several timestamps) are necessarily approximate and
     * must still be re-evaluated by Trino above the connector.
     */
    static Optional<Window> extractWindow(Domain domain, Type columnType)
    {
        if (domain.isAll() || domain.isNone()) {
            return Optional.empty();
        }

        List<Range> ranges = domain.getValues().getRanges().getOrderedRanges();
        OptionalLong overallMin = OptionalLong.empty();
        OptionalLong overallMax = OptionalLong.empty();

        for (Range range : ranges) {
            OptionalLong low = OptionalLong.empty();
            OptionalLong high = OptionalLong.empty();
            if (range.isSingleValue()) {
                long v = toMillis(range.getSingleValue(), columnType);
                low = OptionalLong.of(v);
                high = OptionalLong.of(v);
            }
            else {
                if (!range.isLowUnbounded()) {
                    low = OptionalLong.of(toMillis(range.getLowBoundedValue(), columnType));
                }
                if (!range.isHighUnbounded()) {
                    high = OptionalLong.of(toMillis(range.getHighBoundedValue(), columnType));
                }
            }
            overallMin = mergeMin(overallMin, low);
            overallMax = mergeMax(overallMax, high);
        }

        if (overallMin.isEmpty() && overallMax.isEmpty()) {
            return Optional.empty();
        }

        boolean exact = ranges.size() == 1;
        return Optional.of(new Window(
                overallMin.isPresent() ? Optional.of(overallMin.getAsLong()) : Optional.empty(),
                overallMax.isPresent() ? Optional.of(overallMax.getAsLong()) : Optional.empty(),
                exact));
    }

    /** Returns true if the given column name corresponds to the synthetic timestamp column. */
    static boolean isTimestampColumn(String columnName)
    {
        return "timestamp".equalsIgnoreCase(columnName);
    }

    private static long toMillis(Object value, Type columnType)
    {
        if (value == null) {
            throw new IllegalStateException("null timestamp value in domain");
        }
        long raw = ((Number) value).longValue();
        if (columnType instanceof TimestampType) {
            // Trino stores TIMESTAMP without time zone as microseconds since epoch.
            return raw / 1000L;
        }
        if (columnType instanceof TimestampWithTimeZoneType ttz && ttz.isShort()) {
            return unpackMillisUtc(raw);
        }
        if (columnType.equals(BigintType.BIGINT)) {
            return interpretBigintMillis(raw);
        }
        throw new IllegalStateException("Unsupported timestamp column type: " + columnType);
    }

    /**
     * Re-interprets a BIGINT timestamp value: if a user writes
     * {@code WHERE timestamp = 1234567890} with the BIGINT format, they
     * almost certainly mean epoch seconds, not Unix epoch milliseconds
     * sitting near 1970-01-15.  We accept the value as-is when it lands
     * in 2000-2100, otherwise we attempt a seconds-to-millis
     * re-interpretation and accept it if that lands in range.
     */
    private static long interpretBigintMillis(long raw)
    {
        if (raw >= REASONABLE_START_MILLIS && raw < REASONABLE_END_MILLIS) {
            return raw;
        }
        if (raw < EPOCH_SECONDS_GUARD) {
            long asMillis = raw * 1_000L;
            if (asMillis >= REASONABLE_START_MILLIS && asMillis < REASONABLE_END_MILLIS) {
                log.debug("Re-interpreting bigint timestamp %d as epoch seconds -> %d ms", raw, asMillis);
                return asMillis;
            }
        }
        log.warn("Bigint timestamp %d falls outside the 2000-2100 sanity window; using it as raw milliseconds", raw);
        return raw;
    }

    private static OptionalLong mergeMin(OptionalLong current, OptionalLong incoming)
    {
        if (incoming.isEmpty()) {
            return current;
        }
        if (current.isEmpty()) {
            return incoming;
        }
        return OptionalLong.of(Math.min(current.getAsLong(), incoming.getAsLong()));
    }

    private static OptionalLong mergeMax(OptionalLong current, OptionalLong incoming)
    {
        if (incoming.isEmpty()) {
            return current;
        }
        if (current.isEmpty()) {
            return incoming;
        }
        return OptionalLong.of(Math.max(current.getAsLong(), incoming.getAsLong()));
    }

    /**
     * Inclusive {@code [start, end]} window over epoch milliseconds; either
     * side may be open.  {@code exact == true} iff the window is identical to
     * the predicate it was derived from.
     */
    record Window(Optional<Long> startMillis, Optional<Long> endMillis, boolean exact)
    {
        Window
        {
            if (startMillis.isPresent() && endMillis.isPresent() && startMillis.get() > endMillis.get()) {
                throw new IllegalArgumentException("startMillis > endMillis: " + startMillis.get() + " > " + endMillis.get());
            }
        }

        String pretty()
        {
            return "["
                    + startMillis.map(m -> Instant.ofEpochMilli(m).toString()).orElse("-INF")
                    + ", "
                    + endMillis.map(m -> Instant.ofEpochMilli(m).toString()).orElse("+INF")
                    + "]" + (exact ? " (exact)" : " (approximate)");
        }
    }
}
