package io.examon.trino.kairosdb_connector;

import com.google.common.collect.ImmutableSet;

import java.util.Locale;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Catalogue and parser for the {@code sampling_aggregator} hidden virtual
 * column.  Users push KairosDB sampling aggregators through this column by
 * writing predicates such as
 *
 * <pre>{@code
 *   -- Single aggregator
 *   WHERE sampling_aggregator = 'sum;1h;start_time'
 *
 *   -- Chained aggregators (output of left feeds the right).  Use a single
 *   -- VARCHAR value with '|' between the specs - this is the only form
 *   -- that guarantees the chain order matches what you wrote.
 *   WHERE sampling_aggregator = 'sum;1h;start_time|avg;5m;sampling'
 *
 *   -- IN(...) is accepted but Trino delivers the values in alphabetical
 *   -- order, so the chain order is determined by spec text and not by SQL.
 *   -- Connector logs a warning when this form is used with more than one
 *   -- value.
 *   WHERE sampling_aggregator IN ('sum;1h;start_time', 'avg;5m;sampling')
 * }</pre>
 *
 * <p>The on-the-wire spec is {@code type;interval;alignment}:
 * <ul>
 *   <li><b>type</b>: one of {@link #VALID_AGGREGATORS}.  Anything not in
 *       the list is later rejected at parse time; whether the connector
 *       silently falls back to {@code avg} when KairosDB itself does not
 *       recognise it is the JSON builder's job, not ours.</li>
 *   <li><b>interval</b>: {@code <digits>[smhd]?} - bare digits default to
 *       minutes.</li>
 *   <li><b>alignment</b>: one of {@link #VALID_ALIGN_OPTIONS}; {@code none}
 *       means "do not send any align_* flag to KairosDB".</li>
 * </ul>
 *
 * <p>Invalid values are <em>not</em> a hard error: callers catch the
 * {@link IllegalArgumentException} and skip the malformed entry, so a
 * single bad aggregator never aborts the whole query.
 */
public final class KairosdbSamplingConstants
{
    /** The user-visible column name. */
    public static final String SAMPLING_AGGREGATOR = "sampling_aggregator";

    public static final Set<String> VALID_AGGREGATORS = ImmutableSet.of(
            "avg", "sum", "min", "max", "count", "first", "last",
            "dev", "percentile", "rate", "sampler", "scale", "trim",
            "gaps", "histogram", "least_squares");

    public static final Set<String> VALID_ALIGN_OPTIONS = ImmutableSet.of(
            "start_time", "end_time", "sampling", "none");

    private static final Pattern AGGREGATOR_PATTERN = Pattern.compile("([^;]+);([^;]+);([^;]+)");
    private static final Pattern INTERVAL_PATTERN = Pattern.compile("(\\d+)([smhd]?)");

    private KairosdbSamplingConstants() {}

    /** @return true iff the given identifier is the hidden sampling column. */
    public static boolean isSamplingColumn(String columnName)
    {
        return SAMPLING_AGGREGATOR.equalsIgnoreCase(columnName);
    }

    /**
     * Parses one {@code type;interval;alignment} spec into its components.
     *
     * @throws IllegalArgumentException if any component is missing or
     *         outside the documented value set.  The caller is expected to
     *         log and drop the offending entry rather than fail the query.
     */
    public static ParsedAggregator parse(String spec)
    {
        if (spec == null || spec.isBlank()) {
            throw new IllegalArgumentException("Aggregator spec cannot be null or empty");
        }
        Matcher m = AGGREGATOR_PATTERN.matcher(spec.trim());
        if (!m.matches()) {
            throw new IllegalArgumentException(
                    "Invalid aggregator format; expected 'type;interval;alignment', got: " + spec);
        }
        String type = m.group(1).trim().toLowerCase(Locale.ROOT);
        String interval = m.group(2).trim().toLowerCase(Locale.ROOT);
        String alignment = m.group(3).trim().toLowerCase(Locale.ROOT);

        if (!VALID_AGGREGATORS.contains(type)) {
            throw new IllegalArgumentException("Unknown aggregator type: " + type);
        }
        if (!VALID_ALIGN_OPTIONS.contains(alignment)) {
            throw new IllegalArgumentException("Unknown alignment option: " + alignment);
        }
        Interval parsedInterval = parseInterval(interval);
        return new ParsedAggregator(type, parsedInterval, alignment);
    }

    private static Interval parseInterval(String interval)
    {
        Matcher m = INTERVAL_PATTERN.matcher(interval);
        if (!m.matches()) {
            throw new IllegalArgumentException("Invalid interval format: " + interval);
        }
        int value = Integer.parseInt(m.group(1));
        String unitChar = m.group(2);
        KairosdbTimeUnit unit = switch (unitChar) {
            case "", "m" -> KairosdbTimeUnit.MINUTES;
            case "s" -> KairosdbTimeUnit.SECONDS;
            case "h" -> KairosdbTimeUnit.HOURS;
            case "d" -> KairosdbTimeUnit.DAYS;
            default -> throw new IllegalArgumentException("Invalid interval unit: " + unitChar);
        };
        return new Interval(value, unit);
    }

    /** KairosDB-side time unit identifiers (lowercase plural, as expected on the wire). */
    public enum KairosdbTimeUnit
    {
        MILLISECONDS("milliseconds"),
        SECONDS("seconds"),
        MINUTES("minutes"),
        HOURS("hours"),
        DAYS("days"),
        WEEKS("weeks"),
        MONTHS("months"),
        YEARS("years");

        private final String jsonValue;

        KairosdbTimeUnit(String jsonValue)
        {
            this.jsonValue = jsonValue;
        }

        public String jsonValue()
        {
            return jsonValue;
        }
    }

    public record Interval(int value, KairosdbTimeUnit unit) {}

    public record ParsedAggregator(String type, Interval interval, String alignment) {}
}
