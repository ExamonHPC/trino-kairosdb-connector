package io.examon.trino.kairosdb_connector;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.session.PropertyMetadata;

import java.util.List;

import static io.trino.spi.session.PropertyMetadata.integerProperty;
import static io.trino.spi.session.PropertyMetadata.longProperty;
import static java.util.Objects.requireNonNull;

/**
 * Per-query overrides for the two split-shaping knobs whose right value
 * depends on how the user's individual query is shaped:
 *
 * <ul>
 *   <li>{@link #SPLIT_SIZE_MILLIS_NAME} - width of one time-range split.
 *       Smaller values fan out to more workers (cheaper per-split,
 *       higher parallelism) but pay more round-trips to KairosDB; larger
 *       values do the opposite.  Falls back to {@code kairosdb.split-size}.</li>
 *   <li>{@link #DEFAULT_START_HOURS_NAME} - how far back the split
 *       manager looks when the SQL has no timestamp predicate.  Falls
 *       back to {@code kairosdb.timestamp.default-start-hours}.</li>
 * </ul>
 *
 * <p>Both are exposed via {@code SET SESSION <catalog>.<property> = ...},
 * which gives users a way to tune individual queries without editing the
 * catalog file or restarting Trino.
 */
public final class KairosdbSessionProperties
{
    public static final String SPLIT_SIZE_MILLIS_NAME = "split_size_millis";
    public static final String DEFAULT_START_HOURS_NAME = "default_start_hours";

    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public KairosdbSessionProperties(KairosdbConfig config)
    {
        requireNonNull(config, "config is null");
        this.sessionProperties = ImmutableList.of(
                longProperty(
                        SPLIT_SIZE_MILLIS_NAME,
                        "Width of one time-range split, in milliseconds; overrides kairosdb.split-size for this session",
                        config.getSplitSize().toMillis(),
                        false),
                integerProperty(
                        DEFAULT_START_HOURS_NAME,
                        "Default look-back window in hours when the query does not constrain timestamp; overrides kairosdb.timestamp.default-start-hours",
                        config.getDefaultStartHours(),
                        false));
    }

    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    public static long getSplitSizeMillis(ConnectorSession session)
    {
        return session.getProperty(SPLIT_SIZE_MILLIS_NAME, Long.class);
    }

    public static int getDefaultStartHours(ConnectorSession session)
    {
        return session.getProperty(DEFAULT_START_HOURS_NAME, Integer.class);
    }
}
