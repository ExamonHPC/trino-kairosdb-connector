package io.examon.trino.kairosdb_connector;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedSplitSource;
import io.trino.spi.connector.Constraint;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Turns a table handle into a list of {@link KairosdbSplit}s, each covering a
 * {@code kairosdb.split-size}-wide window.
 *
 * <p>At this stage of the port the time window is fixed: now minus
 * {@code kairosdb.timestamp.default-start-hours} up to now.  Time-range
 * pushdown arrives in the next commit.
 */
public class KairosdbSplitManager
        implements ConnectorSplitManager
{
    private static final Logger log = Logger.get(KairosdbSplitManager.class);

    private final KairosdbConnectorId connectorId;
    private final KairosdbConfig config;

    @Inject
    public KairosdbSplitManager(KairosdbConnectorId connectorId, KairosdbConfig config)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.config = requireNonNull(config, "config is null");
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            DynamicFilter dynamicFilter,
            Constraint constraint)
    {
        KairosdbTableHandle handle = (KairosdbTableHandle) tableHandle;

        long endMillis = Instant.now().toEpochMilli();
        long startMillis = endMillis - Duration.ofHours(config.getDefaultStartHours()).toMillis();
        long splitMillis = config.getSplitSize().toMillis();

        List<KairosdbSplit> splits = chopTimeRange(handle, startMillis, endMillis, splitMillis);
        log.debug("Generated %d splits for %s.%s over [%d, %d) with split size %d ms",
                splits.size(), handle.getSchemaName(), handle.getTableName(), startMillis, endMillis, splitMillis);
        return new FixedSplitSource(splits);
    }

    private List<KairosdbSplit> chopTimeRange(KairosdbTableHandle handle, long startMillis, long endMillis, long splitMillis)
    {
        if (endMillis <= startMillis) {
            return ImmutableList.of();
        }
        if (splitMillis <= 0) {
            // Defensive: configuration validation already requires split size >= 1ms.
            splitMillis = endMillis - startMillis;
        }
        ImmutableList.Builder<KairosdbSplit> builder = ImmutableList.builder();
        long cursor = startMillis;
        while (cursor < endMillis) {
            long sliceEnd = Math.min(cursor + splitMillis, endMillis);
            builder.add(new KairosdbSplit(
                    connectorId.toString(),
                    handle.getSchemaName(),
                    handle.getTableName(),
                    cursor,
                    sliceEnd));
            cursor = sliceEnd;
        }
        return builder.build();
    }
}
