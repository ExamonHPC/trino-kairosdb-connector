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
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Turns a table handle into a list of {@link KairosdbSplit}s, each covering a
 * {@code kairosdb.split-size}-wide window.
 *
 * <p>The active time window is, in order of precedence:
 * <ol>
 *   <li>The bounds the metadata layer pushed down from a
 *       {@code WHERE timestamp ...} predicate (either side may still be open);</li>
 *   <li>The connector defaults: {@code [now - default-start-hours, now]}.</li>
 * </ol>
 *
 * <p>Adjacent splits are offset by one millisecond.  KairosDB query bounds are
 * inclusive on both ends, so without that gap a datapoint landing exactly on
 * a slice boundary would be returned by both neighbours.  This is the same
 * fix the production connector carried for several releases.
 *
 * <p>When a {@code LIMIT N} or any {@code sampling_aggregator} has been
 * pushed down, the manager emits exactly <em>one</em> split covering the
 * whole time window.  Time-based fan-out would multiply the row count under
 * LIMIT (K splits, each up to N rows), and would re-bucket aggregators
 * independently per slice; collapsing to one split delegates that work to
 * KairosDB.  Trino keeps its own LIMIT operator above us as a backstop
 * (see KairosdbMetadata.applyLimit for {@code limitGuaranteed = false}).
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

        long now = Instant.now().toEpochMilli();
        long startMillis = handle.getPushedStartMillis()
                .orElseGet(() -> now - Duration.ofHours(config.getDefaultStartHours()).toMillis());
        long endMillis = handle.getPushedEndMillis().orElse(now);
        long splitMillis = config.getSplitSize().toMillis();
        Map<String, List<String>> tagFilters = handle.getPushedTagFilters();
        Optional<Long> limit = handle.getPushedLimit();
        List<String> aggregators = handle.getPushedAggregators();

        List<KairosdbSplit> splits;
        if (limit.isPresent() || !aggregators.isEmpty()) {
            // Both LIMIT and sampling aggregators must be applied to the whole
            // window as a single KairosDB query: a time-based fan-out would
            // either return up to K*N rows under LIMIT, or independently
            // re-bucket each slice under aggregators (so a daily bucket asked
            // across an N-day window would yield N independent daily buckets,
            // not one).  Collapsing to a single split makes KairosDB do the
            // work and matches the long-running production behaviour.
            splits = ImmutableList.of(new KairosdbSplit(
                    connectorId.toString(),
                    handle.getSchemaName(),
                    handle.getTableName(),
                    startMillis,
                    endMillis,
                    tagFilters,
                    limit,
                    aggregators));
        }
        else {
            splits = chopTimeRange(handle, startMillis, endMillis, splitMillis, tagFilters);
        }

        log.debug("Generated %d split(s) for %s.%s over [%d, %d] (pushed=%s, tags=%s, limit=%s, aggregators=%s) with split size %d ms",
                splits.size(),
                handle.getSchemaName(),
                handle.getTableName(),
                startMillis,
                endMillis,
                handle.getPushedStartMillis().isPresent() || handle.getPushedEndMillis().isPresent(),
                tagFilters,
                limit.orElse(null),
                aggregators,
                splitMillis);
        return new FixedSplitSource(splits);
    }

    private List<KairosdbSplit> chopTimeRange(
            KairosdbTableHandle handle,
            long startMillis,
            long endMillis,
            long splitMillis,
            Map<String, List<String>> tagFilters)
    {
        if (endMillis < startMillis) {
            return ImmutableList.of();
        }
        if (splitMillis <= 0) {
            // Defensive: configuration validation already requires split size >= 1ms.
            splitMillis = Math.max(1L, endMillis - startMillis);
        }
        ImmutableList.Builder<KairosdbSplit> builder = ImmutableList.builder();
        long cursor = startMillis;
        while (cursor <= endMillis) {
            long sliceEnd = Math.min(cursor + splitMillis, endMillis);
            builder.add(new KairosdbSplit(
                    connectorId.toString(),
                    handle.getSchemaName(),
                    handle.getTableName(),
                    cursor,
                    sliceEnd,
                    tagFilters,
                    Optional.empty(),
                    ImmutableList.of()));
            if (sliceEnd >= endMillis) {
                break;
            }
            cursor = sliceEnd + 1;
        }
        return builder.build();
    }
}
