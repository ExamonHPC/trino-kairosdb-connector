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

import static io.examon.trino.kairosdb_connector.KairosdbSessionProperties.getDefaultStartHours;
import static io.examon.trino.kairosdb_connector.KairosdbSessionProperties.getSplitSizeMillis;

import static java.util.Objects.requireNonNull;

/**
 * Turns a table handle into a list of {@link KairosdbSplit}s, each covering a
 * {@code kairosdb.split-size}-wide window.
 *
 * <p>The active time window is resolved as follows:
 * <ul>
 *   <li>{@code endMillis} = pushed high bound if any, otherwise {@code now}.</li>
 *   <li>{@code startMillis} = pushed low bound if any, otherwise
 *       {@code endMillis - default_start_hours}.</li>
 * </ul>
 * Anchoring the default floor to {@code endMillis} (not unconditionally to
 * {@code now}) means a predicate like {@code WHERE timestamp <= old_T}
 * produces {@code [old_T - default_start_hours, old_T]} rather than the
 * inverted window {@code [now - default_start_hours, old_T]} that would
 * silently return zero rows when {@code old_T} sits before the look-back.
 *
 * <p>The look-back comes from the {@code default_start_hours} session
 * property when set, otherwise the catalog
 * {@code kairosdb.timestamp.default-start-hours}.  Split width comes from
 * {@code split_size_millis} (session) defaulting to {@code kairosdb.split-size}.
 *
 * <p>Adjacent splits are offset by one millisecond.  KairosDB query bounds are
 * inclusive on both ends, so without that gap a datapoint landing exactly on
 * a slice boundary would be returned by both neighbours.
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

    @Inject
    public KairosdbSplitManager(KairosdbConnectorId connectorId)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
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
        int defaultStartHours = getDefaultStartHours(session);
        long splitMillis = getSplitSizeMillis(session);
        // Resolve end first so that, when only a high bound was pushed (e.g.
        // WHERE timestamp <= old_T), the default look-back anchors to that
        // bound and produces a sane historical window rather than an
        // inverted [now - lookback, old_T].
        long endMillis = handle.getPushedEndMillis().orElse(now);
        long lookbackMs = Duration.ofHours(defaultStartHours).toMillis();
        long startMillis = handle.getPushedStartMillis()
                .orElseGet(() -> endMillis - lookbackMs);
        Map<String, List<String>> tagFilters = handle.getPushedTagFilters();
        Optional<Long> limit = handle.getPushedLimit();
        List<String> aggregators = handle.getPushedAggregators();

        List<KairosdbSplit> splits;
        if (limit.isPresent() || !aggregators.isEmpty()) {
            // Collapse to a single split when either is pushed:
            //   * LIMIT N: K time-slices would each legally return up to N
            //     rows, so the connector would over-deliver K*N.
            //   * sampling aggregators: Time-based fan-out runs the aggregator 
            //     pipeline independently per split. Any bucket or chained aggregation 
            //     stage that crosses a split boundary is computed from partial input, 
            //     and alignments may be anchored per split rather than for the whole query. 
            //     A single full-window split is required to preserve KairosDB's 
            //     aggregator semantics.
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
