package io.examon.trino.kairosdb_connector;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.LimitApplicationResult;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.VarcharType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Read-only metadata for the KairosDB connector.
 *
 * <p>One schema, named {@value KairosdbNameSpace#SCHEMA}, exposes every
 * KairosDB metric whose name does not start with "kairosdb" (those are
 * internal bookkeeping metrics and not useful to query through SQL).
 */
public class KairosdbMetadata
        implements ConnectorMetadata
{
    private static final Logger log = Logger.get(KairosdbMetadata.class);

    /** KairosDB publishes its own internal counters under this prefix; we hide them. */
    private static final String INTERNAL_METRIC_PREFIX = "kairosdb";

    private final KairosdbConnectorId connectorId;
    private final KairosdbClient client;

    @Inject
    public KairosdbMetadata(KairosdbConnectorId connectorId, KairosdbClient client)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.client = requireNonNull(client, "client is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return ImmutableList.of(KairosdbNameSpace.SCHEMA);
    }

    @Override
    public ConnectorTableHandle getTableHandle(
            ConnectorSession session,
            SchemaTableName tableName,
            Optional<ConnectorTableVersion> startVersion,
            Optional<ConnectorTableVersion> endVersion)
    {
        if (!KairosdbNameSpace.SCHEMA.equals(tableName.getSchemaName())) {
            return null;
        }
        // tableName.getTableName() is the Trino-side identifier; for
        // collision-group members it's already the mangled form.  The
        // mangled prefix happens to be a normal lowercase metric name
        // (e.g. "pue__a1b2c3" starts with "pue") which never matches the
        // internal-metric prefix unless the user has somehow named a
        // metric `kairosdb_*` themselves; the lower-case prefix check
        // catches both raw and mangled forms.
        if (isInternalMetric(tableName.getTableName())) {
            return null;
        }
        return client.resolveTableName(tableName.getTableName())
                .map(kairosOriginal -> {
                    // Carry the Trino-side name on the handle so that
                    // toSchemaTableName() round-trips through SHOW TABLES /
                    // DESCRIBE faithfully.  Empty when the Trino-side name
                    // is just the lowercase of the KairosDB original (no
                    // mangling); set when the metric belongs to a case-
                    // collision group and was given a hash suffix.
                    Optional<String> trinoSide =
                            tableName.getTableName().equals(kairosOriginal.toLowerCase(Locale.ROOT))
                                    ? Optional.empty()
                                    : Optional.of(tableName.getTableName());
                    return new KairosdbTableHandle(
                            connectorId.toString(),
                            KairosdbNameSpace.SCHEMA,
                            kairosOriginal,
                            trinoSide);
                })
                .orElse(null);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        KairosdbTableHandle handle = (KairosdbTableHandle) table;
        List<ColumnMetadata> columns = client.getColumns(handle.getTableName());
        Optional<String> comment = client.getTableComment(handle.getTableName());
        return new ConnectorTableMetadata(handle.toSchemaTableName(), columns, ImmutableMap.of(), comment);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        if (schemaName.isPresent() && !KairosdbNameSpace.SCHEMA.equals(schemaName.get())) {
            return ImmutableList.of();
        }
        ImmutableList.Builder<SchemaTableName> tables = ImmutableList.builder();
        for (String metric : client.listMetricNames()) {
            if (!isInternalMetric(metric)) {
                tables.add(new SchemaTableName(KairosdbNameSpace.SCHEMA, metric));
            }
        }
        return tables.build();
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        KairosdbTableHandle handle = (KairosdbTableHandle) tableHandle;
        ImmutableMap.Builder<String, ColumnHandle> result = ImmutableMap.builder();
        List<ColumnMetadata> columns = client.getColumns(handle.getTableName());
        int ordinal = 0;
        for (ColumnMetadata column : columns) {
            result.put(column.getName(), new KairosdbColumnHandle(
                    connectorId.toString(), column.getName(), column.getType(), ordinal++));
        }
        return result.buildOrThrow();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return ((KairosdbColumnHandle) columnHandle).getColumnMetadata();
    }

    /**
     * Pushes timestamp ranges, tag equality / {@code IN} predicates and
     * {@code sampling_aggregator} hints into KairosDB.  Pushdown is strict:
     * a column is claimed as fully consumed only when KairosDB can natively
     * evaluate the entire predicate.  Otherwise the predicate (or part of
     * it) is left in {@code remainingFilter} so Trino re-applies it above
     * the connector, while the connector may still push a wider
     * <em>bounding</em> window to KairosDB as a fetch hint.
     *
     * <p>Concretely:
     * <ul>
     *   <li><b>Timestamp.</b>  A single, inclusive (after a 1&nbsp;ms shift
     *       for half-open bounds) range is pushed as KairosDB's
     *       {@code [start_absolute, end_absolute]} and claimed as
     *       consumed.  Multi-range predicates ({@code IN}, {@code !=},
     *       disjoint OR-of-ranges) yield a bounding hull which is pushed
     *       as a fetch window but left residual so Trino reduces the
     *       result set to exactly the rows the predicate admits.</li>
     *   <li><b>Tag.</b>  Pure equality / {@code IN} predicates are pushed.
     *       Ranges, {@code NOT IN}, {@code IS NULL}, and mixed shapes are
     *       not expressible in a KairosDB tag filter and stay residual.</li>
     *   <li><b>{@code sampling_aggregator}.</b>  Always claimed as consumed -
     *       the hidden column has no underlying data for Trino to compare
     *       against, so leaving it residual would force every row through
     *       a filter against a non-existent value.</li>
     * </ul>
     */
    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(
            ConnectorSession session, ConnectorTableHandle handle, Constraint constraint)
    {
        KairosdbTableHandle table = (KairosdbTableHandle) handle;
        TupleDomain<ColumnHandle> summary = constraint.getSummary();
        if (summary.isAll() || summary.isNone() || summary.getDomains().isEmpty()) {
            return Optional.empty();
        }

        // Trino lowercases every column identifier before handing it to
        // the connector (see KairosdbClient.discoverSchema for the
        // upstream-tracked rationale).  KairosDB on the other hand is
        // case-sensitive and stores tag names verbatim, so we keep an
        // index that maps the lowercase Trino-side name back to the
        // KairosDB-side original-case name and use that translation when
        // building the tag-filter map.
        List<String> originalTagKeys = client.getOriginalTagKeys(table.getTableName());
        Map<String, String> originalByLower = new HashMap<>();
        for (String original : originalTagKeys) {
            originalByLower.put(original.toLowerCase(Locale.ROOT), original);
        }

        Map<ColumnHandle, Domain> domains = summary.getDomains().get();
        Map<ColumnHandle, Domain> remaining = new HashMap<>(domains);
        Optional<KairosdbTimestampPushdown.Window> window = Optional.empty();
        LinkedHashMap<String, List<String>> tagFilters = new LinkedHashMap<>(table.getPushedTagFilters());
        List<String> aggregators = new ArrayList<>(table.getPushedAggregators());

        for (Map.Entry<ColumnHandle, Domain> entry : domains.entrySet()) {
            KairosdbColumnHandle column = (KairosdbColumnHandle) entry.getKey();
            String columnName = column.getColumnName();

            if (KairosdbTimestampPushdown.isTimestampColumn(columnName)) {
                window = KairosdbTimestampPushdown.extractWindow(entry.getValue(), column.getColumnType());
                // Strict semantics: only release the column from Trino's
                // residual filter when KairosDB can evaluate the predicate
                // exactly.  Inexact bounding hulls (IN-list, !=, multi-range)
                // are still pushed as a fetch hint via the new handle, but
                // Trino keeps the predicate to refine the result above us.
                if (window.isPresent() && window.get().exact()) {
                    remaining.remove(column);
                }
                continue;
            }

            if (KairosdbSamplingConstants.isSamplingColumn(columnName)) {
                // Hidden control column: the predicate becomes one or more
                // sampling aggregators applied to the KairosDB query.  We
                // always claim the domain as fully pushed (whether or not we
                // could parse anything out of it), because there's no
                // underlying data for Trino to compare against -- leaving it
                // as residual would force every row through a filter against
                // a non-existent value and return zero rows.
                aggregators = extractSamplingSpecs(entry.getValue(), aggregators);
                remaining.remove(column);
                continue;
            }

            if (!isTagColumn(column)) {
                continue;
            }
            String originalTagName = originalByLower.get(columnName.toLowerCase(Locale.ROOT));
            if (originalTagName == null) {
                // VARCHAR column that is not actually a tag of this metric
                // (could happen during planning of cross-table joins); leave
                // it for Trino to handle.
                continue;
            }
            Optional<List<String>> admitted = KairosdbTagPushdown.extractAdmittedValues(entry.getValue());
            if (admitted.isEmpty()) {
                continue;
            }
            tagFilters.put(originalTagName, admitted.get());
            // KairosDB will match exactly this set, so Trino doesn't need
            // to re-apply the predicate above us.
            remaining.remove(column);
        }

        Optional<Long> newStart = window.flatMap(KairosdbTimestampPushdown.Window::startMillis);
        Optional<Long> newEnd = window.flatMap(KairosdbTimestampPushdown.Window::endMillis);
        if (window.isEmpty()) {
            newStart = table.getPushedStartMillis();
            newEnd = table.getPushedEndMillis();
        }

        boolean timestampUnchanged = newStart.equals(table.getPushedStartMillis())
                && newEnd.equals(table.getPushedEndMillis());
        boolean tagsUnchanged = tagFilters.equals(table.getPushedTagFilters());
        boolean aggregatorsUnchanged = aggregators.equals(table.getPushedAggregators());
        if (timestampUnchanged && tagsUnchanged && aggregatorsUnchanged) {
            return Optional.empty();
        }

        KairosdbTableHandle pushed = table
                .withTimeRange(newStart, newEnd)
                .withTagFilters(tagFilters)
                .withAggregators(aggregators);

        if (window.isPresent()) {
            log.info("Pushed timestamp window %s into %s.%s", window.get().pretty(), pushed.getSchemaName(), pushed.getTableName());
        }
        if (!tagFilters.isEmpty()) {
            log.info("Pushed tag filters %s into %s.%s", tagFilters, pushed.getSchemaName(), pushed.getTableName());
        }
        if (!aggregators.isEmpty()) {
            log.info("Pushed sampling aggregators %s into %s.%s", aggregators, pushed.getSchemaName(), pushed.getTableName());
        }

        return Optional.of(new ConstraintApplicationResult<>(
                pushed,
                TupleDomain.withColumnDomains(remaining),
                constraint.getExpression(),
                false));
    }

    /**
     * Pushes a {@code LIMIT N} into KairosDB's native per-metric cap.
     *
     * <p>Because KairosDB may legitimately return fewer rows than requested
     * (data is sparse), {@code limitGuaranteed} is {@code false}: Trino
     * keeps its own LIMIT operator above us as a backstop and we are free
     * to over-deliver only as a (small) inefficiency, never as incorrect
     * results.  When the limit is pushed down, {@link KairosdbSplitManager}
     * collapses to a single full-range split so KairosDB sees one query
     * and applies the cap server-side (a fan-out of K splits would each
     * legally return up to N rows, totalling K * N).
     *
     * <p>The loop guard prevents Trino from re-invoking us forever with
     * the same limit: if the handle already carries it, we return empty.
     */
    @Override
    public Optional<LimitApplicationResult<ConnectorTableHandle>> applyLimit(
            ConnectorSession session, ConnectorTableHandle handle, long limit)
    {
        KairosdbTableHandle table = (KairosdbTableHandle) handle;
        if (limit <= 0) {
            return Optional.empty();
        }
        if (table.getPushedLimit().filter(existing -> existing == limit).isPresent()) {
            return Optional.empty();
        }
        KairosdbTableHandle pushed = table.withLimit(Optional.of(limit));
        log.info("Pushed limit=%d into %s.%s", limit, pushed.getSchemaName(), pushed.getTableName());
        return Optional.of(new LimitApplicationResult<>(pushed, false, false));
    }

    /**
     * A column qualifies as a KairosDB tag column when it carries a VARCHAR
     * type and is not one of the synthetic {@code timestamp} / {@code value}
     * columns, nor the hidden {@code sampling_aggregator} control column.
     */
    private static boolean isTagColumn(KairosdbColumnHandle column)
    {
        if (!(column.getColumnType() instanceof VarcharType)) {
            return false;
        }
        String name = column.getColumnName();
        return !KairosdbTimestampPushdown.isTimestampColumn(name)
                && !"value".equalsIgnoreCase(name)
                && !KairosdbSamplingConstants.isSamplingColumn(name);
    }

    /**
     * Extracts and validates the chained sampling aggregator specs from a
     * predicate on the hidden {@code sampling_aggregator} column.  KairosDB
     * aggregators are functional composition - the output of one feeds the
     * next - so the order matters end-to-end.
     *
     * <p>Two ways for the user to express a chain:
     * <ul>
     *   <li><b>Recommended, order-preserving:</b> equality against a single
     *       VARCHAR value with {@code '|'} separating the individual
     *       {@code type;interval;alignment} specs.  Example:
     *       <pre>{@code WHERE sampling_aggregator = 'sum;1h;start_time|avg;5m;sampling'}</pre>
     *       The chain is applied in <em>exactly</em> the order written.</li>
     *   <li><b>Best-effort, order-unspecified:</b> {@code IN (...)} of
     *       single-spec strings.  Trino delivers VARCHAR values out of a
     *       {@link io.trino.spi.predicate.SortedRangeSet} in alphabetical
     *       order, so the chain order is determined by spec text, not by
     *       the SQL the user wrote.  We log a warning and keep going so
     *       existing queries still run, but users who care about chain
     *       order must switch to the {@code '|'}-separated form.</li>
     * </ul>
     *
     * <p>Bad specs are dropped with a warning rather than failing the query;
     * a single malformed entry never aborts the whole SELECT.  If
     * <em>every</em> spec is bad, the previous aggregator list (if any) is
     * preserved so a follow-up applyFilter call doesn't lose state.
     */
    private static List<String> extractSamplingSpecs(Domain domain, List<String> existing)
    {
        Optional<List<String>> values = KairosdbTagPushdown.extractAdmittedValues(domain);
        if (values.isEmpty() || values.get().isEmpty()) {
            return existing;
        }
        if (values.get().size() > 1) {
            log.warn("sampling_aggregator received %d IN-list values whose chain order is " +
                    "alphabetical, not source-written; for deterministic order use " +
                    "sampling_aggregator = '<spec>|<spec>|...' instead.  Got: %s",
                    values.get().size(), values.get());
        }
        List<String> validated = new ArrayList<>();
        for (String value : values.get()) {
            // Each value may itself encode a '|'-separated chain.  Split,
            // trim, and validate each segment.  The order the user wrote
            // the chain in is preserved end-to-end because that is the
            // only knob the user has to control chain order.
            for (String segment : value.split("\\|")) {
                String spec = segment.trim();
                if (spec.isEmpty()) {
                    continue;
                }
                try {
                    KairosdbSamplingConstants.parse(spec);
                    validated.add(spec);
                }
                catch (IllegalArgumentException ex) {
                    log.warn("Dropping invalid sampling_aggregator spec '%s': %s", spec, ex.getMessage());
                }
            }
        }
        return validated.isEmpty() ? existing : validated;
    }

    private static boolean isInternalMetric(String metricName)
    {
        return metricName.toLowerCase(Locale.ROOT).startsWith(INTERNAL_METRIC_PREFIX);
    }
}
