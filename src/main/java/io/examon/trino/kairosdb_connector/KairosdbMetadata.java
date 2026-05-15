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
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.VarcharType;

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
        if (isInternalMetric(tableName.getTableName())) {
            return null;
        }
        return client.resolveTableName(tableName.getTableName())
                .map(resolved -> new KairosdbTableHandle(connectorId.toString(), KairosdbNameSpace.SCHEMA, resolved))
                .orElse(null);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        KairosdbTableHandle handle = (KairosdbTableHandle) table;
        List<ColumnMetadata> columns = client.getColumns(handle.getTableName());
        return new ConnectorTableMetadata(handle.toSchemaTableName(), columns);
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
     * Pushes timestamp range and tag equality / {@code IN} predicates into
     * KairosDB.  Anything else (expressions, dynamic filters, range queries
     * on tags) stays as "remaining" and Trino re-evaluates it above the
     * connector.
     *
     * <p>Both kinds of pushdown follow the production-validated convention
     * of claiming the column as fully consumed even when the equivalent
     * KairosDB query is technically a superset of the predicate.  In
     * practice every observed query has been a single timestamp range plus
     * tag equalities, so the superset case never fires.
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

        // Build a lowercase Trino name -> original KairosDB tag name index
        // up front: the schema cache already knows the original-case keys.
        List<String> originalTagKeys = client.getOriginalTagKeys(table.getTableName());
        Map<String, String> originalByLower = new HashMap<>();
        for (String original : originalTagKeys) {
            originalByLower.put(original.toLowerCase(Locale.ROOT), original);
        }

        Map<ColumnHandle, Domain> domains = summary.getDomains().get();
        Map<ColumnHandle, Domain> remaining = new HashMap<>(domains);
        Optional<KairosdbTimestampPushdown.Window> window = Optional.empty();
        LinkedHashMap<String, List<String>> tagFilters = new LinkedHashMap<>(table.getPushedTagFilters());

        for (Map.Entry<ColumnHandle, Domain> entry : domains.entrySet()) {
            KairosdbColumnHandle column = (KairosdbColumnHandle) entry.getKey();
            String columnName = column.getColumnName();

            if (KairosdbTimestampPushdown.isTimestampColumn(columnName)) {
                window = KairosdbTimestampPushdown.extractWindow(entry.getValue(), column.getColumnType());
                if (window.isPresent()) {
                    remaining.remove(column);
                }
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
            // Claim the tag domain as fully pushed down, matching production
            // behaviour even for the (unlikely) case of a string range that
            // yielded zero concrete values.
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
        if (timestampUnchanged && tagsUnchanged) {
            return Optional.empty();
        }

        KairosdbTableHandle pushed = table
                .withTimeRange(newStart, newEnd)
                .withTagFilters(tagFilters);

        if (window.isPresent()) {
            log.info("Pushed timestamp window %s into %s.%s", window.get().pretty(), pushed.getSchemaName(), pushed.getTableName());
        }
        if (!tagFilters.isEmpty()) {
            log.info("Pushed tag filters %s into %s.%s", tagFilters, pushed.getSchemaName(), pushed.getTableName());
        }

        return Optional.of(new ConstraintApplicationResult<>(
                pushed,
                TupleDomain.withColumnDomains(remaining),
                constraint.getExpression(),
                false));
    }

    /**
     * A column qualifies as a KairosDB tag column when it carries a VARCHAR
     * type and is not one of the synthetic {@code timestamp} / {@code value}
     * columns.  Future commits will also exclude hidden columns such as
     * {@code sampling_aggregator}.
     */
    private static boolean isTagColumn(KairosdbColumnHandle column)
    {
        if (!(column.getColumnType() instanceof VarcharType)) {
            return false;
        }
        String name = column.getColumnName();
        return !KairosdbTimestampPushdown.isTimestampColumn(name) && !"value".equalsIgnoreCase(name);
    }

    private static boolean isInternalMetric(String metricName)
    {
        return metricName.toLowerCase(Locale.ROOT).startsWith(INTERNAL_METRIC_PREFIX);
    }
}
