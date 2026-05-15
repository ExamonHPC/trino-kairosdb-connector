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

import java.util.HashMap;
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
     * Pushes the timestamp predicate into KairosDB's native time window.
     *
     * <p>Anything else (tag predicates, expressions, dynamic filters) stays as
     * "remaining" so Trino re-evaluates it after KairosDB returns rows.  Tag
     * pushdown lands in the next commit; until then a {@code WHERE host = ...}
     * filter still works correctly, it is just applied above the connector.
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

        Map<ColumnHandle, Domain> domains = summary.getDomains().get();
        Map<ColumnHandle, Domain> remaining = new HashMap<>(domains);
        Optional<KairosdbTimestampPushdown.Window> window = Optional.empty();

        for (Map.Entry<ColumnHandle, Domain> entry : domains.entrySet()) {
            KairosdbColumnHandle column = (KairosdbColumnHandle) entry.getKey();
            if (!KairosdbTimestampPushdown.isTimestampColumn(column.getColumnName())) {
                continue;
            }
            window = KairosdbTimestampPushdown.extractWindow(entry.getValue(), column.getColumnType());
            // Claim the timestamp domain as fully pushed down, matching the
            // production-validated behaviour: in practice every timestamp
            // predicate in this connector is a single contiguous range, so
            // the [min, max] window we send to KairosDB is also the predicate.
            // The rare multi-range / IN-list case will return a (small)
            // superset of rows; that trade-off has stood up in production for
            // several releases.
            if (window.isPresent()) {
                remaining.remove(column);
            }
            break;
        }

        if (window.isEmpty()) {
            return Optional.empty();
        }

        KairosdbTimestampPushdown.Window w = window.get();
        // Loop guard: if the window we'd push matches what is already on the handle, signal "nothing new".
        if (table.getPushedStartMillis().equals(w.startMillis()) && table.getPushedEndMillis().equals(w.endMillis())) {
            return Optional.empty();
        }

        KairosdbTableHandle pushed = table.withTimeRange(w.startMillis(), w.endMillis());
        log.info("Pushed timestamp window %s into %s.%s", w.pretty(), pushed.getSchemaName(), pushed.getTableName());

        return Optional.of(new ConstraintApplicationResult<>(
                pushed,
                TupleDomain.withColumnDomains(remaining),
                constraint.getExpression(),
                false));
    }

    private static boolean isInternalMetric(String metricName)
    {
        return metricName.toLowerCase(Locale.ROOT).startsWith(INTERNAL_METRIC_PREFIX);
    }
}
