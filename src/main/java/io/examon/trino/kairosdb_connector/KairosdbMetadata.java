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
import io.trino.spi.connector.SchemaTableName;

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

    private static boolean isInternalMetric(String metricName)
    {
        return metricName.toLowerCase(Locale.ROOT).startsWith(INTERNAL_METRIC_PREFIX);
    }
}
