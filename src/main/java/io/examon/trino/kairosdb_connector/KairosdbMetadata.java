package io.examon.trino.kairosdb_connector;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.SchemaTableName;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Placeholder metadata implementation.
 *
 * <p>At this stage of the port the connector loads, advertises the
 * {@value KairosdbNameSpace#SCHEMA} schema, and lists zero tables.  Subsequent
 * commits introduce table discovery, column discovery, and predicate
 * pushdown.
 */
public class KairosdbMetadata
        implements ConnectorMetadata
{
    private final KairosdbConnectorId connectorId;

    @Inject
    public KairosdbMetadata(KairosdbConnectorId connectorId)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
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
        // Table discovery is added in a follow-up commit.
        return null;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        throw new UnsupportedOperationException("getTableMetadata is implemented in a follow-up commit");
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        return ImmutableList.of();
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        throw new UnsupportedOperationException("getColumnHandles is implemented in a follow-up commit");
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        throw new UnsupportedOperationException("getColumnMetadata is implemented in a follow-up commit");
    }
}
