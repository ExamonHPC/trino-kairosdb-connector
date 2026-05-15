package io.examon.trino.kairosdb_connector;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * Identifies the metric a query targets.
 *
 * <p>At this point the handle is metadata-only.  Predicate state (timestamp
 * range, pushed-down tag filters, LIMIT, sampling aggregators) is added in
 * later commits.
 */
public final class KairosdbTableHandle
        implements ConnectorTableHandle
{
    private final String connectorId;
    private final String schemaName;
    private final String tableName;

    @JsonCreator
    public KairosdbTableHandle(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public String getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    public SchemaTableName toSchemaTableName()
    {
        return new SchemaTableName(schemaName, tableName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(connectorId, schemaName, tableName);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        KairosdbTableHandle other = (KairosdbTableHandle) obj;
        return Objects.equals(connectorId, other.connectorId)
                && Objects.equals(schemaName, other.schemaName)
                && Objects.equals(tableName, other.tableName);
    }

    @Override
    public String toString()
    {
        return connectorId + ":" + schemaName + ":" + tableName;
    }
}
