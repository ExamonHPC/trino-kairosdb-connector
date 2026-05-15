package io.examon.trino.kairosdb_connector;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;

import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Identifies the metric a query targets and carries any predicate state the
 * metadata layer has pushed down to KairosDB.
 *
 * <p>Currently pushed: timestamp range.  Tag filters, LIMIT and sampling
 * aggregators land in follow-up commits.  Each pushed-down piece is stored
 * as a plain JSON-serialisable primitive (no opaque builder objects) so the
 * handle round-trips cleanly between coordinator and worker.
 */
public final class KairosdbTableHandle
        implements ConnectorTableHandle
{
    private final String connectorId;
    private final String schemaName;
    private final String tableName;
    private final Optional<Long> pushedStartMillis;
    private final Optional<Long> pushedEndMillis;

    @JsonCreator
    public KairosdbTableHandle(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("pushedStartMillis") Optional<Long> pushedStartMillis,
            @JsonProperty("pushedEndMillis") Optional<Long> pushedEndMillis)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.pushedStartMillis = requireNonNull(pushedStartMillis, "pushedStartMillis is null");
        this.pushedEndMillis = requireNonNull(pushedEndMillis, "pushedEndMillis is null");
    }

    public KairosdbTableHandle(String connectorId, String schemaName, String tableName)
    {
        this(connectorId, schemaName, tableName, Optional.empty(), Optional.empty());
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

    @JsonProperty
    public Optional<Long> getPushedStartMillis()
    {
        return pushedStartMillis;
    }

    @JsonProperty
    public Optional<Long> getPushedEndMillis()
    {
        return pushedEndMillis;
    }

    public KairosdbTableHandle withTimeRange(Optional<Long> startMillis, Optional<Long> endMillis)
    {
        return new KairosdbTableHandle(connectorId, schemaName, tableName, startMillis, endMillis);
    }

    public SchemaTableName toSchemaTableName()
    {
        return new SchemaTableName(schemaName, tableName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(connectorId, schemaName, tableName, pushedStartMillis, pushedEndMillis);
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
                && Objects.equals(tableName, other.tableName)
                && Objects.equals(pushedStartMillis, other.pushedStartMillis)
                && Objects.equals(pushedEndMillis, other.pushedEndMillis);
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder()
                .append(connectorId).append(':').append(schemaName).append(':').append(tableName);
        if (pushedStartMillis.isPresent() || pushedEndMillis.isPresent()) {
            sb.append(" time=[")
                    .append(pushedStartMillis.map(String::valueOf).orElse("-"))
                    .append(", ")
                    .append(pushedEndMillis.map(String::valueOf).orElse("-"))
                    .append(']');
        }
        return sb.toString();
    }
}
