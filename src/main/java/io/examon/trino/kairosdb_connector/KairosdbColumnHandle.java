package io.examon.trino.kairosdb_connector;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.Type;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * Identity of a column inside a KairosDB metric.
 *
 * <p>Two handles compare equal if they share the same connector id and column
 * name; column type and ordinal position are derived from the metric and not
 * part of identity.  This mirrors the equality the old hand-written
 * KairosdbColumnHandle used.
 */
public final class KairosdbColumnHandle
        implements ColumnHandle
{
    private final String connectorId;
    private final String columnName;
    private final Type columnType;
    private final int ordinalPosition;

    @JsonCreator
    public KairosdbColumnHandle(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("columnName") String columnName,
            @JsonProperty("columnType") Type columnType,
            @JsonProperty("ordinalPosition") int ordinalPosition)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.columnName = requireNonNull(columnName, "columnName is null");
        this.columnType = requireNonNull(columnType, "columnType is null");
        this.ordinalPosition = ordinalPosition;
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public String getColumnName()
    {
        return columnName;
    }

    @JsonProperty
    public Type getColumnType()
    {
        return columnType;
    }

    @JsonProperty
    public int getOrdinalPosition()
    {
        return ordinalPosition;
    }

    public ColumnMetadata getColumnMetadata()
    {
        return new ColumnMetadata(columnName, columnType);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(connectorId, columnName);
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
        KairosdbColumnHandle other = (KairosdbColumnHandle) obj;
        return Objects.equals(connectorId, other.connectorId)
                && Objects.equals(columnName, other.columnName);
    }

    @Override
    public String toString()
    {
        return connectorId + ":" + columnName + ":" + columnType + ":" + ordinalPosition;
    }
}
