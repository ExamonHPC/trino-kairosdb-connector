package io.examon.trino.kairosdb_connector;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSplit;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * One slice of a metric query, sized to {@code kairosdb.split-size}.
 *
 * <p>The slice's {@code [startMillis, endMillis)} window is closed at the
 * coordinator (in the split manager) and shipped to whichever worker Trino
 * picks.  KairosDB is reachable from every Trino worker, so we expose no host
 * preferences ({@link #getAddresses()} is empty) and Trino is free to
 * schedule us anywhere.
 */
public final class KairosdbSplit
        implements ConnectorSplit
{
    private final String connectorId;
    private final String schemaName;
    private final String tableName;
    private final long startMillis;
    private final long endMillis;

    @JsonCreator
    public KairosdbSplit(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("startMillis") long startMillis,
            @JsonProperty("endMillis") long endMillis)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.startMillis = startMillis;
        this.endMillis = endMillis;
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
    public long getStartMillis()
    {
        return startMillis;
    }

    @JsonProperty
    public long getEndMillis()
    {
        return endMillis;
    }

    @Override
    public List<HostAddress> getAddresses()
    {
        return ImmutableList.of();
    }

    @Override
    public String toString()
    {
        return "KairosdbSplit{" + schemaName + "." + tableName + " [" + startMillis + ", " + endMillis + ")}";
    }
}
