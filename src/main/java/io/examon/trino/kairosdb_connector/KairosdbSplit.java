package io.examon.trino.kairosdb_connector;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSplit;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * One slice of a metric query, sized to {@code kairosdb.split-size}.
 *
 * <p>The slice's inclusive {@code [startMillis, endMillis]} window and any
 * pushed-down tag filters are closed at the coordinator (in the split
 * manager) and shipped to whichever worker Trino picks.  KairosDB is
 * reachable from every Trino worker, so we expose no host preferences
 * ({@link #getAddresses()} is empty) and Trino is free to schedule us
 * anywhere.
 */
public final class KairosdbSplit
        implements ConnectorSplit
{
    private final String connectorId;
    private final String schemaName;
    private final String tableName;
    private final long startMillis;
    private final long endMillis;
    private final Map<String, List<String>> tagFilters;
    private final Optional<Long> limit;

    @JsonCreator
    public KairosdbSplit(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("startMillis") long startMillis,
            @JsonProperty("endMillis") long endMillis,
            @JsonProperty("tagFilters") Map<String, List<String>> tagFilters,
            @JsonProperty("limit") Optional<Long> limit)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.startMillis = startMillis;
        this.endMillis = endMillis;
        this.tagFilters = copyImmutable(tagFilters);
        this.limit = requireNonNull(limit, "limit is null");
    }

    public KairosdbSplit(String connectorId, String schemaName, String tableName, long startMillis, long endMillis)
    {
        this(connectorId, schemaName, tableName, startMillis, endMillis, ImmutableMap.of(), Optional.empty());
    }

    public KairosdbSplit(
            String connectorId,
            String schemaName,
            String tableName,
            long startMillis,
            long endMillis,
            Map<String, List<String>> tagFilters)
    {
        this(connectorId, schemaName, tableName, startMillis, endMillis, tagFilters, Optional.empty());
    }

    private static Map<String, List<String>> copyImmutable(Map<String, List<String>> source)
    {
        if (source == null || source.isEmpty()) {
            return ImmutableMap.of();
        }
        ImmutableMap.Builder<String, List<String>> b = ImmutableMap.builder();
        for (Map.Entry<String, List<String>> e : source.entrySet()) {
            b.put(e.getKey(), ImmutableList.copyOf(e.getValue()));
        }
        return b.buildOrThrow();
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

    @JsonProperty
    public Map<String, List<String>> getTagFilters()
    {
        return tagFilters;
    }

    @JsonProperty
    public Optional<Long> getLimit()
    {
        return limit;
    }

    @Override
    public List<HostAddress> getAddresses()
    {
        return ImmutableList.of();
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("KairosdbSplit{")
                .append(schemaName).append('.').append(tableName)
                .append(" [").append(startMillis).append(", ").append(endMillis).append(']');
        if (!tagFilters.isEmpty()) {
            sb.append(' ').append(tagFilters);
        }
        limit.ifPresent(l -> sb.append(" limit=").append(l));
        return sb.append('}').toString();
    }
}
