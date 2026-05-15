package io.examon.trino.kairosdb_connector;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Identifies the metric a query targets and carries any predicate state the
 * metadata layer has pushed down to KairosDB.
 *
 * <p>Currently pushed: timestamp range, tag filters, LIMIT and sampling
 * aggregators.  Each pushed-down piece is stored as a plain
 * JSON-serialisable primitive (no opaque builder objects) so the handle
 * round-trips cleanly between coordinator and worker.
 *
 * <p>Tag filters use KairosDB-side <em>original</em> tag names as keys (case
 * preserving) because that's the form KairosDB expects in the query JSON;
 * Trino-side identifiers are always lowercased and translated when the
 * filter is constructed.  Within a tag, the list of values has OR semantics
 * (matches any); across tags the semantics is AND (all tags must match).
 *
 * <p>Sampling aggregators are stored as the raw {@code type;interval;alignment}
 * spec strings (already validated by {@link KairosdbSamplingConstants}) in
 * the exact order they will be applied; the on-the-wire JSON shape is built
 * by {@link KairosdbClient}.
 */
public final class KairosdbTableHandle
        implements ConnectorTableHandle
{
    private final String connectorId;
    private final String schemaName;
    private final String tableName;
    private final Optional<Long> pushedStartMillis;
    private final Optional<Long> pushedEndMillis;
    private final Map<String, List<String>> pushedTagFilters;
    private final Optional<Long> pushedLimit;
    private final List<String> pushedAggregators;

    @JsonCreator
    public KairosdbTableHandle(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("pushedStartMillis") Optional<Long> pushedStartMillis,
            @JsonProperty("pushedEndMillis") Optional<Long> pushedEndMillis,
            @JsonProperty("pushedTagFilters") Map<String, List<String>> pushedTagFilters,
            @JsonProperty("pushedLimit") Optional<Long> pushedLimit,
            @JsonProperty("pushedAggregators") List<String> pushedAggregators)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.pushedStartMillis = requireNonNull(pushedStartMillis, "pushedStartMillis is null");
        this.pushedEndMillis = requireNonNull(pushedEndMillis, "pushedEndMillis is null");
        this.pushedTagFilters = copyImmutable(pushedTagFilters);
        this.pushedLimit = requireNonNull(pushedLimit, "pushedLimit is null");
        this.pushedAggregators = pushedAggregators == null ? ImmutableList.of() : ImmutableList.copyOf(pushedAggregators);
    }

    public KairosdbTableHandle(String connectorId, String schemaName, String tableName)
    {
        this(connectorId, schemaName, tableName, Optional.empty(), Optional.empty(), ImmutableMap.of(), Optional.empty(), ImmutableList.of());
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
    public Optional<Long> getPushedStartMillis()
    {
        return pushedStartMillis;
    }

    @JsonProperty
    public Optional<Long> getPushedEndMillis()
    {
        return pushedEndMillis;
    }

    @JsonProperty
    public Map<String, List<String>> getPushedTagFilters()
    {
        return pushedTagFilters;
    }

    @JsonProperty
    public Optional<Long> getPushedLimit()
    {
        return pushedLimit;
    }

    @JsonProperty
    public List<String> getPushedAggregators()
    {
        return pushedAggregators;
    }

    public KairosdbTableHandle withTimeRange(Optional<Long> startMillis, Optional<Long> endMillis)
    {
        return new KairosdbTableHandle(connectorId, schemaName, tableName, startMillis, endMillis, pushedTagFilters, pushedLimit, pushedAggregators);
    }

    public KairosdbTableHandle withTagFilters(Map<String, List<String>> tagFilters)
    {
        return new KairosdbTableHandle(connectorId, schemaName, tableName, pushedStartMillis, pushedEndMillis, tagFilters, pushedLimit, pushedAggregators);
    }

    public KairosdbTableHandle withLimit(Optional<Long> limit)
    {
        return new KairosdbTableHandle(connectorId, schemaName, tableName, pushedStartMillis, pushedEndMillis, pushedTagFilters, limit, pushedAggregators);
    }

    public KairosdbTableHandle withAggregators(List<String> aggregators)
    {
        return new KairosdbTableHandle(connectorId, schemaName, tableName, pushedStartMillis, pushedEndMillis, pushedTagFilters, pushedLimit, aggregators);
    }

    public SchemaTableName toSchemaTableName()
    {
        return new SchemaTableName(schemaName, tableName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(connectorId, schemaName, tableName, pushedStartMillis, pushedEndMillis, pushedTagFilters, pushedLimit, pushedAggregators);
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
                && Objects.equals(pushedEndMillis, other.pushedEndMillis)
                && Objects.equals(pushedTagFilters, other.pushedTagFilters)
                && Objects.equals(pushedLimit, other.pushedLimit)
                && Objects.equals(pushedAggregators, other.pushedAggregators);
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
        if (!pushedTagFilters.isEmpty()) {
            sb.append(" tags=").append(pushedTagFilters);
        }
        pushedLimit.ifPresent(l -> sb.append(" limit=").append(l));
        if (!pushedAggregators.isEmpty()) {
            sb.append(" aggregators=").append(pushedAggregators);
        }
        return sb.toString();
    }
}
