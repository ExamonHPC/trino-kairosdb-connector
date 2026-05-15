package io.examon.trino.kairosdb_connector;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Suppliers;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.examon.trino.kairosdb_connector.KairosdbResponses.MetricNamesResponse;
import io.examon.trino.kairosdb_connector.KairosdbResponses.QueryDatapointsResponse;
import io.examon.trino.kairosdb_connector.KairosdbResponses.QueryTagsResponse;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static io.examon.trino.kairosdb_connector.KairosdbErrorCode.KAIROSDB_METRICS_RETRIEVE_ERROR;
import static io.examon.trino.kairosdb_connector.KairosdbErrorCode.KAIROSDB_PARSE_ERROR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * HTTP client for KairosDB.
 *
 * <p>Bound as a singleton per connector instance.  Each catalog therefore has
 * its own OkHttp pool and its own metadata caches; two catalogs configured
 * against different KairosDB servers do not collide.
 *
 * <p>This commit only implements metadata-discovery endpoints:
 * <ul>
 *   <li>{@code GET /api/v1/metricnames} for the list of tables.</li>
 *   <li>{@code POST /api/v1/datapoints/query/tags} for the tag-key set used
 *       as columns.</li>
 * </ul>
 *
 * <p>Data-fetch ({@code POST /api/v1/datapoints/query}) is added in the next
 * commit.
 */
public class KairosdbClient
{
    private static final Logger log = Logger.get(KairosdbClient.class);

    private static final String METRIC_NAMES_PATH = "api/v1/metricnames";
    private static final String QUERY_TAGS_PATH = "api/v1/datapoints/query/tags";
    private static final String QUERY_PATH = "api/v1/datapoints/query";

    private static final MediaType JSON = MediaType.get("application/json; charset=utf-8");

    // The KairosDB /query/tags endpoint requires a time range.  We have no way
    // to know in advance how far back a given metric has been written, so we
    // probe in 30-day windows for up to ~10 years until we see at least one
    // tag.  This same scheme has been in production for a long time and we
    // keep the constants identical here.
    private static final int TAG_LOOKBACK_WINDOW_DAYS = 30;
    private static final int TAG_LOOKBACK_MAX_WINDOWS = 120;

    /**
     * Cached per-metric schema: the KairosDB-side tag names (original case,
     * used for {@code group_by} clauses and for tag predicate pushdown) and
     * the Trino-facing column list (lowercased).
     */
    public record MetricSchema(List<String> originalTagKeys, List<ColumnMetadata> columns) {}

    private final OkHttpClient httpClient;
    private final HttpUrl baseUrl;
    private final ObjectMapper jsonMapper;
    private final KairosdbConfig config;
    private final Supplier<List<String>> metricNamesCache;
    private final Cache<String, MetricSchema> schemaByMetricCache;

    @Inject
    public KairosdbClient(KairosdbConfig config)
    {
        this.config = requireNonNull(config, "config is null");
        this.baseUrl = httpUrlFromUri(config.getKairosdbUri());
        this.httpClient = new OkHttpClient.Builder()
                .readTimeout(config.getReadTimeout().toMillis(), TimeUnit.MILLISECONDS)
                .build();
        this.jsonMapper = new ObjectMapper();
        long ttlMillis = config.getMetadataCacheTtl().toMillis();
        this.metricNamesCache = Suppliers.memoizeWithExpiration(this::fetchMetricNames, ttlMillis, TimeUnit.MILLISECONDS);
        this.schemaByMetricCache = CacheBuilder.newBuilder()
                .expireAfterWrite(ttlMillis, TimeUnit.MILLISECONDS)
                .build();
    }

    /** All metric names KairosDB currently knows about (memoised). */
    public List<String> listMetricNames()
    {
        return metricNamesCache.get();
    }

    /**
     * Trino SQL standard table-name resolution: exact-case first, then
     * case-insensitive (controlled by {@code kairosdb.case-insensitive-name-matching}).
     */
    public Optional<String> resolveTableName(String requested)
    {
        List<String> names = listMetricNames();
        if (names.contains(requested)) {
            return Optional.of(requested);
        }
        if (!config.isCaseInsensitiveNameMatching()) {
            return Optional.empty();
        }
        for (String name : names) {
            if (name.equalsIgnoreCase(requested)) {
                return Optional.of(name);
            }
        }
        return Optional.empty();
    }

    /**
     * Returns the columns of a metric: every tag key the metric has ever
     * published (in discovery order), then the synthetic {@code timestamp}
     * column, then the synthetic {@code value} column.
     *
     * <p>Results are cached per metric for {@code kairosdb.metadata.cache-ttl}.
     */
    public List<ColumnMetadata> getColumns(String metricName)
    {
        return getSchema(metricName).columns();
    }

    /**
     * Returns the original (case-preserving) KairosDB tag keys for a metric.
     * Used to build {@code group_by} clauses and to translate Trino-side
     * lowercase identifiers back to KairosDB-side tag names when pushing
     * down predicates.
     */
    public List<String> getOriginalTagKeys(String metricName)
    {
        return getSchema(metricName).originalTagKeys();
    }

    private MetricSchema getSchema(String metricName)
    {
        try {
            return schemaByMetricCache.get(metricName, () -> discoverSchema(metricName));
        }
        catch (ExecutionException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            if (cause instanceof TrinoException trino) {
                throw trino;
            }
            throw new TrinoException(KAIROSDB_METRICS_RETRIEVE_ERROR,
                    "Failed to load schema for metric " + metricName, cause);
        }
    }

    private MetricSchema discoverSchema(String metricName)
    {
        Set<String> tagKeys = new LinkedHashSet<>();
        Instant cursor = Instant.now();
        for (int window = 0; window < TAG_LOOKBACK_MAX_WINDOWS; window++) {
            Instant start = cursor.minus(Duration.ofDays(TAG_LOOKBACK_WINDOW_DAYS));
            Set<String> found = fetchTagKeys(metricName, start);
            if (!found.isEmpty()) {
                tagKeys.addAll(found);
                break;
            }
            cursor = start;
        }

        if (tagKeys.isEmpty()) {
            log.debug("No tag keys found for metric %s after walking back %d * %d days",
                    metricName, TAG_LOOKBACK_MAX_WINDOWS, TAG_LOOKBACK_WINDOW_DAYS);
        }

        // Trino's SPI forces every ColumnMetadata name to lowercase: the
        // ColumnMetadata constructor silently lowercases what you pass it
        // (verified empirically against 476, and tracked upstream by
        // https://github.com/trinodb/trino/issues/17 -- open since 2019).
        // We therefore cannot expose KairosDB's mixed-case tag names
        // (e.g. "DataCenter") through Trino's data dictionary.
        //
        // The "raw case parity" policy applies as far as Trino's SPI
        // permits:
        //   - the table name keeps its KairosDB case end-to-end (see
        //     resolveTableName), which Trino allows;
        //   - tag *values* are user data and Trino never normalises them,
        //     so they stay case-sensitive (WHERE tag = 'East' really
        //     matches 'East' and not 'east');
        //   - tag *names* are SQL identifiers and get lowercased: the
        //     metric's KairosDB-side names are preserved separately in
        //     originalTagKeys so the connector can address KairosDB
        //     correctly when grouping, pushing predicates, and reading
        //     tag values back out of responses.
        //
        // The two synthetic columns ("timestamp", "value") are introduced
        // by the connector itself and are intentionally lowercase.
        ImmutableList.Builder<ColumnMetadata> columns = ImmutableList.builder();
        for (String tag : tagKeys) {
            columns.add(new ColumnMetadata(tag.toLowerCase(Locale.ROOT), VarcharType.createUnboundedVarcharType()));
        }
        columns.add(new ColumnMetadata("timestamp", timestampColumnType()));
        columns.add(new ColumnMetadata("value", VarcharType.createUnboundedVarcharType()));

        return new MetricSchema(ImmutableList.copyOf(tagKeys), columns.build());
    }

    private Type timestampColumnType()
    {
        return switch (config.getTimestampFormat()) {
            case BIGINT -> BigintType.BIGINT;
            case TIMESTAMP_MILLIS -> TimestampType.TIMESTAMP_MILLIS;
            case TIMESTAMP_TZ -> TimestampWithTimeZoneType.createTimestampWithTimeZoneType(3);
        };
    }

    private List<String> fetchMetricNames()
    {
        HttpUrl url = baseUrl.newBuilder().addPathSegments(METRIC_NAMES_PATH).build();
        Request req = new Request.Builder().url(url).get().build();
        log.debug("Listing KairosDB metric names: %s", url);

        try (Response response = httpClient.newCall(req).execute()) {
            if (!response.isSuccessful()) {
                throw new TrinoException(KAIROSDB_METRICS_RETRIEVE_ERROR,
                        format("KairosDB returned HTTP %d when listing metric names", response.code()));
            }
            ResponseBody body = response.body();
            if (body == null) {
                throw new TrinoException(KAIROSDB_PARSE_ERROR, "Empty response while listing KairosDB metric names");
            }
            MetricNamesResponse parsed = jsonMapper.readValue(body.byteStream(), MetricNamesResponse.class);
            if (parsed.results == null) {
                return ImmutableList.of();
            }
            return ImmutableList.copyOf(parsed.results);
        }
        catch (IOException e) {
            throw new TrinoException(KAIROSDB_METRICS_RETRIEVE_ERROR,
                    "Error communicating with KairosDB at " + baseUrl, e);
        }
    }

    private Set<String> fetchTagKeys(String metricName, Instant startTime)
    {
        // POST /api/v1/datapoints/query/tags
        //   { "start_absolute": <millis>, "metrics": [ { "name": "<metricName>" } ] }
        Map<String, Object> request = Map.of(
                "start_absolute", startTime.toEpochMilli(),
                "metrics", List.of(Map.of("name", metricName)));

        HttpUrl url = baseUrl.newBuilder().addPathSegments(QUERY_TAGS_PATH).build();
        String json;
        try {
            json = jsonMapper.writeValueAsString(request);
        }
        catch (IOException e) {
            throw new TrinoException(KAIROSDB_PARSE_ERROR, "Failed to serialise tag-discovery request", e);
        }

        Request httpRequest = new Request.Builder()
                .url(url)
                .post(RequestBody.create(json, JSON))
                .build();

        try (Response response = httpClient.newCall(httpRequest).execute()) {
            if (!response.isSuccessful()) {
                throw new TrinoException(KAIROSDB_METRICS_RETRIEVE_ERROR,
                        format("KairosDB returned HTTP %d for tag discovery on metric %s",
                                response.code(), metricName));
            }
            ResponseBody body = response.body();
            if (body == null) {
                return Set.of();
            }
            QueryTagsResponse parsed = jsonMapper.readValue(body.byteStream(), QueryTagsResponse.class);
            return extractTagKeys(parsed);
        }
        catch (IOException e) {
            throw new TrinoException(KAIROSDB_METRICS_RETRIEVE_ERROR,
                    "Error communicating with KairosDB at " + baseUrl, e);
        }
    }

    private static Set<String> extractTagKeys(QueryTagsResponse parsed)
    {
        Set<String> keys = new LinkedHashSet<>();
        if (parsed.queries == null) {
            return keys;
        }
        for (QueryTagsResponse.Query query : parsed.queries) {
            if (query.results == null) {
                continue;
            }
            for (QueryTagsResponse.TagResult result : query.results) {
                if (result.tags != null) {
                    keys.addAll(result.tags.keySet());
                }
            }
        }
        return keys;
    }

    /**
     * Fetch the raw datapoints for a metric in the given time range, grouped
     * by every tag the metric publishes so that each entry in the returned
     * list corresponds to one distinct tag combination.  Without this
     * grouping, KairosDB returns every datapoint interleaved into a single
     * stream with a union of tag values, which makes it impossible to tell
     * which tag combination a given datapoint came from.
     *
     * <p>{@code tagFilters} is a possibly-empty map of
     * <em>original-case</em> KairosDB tag names to lists of admitted values
     * (OR semantics within a tag, AND semantics across tags).  Empty values
     * are dropped; an entirely empty map sends no {@code tags} block and the
     * metric is returned unfiltered.
     */
    public List<QueryDatapointsResponse.DataResult> queryDatapoints(
            String metricName,
            long startMillis,
            long endMillis,
            Map<String, List<String>> tagFilters)
    {
        List<String> tagKeys = getOriginalTagKeys(metricName);

        LinkedHashMap<String, Object> metric = new LinkedHashMap<>();
        metric.put("name", metricName);
        if (tagFilters != null && !tagFilters.isEmpty()) {
            metric.put("tags", tagFilters);
        }
        if (!tagKeys.isEmpty()) {
            metric.put("group_by", List.of(Map.of(
                    "name", "tag",
                    "tags", tagKeys)));
        }

        Map<String, Object> request = Map.of(
                "start_absolute", startMillis,
                "end_absolute", endMillis,
                "metrics", List.of(metric));

        log.debug("Querying KairosDB: metric=%s window=[%d,%d] tags=%s group_by=%s",
                metricName, startMillis, endMillis, tagFilters, tagKeys);

        HttpUrl url = baseUrl.newBuilder().addPathSegments(QUERY_PATH).build();
        String json;
        try {
            json = jsonMapper.writeValueAsString(request);
        }
        catch (IOException e) {
            throw new TrinoException(KAIROSDB_PARSE_ERROR, "Failed to serialise datapoint query", e);
        }

        Request httpRequest = new Request.Builder()
                .url(url)
                .post(RequestBody.create(json, JSON))
                .build();

        try (Response response = httpClient.newCall(httpRequest).execute()) {
            if (!response.isSuccessful()) {
                throw new TrinoException(KAIROSDB_METRICS_RETRIEVE_ERROR,
                        format("KairosDB returned HTTP %d querying metric %s", response.code(), metricName));
            }
            ResponseBody body = response.body();
            if (body == null) {
                return ImmutableList.of();
            }
            QueryDatapointsResponse parsed = jsonMapper.readValue(body.byteStream(), QueryDatapointsResponse.class);
            ImmutableList.Builder<QueryDatapointsResponse.DataResult> flattened = ImmutableList.builder();
            if (parsed.queries != null) {
                for (QueryDatapointsResponse.Query q : parsed.queries) {
                    if (q.results != null) {
                        flattened.addAll(q.results);
                    }
                }
            }
            return flattened.build();
        }
        catch (IOException e) {
            throw new TrinoException(KAIROSDB_METRICS_RETRIEVE_ERROR,
                    "Error communicating with KairosDB at " + baseUrl, e);
        }
    }

    private static HttpUrl httpUrlFromUri(URI uri)
    {
        HttpUrl parsed = HttpUrl.get(uri);
        if (parsed == null) {
            throw new IllegalArgumentException("Invalid kairosdb.url: " + uri);
        }
        return parsed;
    }
}
