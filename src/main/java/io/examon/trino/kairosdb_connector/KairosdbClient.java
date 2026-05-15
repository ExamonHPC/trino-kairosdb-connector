package io.examon.trino.kairosdb_connector;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Suppliers;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.examon.trino.kairosdb_connector.KairosdbResponses.MetricNamesResponse;
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
import java.util.LinkedHashSet;
import java.util.List;
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

    private static final MediaType JSON = MediaType.get("application/json; charset=utf-8");

    // The KairosDB /query/tags endpoint requires a time range.  We have no way
    // to know in advance how far back a given metric has been written, so we
    // probe in 30-day windows for up to ~10 years until we see at least one
    // tag.  This same scheme has been in production for a long time and we
    // keep the constants identical here.
    private static final int TAG_LOOKBACK_WINDOW_DAYS = 30;
    private static final int TAG_LOOKBACK_MAX_WINDOWS = 120;

    private final OkHttpClient httpClient;
    private final HttpUrl baseUrl;
    private final ObjectMapper jsonMapper;
    private final KairosdbConfig config;
    private final Supplier<List<String>> metricNamesCache;
    private final Cache<String, List<ColumnMetadata>> columnsByMetricCache;

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
        this.columnsByMetricCache = CacheBuilder.newBuilder()
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
     * published (in discovery order), then the synthetic {@code timeStamp}
     * column, then the synthetic {@code value} column.
     *
     * <p>Results are cached per metric for {@code kairosdb.metadata.cache-ttl}.
     */
    public List<ColumnMetadata> getColumns(String metricName)
    {
        try {
            return columnsByMetricCache.get(metricName, () -> discoverColumns(metricName));
        }
        catch (ExecutionException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            if (cause instanceof TrinoException trino) {
                throw trino;
            }
            throw new TrinoException(KAIROSDB_METRICS_RETRIEVE_ERROR,
                    "Failed to load columns for metric " + metricName, cause);
        }
    }

    private List<ColumnMetadata> discoverColumns(String metricName)
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

        ImmutableList.Builder<ColumnMetadata> columns = ImmutableList.builder();
        for (String tag : tagKeys) {
            columns.add(new ColumnMetadata(tag, VarcharType.createUnboundedVarcharType()));
        }
        columns.add(new ColumnMetadata("timeStamp", timestampColumnType()));
        columns.add(new ColumnMetadata("value", VarcharType.createUnboundedVarcharType()));
        return columns.build();
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

    private static HttpUrl httpUrlFromUri(URI uri)
    {
        HttpUrl parsed = HttpUrl.get(uri);
        if (parsed == null) {
            throw new IllegalArgumentException("Invalid kairosdb.url: " + uri);
        }
        return parsed;
    }
}
