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

    // Tag discovery: see discoverSchema() for the full design discussion.
    // The KairosDB /query/tags endpoint requires a time range, and there is
    // no out-of-band way to know how far back a metric has been written, so
    // we probe in fixed 30-day jumps for up to 120 iterations (~10 years).
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
    private final Supplier<KairosdbMetricView> metricViewCache;
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
        this.metricViewCache = Suppliers.memoizeWithExpiration(this::buildMetricView, ttlMillis, TimeUnit.MILLISECONDS);
        this.schemaByMetricCache = CacheBuilder.newBuilder()
                .expireAfterWrite(ttlMillis, TimeUnit.MILLISECONDS)
                .build();
    }

    /**
     * All Trino-side metric names currently visible to SQL (memoised).
     *
     * <p>For every KairosDB metric whose lowercase form is unique this is
     * just that lowercase form (e.g. {@code sys.load}, or {@code sys.mem}
     * for a KairosDB original of {@code Sys.Mem}).  When two or more
     * KairosDB metrics share a lowercase form (e.g. {@code pue} and
     * {@code Pue}), <em>every</em> member of the collision group is
     * exposed under a deterministic hash-suffixed name like
     * {@code pue__a1b2c3} - see {@link KairosdbMetricView} for the rule
     * and the rationale.
     */
    public List<String> listMetricNames()
    {
        return metricViewCache.get().trinoSideNames();
    }

    /**
     * Translates a Trino-side identifier (always lowercase, as Trino's
     * SPI lowercases {@link io.trino.spi.connector.SchemaTableName} before
     * the connector ever sees it) to the case-preserving KairosDB metric
     * name suitable for HTTP requests.
     *
     * <p>For collision-group members the input is the mangled form; the
     * unmangled lowercase form returns empty so that {@code SELECT * FROM
     * pue} fails loudly when both {@code pue} and {@code Pue} exist in
     * KairosDB rather than silently routing to one variant.  When no
     * collision exists, the lowercase form maps to the (possibly mixed-case)
     * original via the case-insensitive fallback.
     */
    public Optional<String> resolveTableName(String requested)
    {
        return metricViewCache.get().resolve(requested);
    }

    /**
     * Auto-generated table comment for collision-group members (returns
     * empty for unmangled metrics).  Surfaces the original KairosDB name
     * via {@code SHOW CREATE TABLE} and {@code system.metadata.table_comments}
     * so users can map mangled SQL identifiers back to their KairosDB
     * source without out-of-band tooling.
     */
    public Optional<String> getTableComment(String kairosOriginalName)
    {
        return metricViewCache.get().commentFor(kairosOriginalName);
    }

    /**
     * Inverse of {@link #resolveTableName(String)}: given a case-preserving
     * KairosDB metric name, returns the Trino-side name that addresses it.
     * Empty if the metric is not currently exposed.  Used when constructing
     * a {@link KairosdbTableHandle} so that
     * {@code ConnectorTableHandle.toSchemaTableName()} matches the SQL
     * identifier the user typed.
     */
    public Optional<String> trinoSideNameOf(String kairosOriginalName)
    {
        return metricViewCache.get().trinoSideNameOf(kairosOriginalName);
    }

    private KairosdbMetricView buildMetricView()
    {
        List<String> raw = fetchMetricNames();
        KairosdbMetricView view = KairosdbMetricView.build(raw, config.isCaseInsensitiveNameMatching());
        if (view.hasAnyCollision()) {
            log.info("KairosDB metric list has case collisions; affected metrics are exposed under hash-suffixed names. " +
                    "Inspect via SHOW TABLES + system.metadata.table_comments to discover the mapping.");
        }
        return view;
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

    /**
     * Discovers the tag schema of one metric by probing the KairosDB
     * {@code /query/tags} endpoint backwards through time.
     *
     * <p><b>Algorithm</b>:
     *
     * <pre>{@code
     *   cursor = now
     *   repeat up to 120 times:
     *     start = cursor - 30 days
     *     ask KairosDB: "for metric M, what tags exist with start_absolute=start?"
     *       (note: NO end_absolute -- KairosDB defaults end to "current time",
     *        so each probe is an EXPANDING window anchored at now)
     *     if any tags returned: keep them, stop.
     *     cursor = start
     * }</pre>
     *
     * The query KairosDB sees on iteration k therefore covers
     * {@code [now - k * 30d, now]}, not a sliding 30-day slice.
     *
     * <p><b>Why this shape</b>: the common case is a "live" metric with at
     * least one datapoint in the last 30 days, so the very first probe
     * succeeds and the loop exits.  The remaining tail caters to metrics
     * that wrote data once a long time ago and went silent.  "Latest
     * window wins" is a useful semantic byproduct: because the probes
     * start at now, any tags currently in use - even if the metric's
     * schema has evolved over the years - are caught first.
     *
     * <p><b>Worst case</b>: a metric whose only datapoint is from years
     * ago triggers many probes, each scanning more of KairosDB's index
     * than the previous (the expanding window).  This is a known
     * property of the algorithm.
     *
     * <p><b>Possible future improvements</b>:
     * <ul>
     *   <li><i>Exponential cadence</i> instead of fixed 30-day jumps -
     *       e.g. probe {@code 1h, 1d, 30d, 365d, 10y}.  Caps the loop at
     *       ~5 iterations and turns "live metric tag discovery" from a
     *       30-day index scan into a 1-hour scan (a ~720x speed-up on
     *       the common path).  Trivial to make configurable via a
     *       comma-separated {@code kairosdb.tag-discovery.lookback}.</li>
     *   <li><i>Disjoint sliding windows</i> - same exponential cadence
     *       but each probe's window is non-overlapping, capping total
     *       worst-case scan at ~10 years.  Slightly different semantics
     *       (loses the redundancy of always covering "now"), so deserves
     *       its own evaluation against representative datasets.</li>
     *   <li><i>Random-sample-then-refine</i> - probe a handful of random
     *       short windows across the metric's lifetime.  If they all
     *       return the same tag set, return it.  If they differ, drill
     *       down on the boundary to surface schema evolution explicitly.
     *       Useful when tags genuinely change over time and we care
     *       about exposing the union vs. just the latest.</li>
     *   <li><i>Server-side last-write hint</i> - if a future KairosDB
     *       version exposes a per-metric "last written at" attribute,
     *       we can target a single probe at that timestamp and skip the
     *       loop entirely.</li>
     * </ul>
     */
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
            // Two ways to land here: (a) the metric genuinely has no tags
            // (uncommon but valid in KairosDB), or (b) the metric is older
            // than the 10-year ceiling.  Both produce a tagless schema with
            // just the synthetic timestamp/value columns, which is a
            // reasonable degraded mode - the user can still SELECT, just
            // with no tag dimension.  We log this at DEBUG because case
            // (a) is by far the more common cause.
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

        // The sampling_aggregator hidden column is a control channel: users
        // push KairosDB sampling aggregators by writing predicates on it
        // (WHERE sampling_aggregator = 'sum;1h;start_time' or IN-lists of
        // such specs).  It has no underlying data, never appears in
        // SELECT *, and is dropped from the query's projection list by the
        // setHidden flag.  KairosdbMetadata.applyFilter recognises the
        // predicate, validates each spec, and stashes the list on the
        // table handle for the worker side to translate into a JSON
        // aggregators block.
        columns.add(ColumnMetadata.builder()
                .setName(KairosdbSamplingConstants.SAMPLING_AGGREGATOR)
                .setType(VarcharType.createUnboundedVarcharType())
                .setHidden(true)
                .build());

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
     *
     * <p>{@code limit}, when present, caps the number of datapoints KairosDB
     * returns for this metric (i.e. it is rendered as {@code "limit": N} on
     * the metric block).  The cap is best-effort: KairosDB may return fewer
     * rows if the underlying data is sparser than {@code limit}, and Trino
     * keeps its own LIMIT operator above the connector for that reason
     * (see {@code applyLimit}).
     *
     * <p>{@code aggregators} is the ordered list of {@code type;interval;alignment}
     * specs sourced from the hidden {@code sampling_aggregator} column.  Each
     * spec is re-parsed and serialised into a KairosDB aggregator JSON
     * object; an entry that has somehow survived but cannot be re-parsed is
     * logged and skipped (lenient: a malformed entry never aborts the
     * whole query).
     */
    public List<QueryDatapointsResponse.DataResult> queryDatapoints(
            String metricName,
            long startMillis,
            long endMillis,
            Map<String, List<String>> tagFilters,
            Optional<Long> limit,
            List<String> aggregators)
    {
        List<String> tagKeys = getOriginalTagKeys(metricName);

        LinkedHashMap<String, Object> metric = new LinkedHashMap<>();
        metric.put("name", metricName);
        if (tagFilters != null && !tagFilters.isEmpty()) {
            metric.put("tags", tagFilters);
        }
        limit.ifPresent(l -> metric.put("limit", l));
        List<Map<String, Object>> aggregatorJson = buildAggregatorJson(aggregators);
        if (!aggregatorJson.isEmpty()) {
            metric.put("aggregators", aggregatorJson);
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

        log.debug("Querying KairosDB: metric=%s window=[%d,%d] tags=%s limit=%s aggregators=%s group_by=%s",
                metricName, startMillis, endMillis, tagFilters, limit.orElse(null), aggregators, tagKeys);

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

    /**
     * Translates already-validated {@code type;interval;alignment} specs into
     * the JSON aggregator objects KairosDB expects:
     *
     * <pre>{@code
     * { "name": "sum",
     *   "sampling": {"value": 1, "unit": "hours"},
     *   "align_start_time": true }
     * }</pre>
     *
     * <p>Re-parsing here (rather than carrying parsed POJOs through the
     * handle/split) keeps the serialised form a stable list-of-strings.  A
     * spec that fails to re-parse is dropped with a warning rather than
     * failing the query: the same spec passed validation in the
     * coordinator earlier, so re-failing here would be fatal for no good
     * reason.
     */
    private static List<Map<String, Object>> buildAggregatorJson(List<String> specs)
    {
        if (specs == null || specs.isEmpty()) {
            return ImmutableList.of();
        }
        ImmutableList.Builder<Map<String, Object>> out = ImmutableList.builder();
        for (String spec : specs) {
            KairosdbSamplingConstants.ParsedAggregator parsed;
            try {
                parsed = KairosdbSamplingConstants.parse(spec);
            }
            catch (IllegalArgumentException ex) {
                log.warn("Dropping unparseable sampling aggregator spec '%s': %s", spec, ex.getMessage());
                continue;
            }
            LinkedHashMap<String, Object> agg = new LinkedHashMap<>();
            agg.put("name", parsed.type());
            agg.put("sampling", Map.of(
                    "value", parsed.interval().value(),
                    "unit", parsed.interval().unit().jsonValue()));
            switch (parsed.alignment()) {
                case "start_time" -> agg.put("align_start_time", true);
                case "end_time" -> agg.put("align_end_time", true);
                case "sampling" -> agg.put("align_sampling", true);
                case "none" -> { /* no alignment flag */ }
                default -> { /* parse() already validated, defensive only */ }
            }
            out.add(agg);
        }
        return out.build();
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
