package io.examon.trino.kairosdb_connector;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;
import java.util.Map;

/**
 * Container for Jackson DTOs that mirror the subset of KairosDB HTTP responses
 * we actually consume.  Kept in a single file because each DTO is trivial and
 * deserialised in only one place.
 */
final class KairosdbResponses
{
    private KairosdbResponses() {}

    /** Response from {@code GET /api/v1/metricnames}. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    static final class MetricNamesResponse
    {
        public List<String> results;
    }

    /** Response from {@code POST /api/v1/datapoints/query/tags}. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    static final class QueryTagsResponse
    {
        public List<Query> queries;

        @JsonIgnoreProperties(ignoreUnknown = true)
        static final class Query
        {
            public List<TagResult> results;
        }

        @JsonIgnoreProperties(ignoreUnknown = true)
        static final class TagResult
        {
            public String name;
            public Map<String, List<String>> tags;
        }
    }

    /** Response from {@code POST /api/v1/datapoints/query}. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    static final class QueryDatapointsResponse
    {
        public List<Query> queries;

        @JsonIgnoreProperties(ignoreUnknown = true)
        static final class Query
        {
            public int sample_size;
            public List<DataResult> results;
        }

        @JsonIgnoreProperties(ignoreUnknown = true)
        static final class DataResult
        {
            public String name;
            /**
             * Each value is {@code [epoch_millis, value]} where the value can
             * be a number or a string.  We keep Jackson's {@code Object}
             * deserialisation and render to text at row-emit time.
             */
            public List<List<Object>> values;
            public Map<String, List<String>> tags;
        }
    }
}
