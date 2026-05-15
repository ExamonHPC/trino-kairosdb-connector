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
}
