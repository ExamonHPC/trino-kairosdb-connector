package io.examon.trino.kairosdb_connector;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Locks in the JSON serialisation contract for the two objects that cross
 * the coordinator -> worker boundary at query time.  If either the field
 * names or the shape change here, an in-flight query in a
 * heterogeneous-version cluster would silently lose pushed-down state -
 * hence the test.
 *
 * <p>{@link JsonCodec} is the same Jackson configuration Trino itself
 * wires up for serialising connector handles, so this test exercises the
 * exact code path the cluster uses, including {@code Optional} support.
 */
final class TestSerialization
{
    private static final JsonCodec<KairosdbTableHandle> TABLE_HANDLE_CODEC =
            JsonCodec.jsonCodec(KairosdbTableHandle.class);
    private static final JsonCodec<KairosdbSplit> SPLIT_CODEC =
            JsonCodec.jsonCodec(KairosdbSplit.class);

    @Test
    void emptyTableHandleRoundTrip()
    {
        KairosdbTableHandle original = new KairosdbTableHandle("kdb", "kairosdb", "ts_test");

        KairosdbTableHandle round = TABLE_HANDLE_CODEC.fromJson(TABLE_HANDLE_CODEC.toJson(original));

        assertThat(round).isEqualTo(original);
        assertThat(round.getPushedTagFilters()).isEmpty();
        assertThat(round.getPushedAggregators()).isEmpty();
        assertThat(round.getPushedLimit()).isEmpty();
    }

    @Test
    void fullyPushedTableHandleRoundTrip()
    {
        Map<String, List<String>> tags = ImmutableMap.of(
                "Host", ImmutableList.of("n01", "n02"),
                "DataCenter", ImmutableList.of("east"));
        KairosdbTableHandle original = new KairosdbTableHandle(
                "kdb",
                "kairosdb",
                "ts_test",
                Optional.of(1_700_000_000_000L),
                Optional.of(1_700_000_300_000L),
                tags,
                Optional.of(42L),
                ImmutableList.of("sum;1h;start_time", "avg;5m;sampling"));

        KairosdbTableHandle round = TABLE_HANDLE_CODEC.fromJson(TABLE_HANDLE_CODEC.toJson(original));

        assertThat(round).isEqualTo(original);
        assertThat(round.getPushedStartMillis()).contains(1_700_000_000_000L);
        assertThat(round.getPushedEndMillis()).contains(1_700_000_300_000L);
        assertThat(round.getPushedTagFilters()).containsAllEntriesOf(tags);
        assertThat(round.getPushedLimit()).contains(42L);
        assertThat(round.getPushedAggregators())
                .containsExactly("sum;1h;start_time", "avg;5m;sampling");
    }

    @Test
    void splitRoundTrip()
    {
        KairosdbSplit original = new KairosdbSplit(
                "kdb",
                "kairosdb",
                "ts_test",
                1_700_000_000_000L,
                1_700_000_300_000L,
                ImmutableMap.of("host", ImmutableList.of("n01")),
                Optional.of(100L),
                ImmutableList.of("sum;1h;start_time"));

        KairosdbSplit round = SPLIT_CODEC.fromJson(SPLIT_CODEC.toJson(original));

        assertThat(round.getConnectorId()).isEqualTo("kdb");
        assertThat(round.getSchemaName()).isEqualTo("kairosdb");
        assertThat(round.getTableName()).isEqualTo("ts_test");
        assertThat(round.getStartMillis()).isEqualTo(1_700_000_000_000L);
        assertThat(round.getEndMillis()).isEqualTo(1_700_000_300_000L);
        assertThat(round.getTagFilters()).containsExactlyEntriesOf(
                ImmutableMap.of("host", ImmutableList.of("n01")));
        assertThat(round.getLimit()).contains(100L);
        assertThat(round.getAggregators()).containsExactly("sum;1h;start_time");
        assertThat(round.getAddresses()).isEmpty();
    }

    @Test
    void splitWithoutOptionalFieldsRoundTrips()
    {
        // Mimic the path that the split manager actually exercises for
        // time-sliced (no aggregator, no limit) splits.
        KairosdbSplit original = new KairosdbSplit(
                "kdb",
                "kairosdb",
                "ts_test",
                1_700_000_000_000L,
                1_700_000_300_000L,
                ImmutableMap.of(),
                Optional.empty(),
                ImmutableList.of());

        KairosdbSplit round = SPLIT_CODEC.fromJson(SPLIT_CODEC.toJson(original));

        assertThat(round.getTagFilters()).isEmpty();
        assertThat(round.getLimit()).isEmpty();
        assertThat(round.getAggregators()).isEmpty();
    }
}
