package io.examon.trino.kairosdb_connector;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;
import io.trino.plugin.base.TypeDeserializer;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeId;
import io.trino.spi.type.VarcharType;
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

    /**
     * Column handles carry a Trino {@link Type} field, which only deserialises
     * with a {@link TypeDeserializer} that can look up the type by id.  In
     * production this is wired by Trino's {@code TypeDeserializerModule}; for
     * tests we hand-roll a minimal lookup over the few types this connector
     * emits.  This mirrors the codec the coordinator-worker dispatch uses
     * for real - it has caught at least one historical regression where the
     * handle accidentally embedded a non-JSON-friendly object.
     */
    private static final JsonCodec<KairosdbColumnHandle> COLUMN_HANDLE_CODEC = buildColumnHandleCodec();

    private static JsonCodec<KairosdbColumnHandle> buildColumnHandleCodec()
    {
        ObjectMapperProvider provider = new ObjectMapperProvider();
        provider.setJsonDeserializers(ImmutableMap.of(Type.class, new TypeDeserializer(id -> {
            String s = id.getId();
            if (BigintType.BIGINT.getTypeId().getId().equals(s)) {
                return BigintType.BIGINT;
            }
            if (s.equals(TimestampType.TIMESTAMP_MILLIS.getTypeId().getId())) {
                return TimestampType.TIMESTAMP_MILLIS;
            }
            if (s.startsWith("timestamp(") && s.contains("with time zone")) {
                return TimestampWithTimeZoneType.createTimestampWithTimeZoneType(3);
            }
            if (s.startsWith("varchar")) {
                return VarcharType.createUnboundedVarcharType();
            }
            throw new IllegalArgumentException("Unsupported type for test codec: " + s);
        })));
        return new JsonCodecFactory(provider).jsonCodec(KairosdbColumnHandle.class);
    }

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

    @Test
    void columnHandleRoundTrip()
    {
        // Exercise every type variant the connector emits.
        for (Type t : List.of(
                BigintType.BIGINT,
                TimestampType.TIMESTAMP_MILLIS,
                TimestampWithTimeZoneType.createTimestampWithTimeZoneType(3),
                VarcharType.createUnboundedVarcharType())) {
            KairosdbColumnHandle original = new KairosdbColumnHandle("kdb", "host", t, 0);
            KairosdbColumnHandle round = COLUMN_HANDLE_CODEC.fromJson(COLUMN_HANDLE_CODEC.toJson(original));
            assertThat(round).isEqualTo(original);
            assertThat(round.getColumnType()).isEqualTo(t);
            assertThat(round.getOrdinalPosition()).isEqualTo(0);
        }
    }
}
