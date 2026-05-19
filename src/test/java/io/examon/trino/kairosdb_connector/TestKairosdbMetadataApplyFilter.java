package io.examon.trino.kairosdb_connector;

import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slices;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.VarcharType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Metadata-level coverage for the strict pushdown semantics.
 *
 * <p>These tests exercise the full {@link KairosdbMetadata#applyFilter}
 * composition - both the pushed handle's state AND the returned remaining
 * {@link TupleDomain} - so they catch the "claim more than we can deliver"
 * class of bug that helper-level tests miss.
 *
 * <p>The {@link KairosdbClient} dependency is satisfied by a minimal
 * subclass that overrides only {@code getOriginalTagKeys}; no network I/O
 * happens during these tests.
 */
final class TestKairosdbMetadataApplyFilter
{
    private static final VarcharType VARCHAR = VarcharType.createUnboundedVarcharType();
    private static final BigintType BIGINT = BigintType.BIGINT;
    private static final long T_MS = 1_700_000_000_000L;
    private static final String TABLE = "cpu1_temp";
    private static final String SCHEMA = KairosdbNameSpace.SCHEMA;
    private static final String CONNECTOR = "kairosdb-test";

    private KairosdbMetadata metadata;
    private KairosdbColumnHandle timestampColumn;
    private KairosdbColumnHandle hostColumn;

    @BeforeEach
    void setUp()
    {
        KairosdbConfig config = new KairosdbConfig()
                .setKairosdbUri(URI.create("http://localhost:8080"));
        KairosdbClient client = new KairosdbClient(config) {
            @Override
            public List<String> getOriginalTagKeys(String metricName)
            {
                return List.of("host");
            }
        };
        metadata = new KairosdbMetadata(new KairosdbConnectorId(CONNECTOR), client);
        timestampColumn = new KairosdbColumnHandle(CONNECTOR, "timestamp", BIGINT, 0);
        hostColumn = new KairosdbColumnHandle(CONNECTOR, "host", VARCHAR, 1);
    }

    // -------------------------------------------------------------------
    // Tag pushdown
    // -------------------------------------------------------------------

    @Test
    void tagEquality_pushedAndRemoved()
    {
        ConstraintApplicationResult<ConnectorTableHandle> result = applyFilter(Map.of(
                hostColumn, Domain.singleValue(VARCHAR, Slices.utf8Slice("h1"))));

        KairosdbTableHandle pushed = (KairosdbTableHandle) result.getHandle();
        assertThat(pushed.getPushedTagFilters()).isEqualTo(Map.of("host", List.of("h1")));
        assertRemainingHasNoColumn(result, hostColumn);
    }

    @Test
    void tagInList_pushedAndRemoved()
    {
        ConstraintApplicationResult<ConnectorTableHandle> result = applyFilter(Map.of(
                hostColumn, Domain.multipleValues(VARCHAR, List.of(
                        Slices.utf8Slice("h1"),
                        Slices.utf8Slice("h2")))));

        KairosdbTableHandle pushed = (KairosdbTableHandle) result.getHandle();
        assertThat(pushed.getPushedTagFilters()).isEqualTo(Map.of("host", List.of("h1", "h2")));
        assertRemainingHasNoColumn(result, hostColumn);
    }

    @Test
    void tagRange_notPushedAndResidualPreserved()
    {
        // WHERE host > 'm' - KairosDB has no string range filter, so the
        // entire predicate stays residual.  No applyFilter result at all
        // (nothing changed in the handle).
        Optional<ConstraintApplicationResult<ConnectorTableHandle>> result = applyFilterOptional(Map.of(
                hostColumn,
                Domain.create(ValueSet.ofRanges(Range.greaterThan(VARCHAR, Slices.utf8Slice("m"))), false)));

        assertThat(result).isEmpty();
    }

    @Test
    void tagMixedEqualityAndRange_notPushed()
    {
        // host = 'h1' OR host > 'm' - the range part is unrepresentable,
        // so the whole predicate must stay residual.
        Domain d = Domain.create(
                ValueSet.ofRanges(
                        Range.equal(VARCHAR, Slices.utf8Slice("h1")),
                        Range.greaterThan(VARCHAR, Slices.utf8Slice("m"))),
                false);

        Optional<ConstraintApplicationResult<ConnectorTableHandle>> result =
                applyFilterOptional(Map.of(hostColumn, d));

        assertThat(result).isEmpty();
    }

    @Test
    void tagNotIn_notPushed()
    {
        // host != 'foo'
        Domain d = Domain.create(
                ValueSet.ofRanges(
                        Range.lessThan(VARCHAR, Slices.utf8Slice("foo")),
                        Range.greaterThan(VARCHAR, Slices.utf8Slice("foo"))),
                false);

        assertThat(applyFilterOptional(Map.of(hostColumn, d))).isEmpty();
    }

    // -------------------------------------------------------------------
    // Timestamp pushdown
    // -------------------------------------------------------------------

    @Test
    void timestampBetween_pushedExactlyAndRemoved()
    {
        long a = T_MS;
        long b = T_MS + 3_600_000L;
        Domain d = Domain.create(
                ValueSet.ofRanges(Range.range(BIGINT, a, true, b, true)),
                false);

        ConstraintApplicationResult<ConnectorTableHandle> result =
                applyFilter(Map.of(timestampColumn, d));

        KairosdbTableHandle pushed = (KairosdbTableHandle) result.getHandle();
        assertThat(pushed.getPushedStartMillis()).contains(a);
        assertThat(pushed.getPushedEndMillis()).contains(b);
        assertRemainingHasNoColumn(result, timestampColumn);
    }

    @Test
    void timestampSinglePoint_pushedExactlyAndRemoved()
    {
        ConstraintApplicationResult<ConnectorTableHandle> result =
                applyFilter(Map.of(timestampColumn, Domain.singleValue(BIGINT, T_MS)));

        KairosdbTableHandle pushed = (KairosdbTableHandle) result.getHandle();
        assertThat(pushed.getPushedStartMillis()).contains(T_MS);
        assertThat(pushed.getPushedEndMillis()).contains(T_MS);
        assertRemainingHasNoColumn(result, timestampColumn);
    }

    @Test
    void timestampGreaterThanOrEqual_pushedExactlyAndRemoved()
    {
        // timestamp >= A; no upper bound, but single inclusive-low range is
        // exact for KairosDB (which fills the high side with `now`).
        Domain d = Domain.create(
                ValueSet.ofRanges(Range.greaterThanOrEqual(BIGINT, T_MS)),
                false);

        ConstraintApplicationResult<ConnectorTableHandle> result =
                applyFilter(Map.of(timestampColumn, d));

        KairosdbTableHandle pushed = (KairosdbTableHandle) result.getHandle();
        assertThat(pushed.getPushedStartMillis()).contains(T_MS);
        assertThat(pushed.getPushedEndMillis()).isEmpty();
        assertRemainingHasNoColumn(result, timestampColumn);
    }

    @Test
    void timestampHalfOpenRange_shiftedAndRemoved()
    {
        // The canonical Superset shape: >= A AND < B.
        // After the 1ms shift the window is [A, B-1] - exact.
        long a = T_MS;
        long b = T_MS + 3_600_000L;
        Domain d = Domain.create(
                ValueSet.ofRanges(Range.range(BIGINT, a, true, b, false)),
                false);

        ConstraintApplicationResult<ConnectorTableHandle> result =
                applyFilter(Map.of(timestampColumn, d));

        KairosdbTableHandle pushed = (KairosdbTableHandle) result.getHandle();
        assertThat(pushed.getPushedStartMillis()).contains(a);
        assertThat(pushed.getPushedEndMillis()).contains(b - 1);
        assertRemainingHasNoColumn(result, timestampColumn);
    }

    @Test
    void timestampInList_boundingWindowPushedAndResidualPreserved()
    {
        // timestamp IN (t1, t2, t3) - the bounding hull is a superset, so
        // Trino must re-apply the predicate above the connector.  KairosDB
        // still gets the window as a fetch hint.
        long t1 = T_MS;
        long t2 = T_MS + 3_600_000L;
        long t3 = T_MS + 7_200_000L;
        Domain d = Domain.multipleValues(BIGINT, List.of(t1, t2, t3));

        ConstraintApplicationResult<ConnectorTableHandle> result =
                applyFilter(Map.of(timestampColumn, d));

        KairosdbTableHandle pushed = (KairosdbTableHandle) result.getHandle();
        assertThat(pushed.getPushedStartMillis()).contains(t1);
        assertThat(pushed.getPushedEndMillis()).contains(t3);
        // The IN-list must stay residual; Trino keeps the predicate node
        // above the connector and filters the bounded fetch down to t1/t2/t3.
        assertRemainingHasColumn(result, timestampColumn);
    }

    @Test
    void timestampUnboundedHighOnly_pushedExactlyAndRemoved()
    {
        // timestamp <= old_T - single inclusive-high range, exact.  The
        // KairosdbSplitManager will anchor its default floor to old_T (not
        // now()) so the resulting KairosDB window is sane even when old_T
        // is months in the past.
        Domain d = Domain.create(
                ValueSet.ofRanges(Range.lessThanOrEqual(BIGINT, T_MS)),
                false);

        ConstraintApplicationResult<ConnectorTableHandle> result =
                applyFilter(Map.of(timestampColumn, d));

        KairosdbTableHandle pushed = (KairosdbTableHandle) result.getHandle();
        assertThat(pushed.getPushedStartMillis()).isEmpty();
        assertThat(pushed.getPushedEndMillis()).contains(T_MS);
        assertRemainingHasNoColumn(result, timestampColumn);
    }

    @Test
    void timestampExclusiveHigh_shiftedAndRemoved()
    {
        // timestamp < T -> (-INF, T-1] after the 1ms shift, exact.
        Domain d = Domain.create(
                ValueSet.ofRanges(Range.lessThan(BIGINT, T_MS)),
                false);

        ConstraintApplicationResult<ConnectorTableHandle> result =
                applyFilter(Map.of(timestampColumn, d));

        KairosdbTableHandle pushed = (KairosdbTableHandle) result.getHandle();
        assertThat(pushed.getPushedEndMillis()).contains(T_MS - 1);
        assertRemainingHasNoColumn(result, timestampColumn);
    }

    @Test
    void timestampExclusiveLow_shiftedAndRemoved()
    {
        // timestamp > T -> [T+1, +INF) after the 1ms shift, exact.
        Domain d = Domain.create(
                ValueSet.ofRanges(Range.greaterThan(BIGINT, T_MS)),
                false);

        ConstraintApplicationResult<ConnectorTableHandle> result =
                applyFilter(Map.of(timestampColumn, d));

        KairosdbTableHandle pushed = (KairosdbTableHandle) result.getHandle();
        assertThat(pushed.getPushedStartMillis()).contains(T_MS + 1);
        assertRemainingHasNoColumn(result, timestampColumn);
    }

    // -------------------------------------------------------------------
    // Combined predicates
    // -------------------------------------------------------------------

    @Test
    void timestampRangeAndTagEquality_bothPushedAndRemoved()
    {
        long a = T_MS;
        long b = T_MS + 3_600_000L;
        ConstraintApplicationResult<ConnectorTableHandle> result = applyFilter(Map.of(
                timestampColumn, Domain.create(
                        ValueSet.ofRanges(Range.range(BIGINT, a, true, b, true)),
                        false),
                hostColumn, Domain.singleValue(VARCHAR, Slices.utf8Slice("h1"))));

        KairosdbTableHandle pushed = (KairosdbTableHandle) result.getHandle();
        assertThat(pushed.getPushedStartMillis()).contains(a);
        assertThat(pushed.getPushedEndMillis()).contains(b);
        assertThat(pushed.getPushedTagFilters()).isEqualTo(Map.of("host", List.of("h1")));
        assertRemainingHasNoColumn(result, timestampColumn);
        assertRemainingHasNoColumn(result, hostColumn);
    }

    @Test
    void timestampInListAndTagRange_neitherClaimedExactly()
    {
        // timestamp IN (...) -> bounding window pushed, residual kept.
        // host > 'm'         -> not pushed at all.
        long t1 = T_MS;
        long t2 = T_MS + 60_000L;
        Domain ts = Domain.multipleValues(BIGINT, List.of(t1, t2));
        Domain tag = Domain.create(
                ValueSet.ofRanges(Range.greaterThan(VARCHAR, Slices.utf8Slice("m"))),
                false);

        ConstraintApplicationResult<ConnectorTableHandle> result =
                applyFilter(Map.of(timestampColumn, ts, hostColumn, tag));

        KairosdbTableHandle pushed = (KairosdbTableHandle) result.getHandle();
        assertThat(pushed.getPushedStartMillis()).contains(t1);
        assertThat(pushed.getPushedEndMillis()).contains(t2);
        assertThat(pushed.getPushedTagFilters()).isEmpty();
        // Both predicates must reach Trino's residual filter above us.
        assertRemainingHasColumn(result, timestampColumn);
        assertRemainingHasColumn(result, hostColumn);
    }

    // -------------------------------------------------------------------
    // Convergence: applyFilter must reach a fixed point.
    // -------------------------------------------------------------------

    @Test
    void applyFilter_reachesFixedPointInOneCall()
    {
        // Trino calls applyFilter repeatedly until empty is returned; if a
        // bug made us recompute the same window forever, planning would
        // loop until the engine bails.  Verify converge-in-one.
        long a = T_MS;
        long b = T_MS + 60_000L;
        Map<KairosdbColumnHandle, Domain> domains = Map.of(
                timestampColumn,
                Domain.create(ValueSet.ofRanges(Range.range(BIGINT, a, true, b, true)), false),
                hostColumn,
                Domain.singleValue(VARCHAR, Slices.utf8Slice("h1")));
        ConstraintApplicationResult<ConnectorTableHandle> first = applyFilter(domains);

        ImmutableMap.Builder<ColumnHandle, Domain> wide = ImmutableMap.builder();
        for (Map.Entry<KairosdbColumnHandle, Domain> e : domains.entrySet()) {
            wide.put(e.getKey(), e.getValue());
        }
        Optional<ConstraintApplicationResult<ConnectorTableHandle>> second = metadata.applyFilter(
                null, first.getHandle(),
                new Constraint(TupleDomain.withColumnDomains(wide.buildOrThrow())));

        assertThat(second).isEmpty();
    }

    // -------------------------------------------------------------------
    // Test helpers
    // -------------------------------------------------------------------

    private KairosdbTableHandle baseHandle()
    {
        return new KairosdbTableHandle(CONNECTOR, SCHEMA, TABLE);
    }

    private ConstraintApplicationResult<ConnectorTableHandle> applyFilter(Map<KairosdbColumnHandle, Domain> domains)
    {
        return applyFilterOptional(domains).orElseThrow(() ->
                new AssertionError("applyFilter returned empty; expected a pushdown result"));
    }

    private Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilterOptional(Map<KairosdbColumnHandle, Domain> domains)
    {
        ImmutableMap.Builder<ColumnHandle, Domain> built = ImmutableMap.builder();
        for (Map.Entry<KairosdbColumnHandle, Domain> e : domains.entrySet()) {
            built.put(e.getKey(), e.getValue());
        }
        TupleDomain<ColumnHandle> summary = TupleDomain.withColumnDomains(built.buildOrThrow());
        return metadata.applyFilter(null, baseHandle(), new Constraint(summary));
    }

    private static void assertRemainingHasNoColumn(
            ConstraintApplicationResult<ConnectorTableHandle> result,
            ColumnHandle column)
    {
        TupleDomain<ColumnHandle> remaining = result.getRemainingFilter();
        if (remaining.isAll()) {
            return;
        }
        assertThat(remaining.getDomains().orElseThrow().keySet()).doesNotContain(column);
    }

    private static void assertRemainingHasColumn(
            ConstraintApplicationResult<ConnectorTableHandle> result,
            ColumnHandle column)
    {
        TupleDomain<ColumnHandle> remaining = result.getRemainingFilter();
        assertThat(remaining.getDomains().orElseThrow().keySet()).contains(column);
    }
}
