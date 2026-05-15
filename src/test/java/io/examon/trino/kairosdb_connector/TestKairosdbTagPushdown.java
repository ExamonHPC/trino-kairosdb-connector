package io.examon.trino.kairosdb_connector;

import io.airlift.slice.Slices;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.VarcharType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Locks in the VARCHAR Domain -> KairosDB tag-filter value list translation.
 * The bullet-point production behaviour is:
 * <ul>
 *   <li>equality and IN both collapse to a list of admitted values;</li>
 *   <li>string-range predicates have no KairosDB equivalent and are
 *       intentionally dropped (callers still claim the column as pushed);</li>
 *   <li>ALL / NONE domains do not produce a tag filter.</li>
 * </ul>
 */
final class TestKairosdbTagPushdown
{
    private static final VarcharType VARCHAR = VarcharType.createUnboundedVarcharType();

    @Test
    void equalityProducesSingletonList()
    {
        Domain d = Domain.singleValue(VARCHAR, Slices.utf8Slice("n01"));

        Optional<List<String>> values = KairosdbTagPushdown.extractAdmittedValues(d);

        assertThat(values).isPresent();
        assertThat(values.get()).containsExactly("n01");
    }

    @Test
    void inListProducesEveryAdmittedValue()
    {
        Domain d = Domain.multipleValues(VARCHAR, List.of(
                Slices.utf8Slice("n01"),
                Slices.utf8Slice("n02"),
                Slices.utf8Slice("n03")));

        List<String> values = KairosdbTagPushdown.extractAdmittedValues(d).orElseThrow();

        // Order comes from SortedRangeSet (alphabetical for VARCHAR).
        assertThat(values).containsExactly("n01", "n02", "n03");
    }

    @Test
    void stringRangePredicateIsDroppedSilently()
    {
        // host > 'm' AND host < 'p'  -- KairosDB has no string-range filter,
        // so we yield an empty admitted list.  The caller still claims the
        // tag column as fully pushed to match the production behaviour.
        Domain d = Domain.create(
                ValueSet.ofRanges(Range.range(VARCHAR,
                        Slices.utf8Slice("m"), false,
                        Slices.utf8Slice("p"), false)),
                false);

        Optional<List<String>> values = KairosdbTagPushdown.extractAdmittedValues(d);

        // We recognise the domain shape; the list is empty because no concrete
        // values came out of the range.
        assertThat(values).isPresent();
        assertThat(values.get()).isEmpty();
    }

    @Test
    void allAndNoneDomainsAreNotPushed()
    {
        assertThat(KairosdbTagPushdown.extractAdmittedValues(Domain.all(VARCHAR))).isEmpty();
        assertThat(KairosdbTagPushdown.extractAdmittedValues(Domain.none(VARCHAR))).isEmpty();
    }

    @Test
    void mixedEqualityAndRangeKeepsTheEqualityOnly()
    {
        // host = 'n01' OR host BETWEEN 'x' AND 'z'  (non-overlapping with 'n01').
        // Only 'n01' survives because the range portion has no KairosDB
        // equivalent and is dropped.
        Domain d = Domain.create(
                ValueSet.ofRanges(
                        Range.equal(VARCHAR, Slices.utf8Slice("n01")),
                        Range.range(VARCHAR,
                                Slices.utf8Slice("x"), true,
                                Slices.utf8Slice("z"), false)),
                false);

        List<String> values = KairosdbTagPushdown.extractAdmittedValues(d).orElseThrow();

        assertThat(values).containsExactly("n01");
    }
}
