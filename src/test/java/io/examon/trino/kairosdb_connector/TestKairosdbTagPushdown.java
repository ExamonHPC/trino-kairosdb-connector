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
 * Locks in the strict VARCHAR Domain -> KairosDB tag-filter translation:
 * the method returns a non-empty value list only when the entire domain
 * can be expressed as a pure equality / {@code IN} predicate.  Any
 * non-singleton range, blacklist set, {@code NULL} admission, or mixed
 * shape returns {@link Optional#empty()} so the caller leaves the
 * predicate in {@code remainingFilter} for Trino to re-evaluate.
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
    void stringRangePredicateIsNotPushed()
    {
        // host > 'm' AND host < 'p' - KairosDB has no string-range filter,
        // so the whole domain must stay residual.
        Domain d = Domain.create(
                ValueSet.ofRanges(Range.range(VARCHAR,
                        Slices.utf8Slice("m"), false,
                        Slices.utf8Slice("p"), false)),
                false);

        assertThat(KairosdbTagPushdown.extractAdmittedValues(d)).isEmpty();
    }

    @Test
    void notEqualsIsNotPushed()
    {
        // host != 'foo' - represented as two open ranges, neither singleton.
        Domain d = Domain.create(
                ValueSet.ofRanges(
                        Range.lessThan(VARCHAR, Slices.utf8Slice("foo")),
                        Range.greaterThan(VARCHAR, Slices.utf8Slice("foo"))),
                false);

        assertThat(KairosdbTagPushdown.extractAdmittedValues(d)).isEmpty();
    }

    @Test
    void mixedEqualityAndRangeIsNotPushed()
    {
        // host = 'n01' OR host BETWEEN 'x' AND 'z'.  Even though the equality
        // part is expressible, the range part is not, so the whole predicate
        // must stay residual - claiming partial pushdown would silently drop
        // the range portion.
        Domain d = Domain.create(
                ValueSet.ofRanges(
                        Range.equal(VARCHAR, Slices.utf8Slice("n01")),
                        Range.range(VARCHAR,
                                Slices.utf8Slice("x"), true,
                                Slices.utf8Slice("z"), false)),
                false);

        assertThat(KairosdbTagPushdown.extractAdmittedValues(d)).isEmpty();
    }

    @Test
    void isNullIsNotPushed()
    {
        // host IS NULL - tag values in KairosDB are never NULL.
        assertThat(KairosdbTagPushdown.extractAdmittedValues(Domain.onlyNull(VARCHAR))).isEmpty();
    }

    @Test
    void isNotNullIsNotPushed()
    {
        // host IS NOT NULL - admits every value; not a finite set match.
        assertThat(KairosdbTagPushdown.extractAdmittedValues(Domain.notNull(VARCHAR))).isEmpty();
    }

    @Test
    void equalityOrNullIsNotPushed()
    {
        // host = 'n01' OR host IS NULL - mixed with NULL admission.
        Domain d = Domain.create(
                ValueSet.of(VARCHAR, Slices.utf8Slice("n01")),
                true);

        assertThat(KairosdbTagPushdown.extractAdmittedValues(d)).isEmpty();
    }

    @Test
    void allAndNoneDomainsAreNotPushed()
    {
        assertThat(KairosdbTagPushdown.extractAdmittedValues(Domain.all(VARCHAR))).isEmpty();
        assertThat(KairosdbTagPushdown.extractAdmittedValues(Domain.none(VARCHAR))).isEmpty();
    }
}
