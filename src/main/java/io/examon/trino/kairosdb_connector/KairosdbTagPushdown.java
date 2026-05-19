package io.examon.trino.kairosdb_connector;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.trino.spi.predicate.DiscreteValues;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.EquatableValueSet;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.SortedRangeSet;
import io.trino.spi.predicate.ValueSet;

import java.util.List;
import java.util.Optional;

/**
 * Translates a Trino {@link Domain} on a VARCHAR tag column into the list of
 * concrete tag values KairosDB should match.
 *
 * <p>KairosDB tag filters are strictly set-membership: {@code {"tag": ["v1","v2",...]}}.
 * No range queries ({@code >}, {@code <}, {@code BETWEEN}), no inequality
 * ({@code !=}, {@code NOT IN}), no {@code LIKE}, no regex, no {@code NULL}
 * semantics.  To avoid silently dropping unrepresentable predicates this
 * method is strict: it returns a value list <em>only</em> when the entire
 * domain can be expressed as a pure equality / {@code IN} match.  Anything
 * else returns {@link Optional#empty()}, which signals the caller to leave
 * the predicate in {@code remainingFilter} so Trino re-evaluates it above
 * the connector.
 */
final class KairosdbTagPushdown
{
    private KairosdbTagPushdown() {}

    /**
     * @return the values KairosDB should match for this tag, or
     *         {@link Optional#empty()} if the domain is anything other than
     *         a pure equality / {@code IN} predicate.  Callers must claim
     *         the column as pushed only on a non-empty result.
     */
    static Optional<List<String>> extractAdmittedValues(Domain domain)
    {
        if (domain.isAll() || domain.isNone()) {
            return Optional.empty();
        }
        // Mixed-with-NULL predicates (e.g. host IS NULL OR host = 'foo')
        // can't be expressed in a KairosDB tag filter.
        if (domain.isNullAllowed()) {
            return Optional.empty();
        }
        ValueSet values = domain.getValues();
        ImmutableList.Builder<String> out = ImmutableList.builder();
        if (values instanceof SortedRangeSet) {
            for (Range range : values.getRanges().getOrderedRanges()) {
                if (!range.isSingleValue()) {
                    // Any non-singleton range (host > 'm', BETWEEN, etc.)
                    // disqualifies the whole domain.
                    return Optional.empty();
                }
                String v = stringValue(range.getSingleValue());
                if (v == null) {
                    return Optional.empty();
                }
                out.add(v);
            }
            return Optional.of(out.build());
        }
        if (values instanceof EquatableValueSet eqs) {
            DiscreteValues discrete = eqs.getDiscreteValues();
            // Blacklist form (host NOT IN (...)) is not expressible.
            if (!discrete.isInclusive()) {
                return Optional.empty();
            }
            for (Object value : discrete.getValues()) {
                String v = stringValue(value);
                if (v == null) {
                    return Optional.empty();
                }
                out.add(v);
            }
            return Optional.of(out.build());
        }
        return Optional.empty();
    }

    private static String stringValue(Object value)
    {
        if (value instanceof Slice s) {
            return s.toStringUtf8();
        }
        return value == null ? null : value.toString();
    }
}
