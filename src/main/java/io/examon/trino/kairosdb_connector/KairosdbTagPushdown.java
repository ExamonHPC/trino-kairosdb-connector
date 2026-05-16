package io.examon.trino.kairosdb_connector;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
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
 * <p>KairosDB tag filters only understand equality / set membership, never
 * ordered range queries.  We therefore extract:
 * <ul>
 *   <li>single-value sub-ranges of a {@link SortedRangeSet} (one per
 *       equality / {@code IN} value), and</li>
 *   <li>every discrete value of an {@link EquatableValueSet}.</li>
 * </ul>
 *
 * <p>True string-range predicates (e.g. {@code host > 'foo'}) yield no
 * concrete values; we push down what we <em>can</em> extract and silently
 * let the rest evaluate above the connector.  Tag predicates against this
 * connector are expected to be equality or {@code IN} lists; the limitation
 * is documented here in case it ever needs revisiting.
 */
final class KairosdbTagPushdown
{
    private KairosdbTagPushdown() {}

    /**
     * @return the values KairosDB should match for this tag, or
     *         {@link Optional#empty()} if the domain is unrecognised.  An
     *         empty list (but {@code Optional} present) means "the predicate
     *         is shaped like something we handle, but it admits no concrete
     *         values we can forward" – callers claim the domain as pushed
     *         and forward an empty value list.  KairosDB then returns rows
     *         this tag would otherwise have filtered, but in practice this
     *         branch is unreachable.
     */
    static Optional<List<String>> extractAdmittedValues(Domain domain)
    {
        if (domain.isAll() || domain.isNone()) {
            return Optional.empty();
        }
        ValueSet values = domain.getValues();
        ImmutableList.Builder<String> out = ImmutableList.builder();
        if (values instanceof SortedRangeSet) {
            for (Range range : values.getRanges().getOrderedRanges()) {
                if (range.isSingleValue()) {
                    String v = stringValue(range.getSingleValue());
                    if (v != null) {
                        out.add(v);
                    }
                }
                // Range queries on string tags are intentionally ignored –
                // KairosDB has no equivalent and the connector silently
                // drops them (Trino still re-evaluates the predicate above
                // us, so the user-visible result is correct).
            }
            return Optional.of(out.build());
        }
        if (values instanceof EquatableValueSet eqs) {
            for (Object value : eqs.getDiscreteValues().getValues()) {
                String v = stringValue(value);
                if (v != null) {
                    out.add(v);
                }
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
