package io.examon.trino.kairosdb_connector;

import io.examon.trino.kairosdb_connector.KairosdbSamplingConstants.Interval;
import io.examon.trino.kairosdb_connector.KairosdbSamplingConstants.KairosdbTimeUnit;
import io.examon.trino.kairosdb_connector.KairosdbSamplingConstants.ParsedAggregator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Golden + negative cases for the {@code sampling_aggregator} spec parser.
 * Every rule the parser enforces is exercised here so a future refactor
 * can't silently relax the contract.
 */
final class TestKairosdbSamplingConstants
{
    @Test
    void parsesAllThreeComponents()
    {
        ParsedAggregator p = KairosdbSamplingConstants.parse("sum;10m;start_time");
        assertThat(p.type()).isEqualTo("sum");
        assertThat(p.interval()).isEqualTo(new Interval(10, KairosdbTimeUnit.MINUTES));
        assertThat(p.alignment()).isEqualTo("start_time");
    }

    @Test
    void typeIsNormalisedToLowerCase()
    {
        ParsedAggregator p = KairosdbSamplingConstants.parse("SUM;1h;sampling");
        assertThat(p.type()).isEqualTo("sum");
    }

    @Test
    void intervalDefaultsToMinutesWhenNoUnitSpecified()
    {
        ParsedAggregator p = KairosdbSamplingConstants.parse("avg;30;none");
        assertThat(p.interval()).isEqualTo(new Interval(30, KairosdbTimeUnit.MINUTES));
    }

    @ParameterizedTest
    @ValueSource(strings = {"5s", "5m", "5h", "5d"})
    void intervalAcceptsEveryDocumentedUnit(String interval)
    {
        ParsedAggregator p = KairosdbSamplingConstants.parse("count;" + interval + ";none");
        assertThat(p.interval().value()).isEqualTo(5);
    }

    @Test
    void wholeChainOfDocumentedTypesParses()
    {
        // Sanity check that every documented type round-trips.  Picking one
        // interval / alignment combination is enough; the parser doesn't
        // care about type-specific shapes.
        for (String type : KairosdbSamplingConstants.VALID_AGGREGATORS) {
            ParsedAggregator p = KairosdbSamplingConstants.parse(type + ";1h;sampling");
            assertThat(p.type()).isEqualTo(type);
        }
    }

    @Test
    void rejectsEmptyAndNull()
    {
        assertThatThrownBy(() -> KairosdbSamplingConstants.parse(null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> KairosdbSamplingConstants.parse(""))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> KairosdbSamplingConstants.parse("   "))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsWrongArity()
    {
        // Two segments
        assertThatThrownBy(() -> KairosdbSamplingConstants.parse("sum;1h"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("type;interval;alignment");
        // Four segments
        assertThatThrownBy(() -> KairosdbSamplingConstants.parse("sum;1h;start_time;extra"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsUnknownAggregatorType()
    {
        assertThatThrownBy(() -> KairosdbSamplingConstants.parse("bogus;1h;start_time"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown aggregator type");
    }

    @Test
    void rejectsUnknownAlignment()
    {
        assertThatThrownBy(() -> KairosdbSamplingConstants.parse("sum;1h;sideways"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown alignment option");
    }

    @Test
    void rejectsMalformedInterval()
    {
        assertThatThrownBy(() -> KairosdbSamplingConstants.parse("sum;ten_minutes;start_time"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid interval");
    }

    @Test
    void isSamplingColumnIsCaseInsensitive()
    {
        assertThat(KairosdbSamplingConstants.isSamplingColumn("sampling_aggregator")).isTrue();
        assertThat(KairosdbSamplingConstants.isSamplingColumn("SAMPLING_AGGREGATOR")).isTrue();
        assertThat(KairosdbSamplingConstants.isSamplingColumn("host")).isFalse();
        assertThat(KairosdbSamplingConstants.isSamplingColumn(null)).isFalse();
    }
}
