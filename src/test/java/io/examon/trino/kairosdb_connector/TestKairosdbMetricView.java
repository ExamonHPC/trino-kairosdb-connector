package io.examon.trino.kairosdb_connector;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Locks in the case-collision contract documented on
 * {@link KairosdbMetricView}.  The five rows match the simulation table
 * the design discussion produced - if any of these flips, the connector's
 * "no silent shadowing" promise is at risk.
 */
final class TestKairosdbMetricView
{
    @Test
    void singleLowercaseSingletonIsExposedUnchanged()
    {
        KairosdbMetricView view = KairosdbMetricView.build(List.of("pue"), true);

        assertThat(view.trinoSideNames()).containsExactly("pue");
        assertThat(view.resolve("pue")).contains("pue");
        assertThat(view.commentFor("pue")).isEmpty();
        assertThat(view.hasAnyCollision()).isFalse();
    }

    @Test
    void singleMixedCaseSingletonIsLowercasedWithCaseInsensitive()
    {
        // The Sys.Mem path the existing integration test depends on:
        // one mixed-case metric, no twin, exposed via its lowercase form.
        KairosdbMetricView view = KairosdbMetricView.build(List.of("Sys.Mem"), true);

        assertThat(view.trinoSideNames()).containsExactly("sys.mem");
        assertThat(view.resolve("sys.mem")).contains("Sys.Mem");
        assertThat(view.commentFor("Sys.Mem")).isEmpty();
        assertThat(view.hasAnyCollision()).isFalse();
    }

    @Test
    void singleMixedCaseSingletonIsHiddenWhenStrict()
    {
        // caseInsensitive=false: mixed-case singleton is intentionally
        // unreachable from SQL.  This preserves the strict mode the
        // legacy connector exposed via the same config knob.
        KairosdbMetricView view = KairosdbMetricView.build(List.of("Sys.Mem"), false);

        assertThat(view.trinoSideNames()).isEmpty();
        assertThat(view.resolve("sys.mem")).isEmpty();
        assertThat(view.resolve("Sys.Mem")).isEmpty();
    }

    @Test
    void twoVariantCollisionMangleEverything()
    {
        // Production scenario: KairosDB legitimately has both `pue` and
        // `Pue` (separate metrics from different scrapers).  Neither wins
        // the unmangled slot - both get hash suffixes.
        KairosdbMetricView view = KairosdbMetricView.build(List.of("pue", "Pue"), true);

        String suffixLower = KairosdbMetricView.hashSuffix("pue");
        String suffixUpper = KairosdbMetricView.hashSuffix("Pue");
        assertThat(suffixLower).isNotEqualTo(suffixUpper);

        assertThat(view.trinoSideNames())
                .containsExactlyInAnyOrder("pue__" + suffixLower, "pue__" + suffixUpper);
        assertThat(view.resolve("pue")).isEmpty();           // unmangled lookup fails
        assertThat(view.resolve("pue__" + suffixLower)).contains("pue");
        assertThat(view.resolve("pue__" + suffixUpper)).contains("Pue");
        assertThat(view.commentFor("pue")).hasValueSatisfying(s -> assertThat(s).contains("\"pue\""));
        assertThat(view.commentFor("Pue")).hasValueSatisfying(s -> assertThat(s).contains("\"Pue\""));
        assertThat(view.hasAnyCollision()).isTrue();
    }

    @Test
    void threeVariantCollisionMangleEverything()
    {
        // The classic worst case: pue / Pue / PUE all distinct in KairosDB.
        KairosdbMetricView view = KairosdbMetricView.build(List.of("pue", "Pue", "PUE"), true);

        assertThat(view.trinoSideNames()).hasSize(3);
        assertThat(view.trinoSideNames())
                .allSatisfy(n -> assertThat(n).startsWith("pue__"));

        // Every mangled name maps back unambiguously to its KairosDB original.
        assertThat(view.resolve("pue__" + KairosdbMetricView.hashSuffix("pue"))).contains("pue");
        assertThat(view.resolve("pue__" + KairosdbMetricView.hashSuffix("Pue"))).contains("Pue");
        assertThat(view.resolve("pue__" + KairosdbMetricView.hashSuffix("PUE"))).contains("PUE");

        // The unmangled lowercase form returns empty - silent shadowing
        // is the bug we are explicitly preventing.
        assertThat(view.resolve("pue")).isEmpty();
    }

    @Test
    void noLowercaseTwinCollisionStillManglesEverything()
    {
        // Edge case: two mixed-case variants but no exact-lowercase form.
        // The hybrid rule says "any collision -> all mangled", with no
        // arbitrary "first wins the unmangled slot".
        KairosdbMetricView view = KairosdbMetricView.build(List.of("Pue", "PUE"), true);

        assertThat(view.trinoSideNames()).hasSize(2);
        assertThat(view.resolve("pue")).isEmpty();
        assertThat(view.resolve("pue__" + KairosdbMetricView.hashSuffix("Pue"))).contains("Pue");
        assertThat(view.resolve("pue__" + KairosdbMetricView.hashSuffix("PUE"))).contains("PUE");
    }

    @Test
    void independentMetricsAreUnaffectedByOthersCollision()
    {
        // sys.load is in its own group so the pue collision doesn't
        // perturb its representation.
        KairosdbMetricView view = KairosdbMetricView.build(List.of("sys.load", "Sys.Mem", "pue", "Pue"), true);

        assertThat(view.trinoSideNames()).contains("sys.load", "sys.mem");
        assertThat(view.resolve("sys.load")).contains("sys.load");
        assertThat(view.resolve("sys.mem")).contains("Sys.Mem");

        // pue and Pue both mangled
        assertThat(view.resolve("pue")).isEmpty();
        assertThat(view.trinoSideNames()).filteredOn(n -> n.startsWith("pue__")).hasSize(2);
    }

    @Test
    void hashSuffixIsDeterministicAcrossInvocations()
    {
        // Multi-worker contract: every JVM that hashes the same metric
        // name must produce the same suffix.  No reseeding, no clock,
        // no randomness allowed.
        for (String name : List.of("Pue", "MyMetric", "sys.load", "X")) {
            String first = KairosdbMetricView.hashSuffix(name);
            String second = KairosdbMetricView.hashSuffix(name);
            assertThat(first).hasSize(6).isEqualTo(second);
        }
    }

    @Test
    void hashSuffixDiffersBetweenCaseVariants()
    {
        // The whole scheme rests on case-sensitive hashing - if `Pue` and
        // `PUE` happened to hash to the same suffix, they'd collide post-
        // mangling and one would silently overwrite the other.  Spot-check
        // a handful of variants.
        String pue = KairosdbMetricView.hashSuffix("pue");
        String puE = KairosdbMetricView.hashSuffix("Pue");
        String PUE = KairosdbMetricView.hashSuffix("PUE");
        String PUe = KairosdbMetricView.hashSuffix("PUe");
        assertThat(ImmutableList.of(pue, puE, PUE, PUe)).doesNotHaveDuplicates();
    }

    @Test
    void duplicateRawNamesAreDeduped()
    {
        // KairosDB shouldn't emit duplicates but be defensive: the same
        // name twice must be treated as one entry, NOT as a collision
        // group of size 2 (which would mangle the lone metric).
        KairosdbMetricView view = KairosdbMetricView.build(
                ImmutableList.of("sys.load", "sys.load", "Pue", "Pue"),
                true);

        assertThat(view.trinoSideNames())
                .containsExactly("sys.load", "pue");
        assertThat(view.resolve("sys.load")).contains("sys.load");
        assertThat(view.resolve("pue")).contains("Pue");
        assertThat(view.hasAnyCollision()).isFalse();
    }

    @Test
    void trinoSideNameOfReversesTheMapping()
    {
        KairosdbMetricView view = KairosdbMetricView.build(List.of("pue", "Pue"), true);

        String mangledForLowercase = "pue__" + KairosdbMetricView.hashSuffix("pue");
        String mangledForCapitalised = "pue__" + KairosdbMetricView.hashSuffix("Pue");
        assertThat(view.trinoSideNameOf("pue")).contains(mangledForLowercase);
        assertThat(view.trinoSideNameOf("Pue")).contains(mangledForCapitalised);
        assertThat(view.trinoSideNameOf("DoesNotExist")).isEmpty();
    }
}
