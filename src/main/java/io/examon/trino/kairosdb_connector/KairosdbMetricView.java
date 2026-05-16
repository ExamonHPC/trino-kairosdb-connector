package io.examon.trino.kairosdb_connector;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Snapshot of the connector's catalog as exposed to Trino.
 *
 * <p>Trino's SPI forces every {@link io.trino.spi.connector.SchemaTableName}
 * to lowercase ({@code SchemaTableName} unconditionally lowercases its
 * arguments in the constructor; tracked upstream by
 * <a href="https://github.com/trinodb/trino/issues/17">trinodb/trino#17</a>).
 * KairosDB on the other hand is case-sensitive and is happy to host two
 * distinct metrics whose names differ only in case (e.g. {@code pue} and
 * {@code Pue}).  A naive 1-to-1 mapping silently collapses them into a
 * single Trino-side row and leaves all but one of the variants
 * unreachable.
 *
 * <p>This class implements the connector's reconciliation rule:
 *
 * <ol>
 *   <li>Group raw KairosDB metric names by their lowercase form.</li>
 *   <li>For groups of size 1, the metric is exposed as its own lowercase
 *       form (no suffix).  When the original is already lowercase, the
 *       Trino-side and KairosDB-side names coincide; otherwise the lowercase
 *       form maps back to the original via case-insensitive fallback,
 *       gated on the {@code kairosdb.case-insensitive-name-matching}
 *       config (when {@code false}, mixed-case singletons are intentionally
 *       hidden, matching the strict behaviour of the legacy connector).</li>
 *   <li>For groups of size &gt; 1 (a real collision), <em>every</em> member
 *       gets a deterministic hash-suffixed Trino-side name of the form
 *       {@code <lowercase>__<6 hex of sha256(original)>}.  This guarantees
 *       that no metric ever silently shadows another, at the cost of
 *       requiring users to discover the mangled name (via {@code SHOW
 *       TABLES}) before querying.  A per-table comment exposes the original
 *       KairosDB name so {@code SHOW CREATE TABLE} and
 *       {@code system.metadata.table_comments} reveal the mapping.</li>
 * </ol>
 *
 * <p>The mangling is deterministic across nodes and across restarts because
 * the hash is computed only from the original metric name, with no
 * cluster-side state.  A coordinator and its workers therefore agree on the
 * mapping without coordination.
 */
final class KairosdbMetricView
{
    /** Six hex characters = 24 bits.  Birthday-paradox collision probability
     * is around 2x10^-7 for three case-variants of the same name, and
     * stays below 0.1% even at 100 case-variants in one collision group.
     * If a hash collision is ever observed at view-build time it surfaces
     * as a hard error (we use {@link ImmutableMap.Builder#buildOrThrow()})
     * rather than silently mismapping. */
    private static final int SUFFIX_LENGTH_HEX = 6;

    private static final String SUFFIX_SEPARATOR = "__";

    private final List<String> trinoSideNames;
    private final Map<String, String> trinoToKairos;
    private final Map<String, String> commentByKairos;

    private KairosdbMetricView(
            List<String> trinoSideNames,
            Map<String, String> trinoToKairos,
            Map<String, String> commentByKairos)
    {
        this.trinoSideNames = requireNonNull(trinoSideNames, "trinoSideNames is null");
        this.trinoToKairos = requireNonNull(trinoToKairos, "trinoToKairos is null");
        this.commentByKairos = requireNonNull(commentByKairos, "commentByKairos is null");
    }

    /**
     * Builds a view from a raw KairosDB metric list.  The order of
     * {@link #trinoSideNames()} follows the iteration order of the
     * lowercase grouping (insertion order of the first variant per group),
     * which keeps {@code SHOW TABLES} output stable and human-friendly.
     */
    static KairosdbMetricView build(List<String> rawNames, boolean caseInsensitiveNameMatching)
    {
        // De-dup raw names defensively.  KairosDB shouldn't return
        // duplicates, but if it ever did the grouping below would put two
        // identical entries in the same bucket and the mangling step
        // would emit two identical Trino-side names; buildOrThrow() would
        // then surface that as a hard error.  Pre-dedup keeps the contract
        // tight without hiding genuine collisions.
        LinkedHashSet<String> deduped = new LinkedHashSet<>(rawNames);

        // Group by lowercase form, preserving the order of first occurrence.
        LinkedHashMap<String, List<String>> byLower = new LinkedHashMap<>();
        for (String name : deduped) {
            String lc = name.toLowerCase(Locale.ROOT);
            byLower.computeIfAbsent(lc, k -> new ArrayList<>()).add(name);
        }

        ImmutableList.Builder<String> trinoSideNames = ImmutableList.builder();
        ImmutableMap.Builder<String, String> trinoToKairos = ImmutableMap.builder();
        ImmutableMap.Builder<String, String> commentByKairos = ImmutableMap.builder();

        for (Map.Entry<String, List<String>> entry : byLower.entrySet()) {
            String lc = entry.getKey();
            List<String> originals = entry.getValue();
            if (originals.size() == 1) {
                String original = originals.get(0);
                boolean alreadyLowercase = original.equals(lc);
                if (alreadyLowercase || caseInsensitiveNameMatching) {
                    // No collision: use the lowercase form as the Trino-side
                    // name.  When the original is mixed case we still map
                    // back to the original (case-insensitive fallback).
                    trinoSideNames.add(lc);
                    trinoToKairos.put(lc, original);
                }
                // else: mixed-case singleton with strict matching configured;
                // intentionally drop it so it's unreachable from SQL.
            }
            else {
                // Collision: mangle every member.  Comments document the
                // mapping for SHOW CREATE TABLE / system.metadata.table_comments.
                for (String original : originals) {
                    String mangled = lc + SUFFIX_SEPARATOR + hashSuffix(original);
                    trinoSideNames.add(mangled);
                    trinoToKairos.put(mangled, original);
                    commentByKairos.put(original,
                            format("KairosDB metric \"%s\" (case-mangled: original case differs from another metric)", original));
                }
            }
        }

        return new KairosdbMetricView(
                trinoSideNames.build(),
                trinoToKairos.buildOrThrow(),
                commentByKairos.buildOrThrow());
    }

    /** Trino-side names in the order they should appear in {@code SHOW TABLES}. */
    List<String> trinoSideNames()
    {
        return trinoSideNames;
    }

    /**
     * Resolves a Trino-side name (always lowercase, as Trino lowercases
     * identifiers via {@link io.trino.spi.connector.SchemaTableName} before
     * the connector ever sees them) to the original case-preserving
     * KairosDB metric name suitable for HTTP requests.
     */
    java.util.Optional<String> resolve(String trinoSideName)
    {
        return java.util.Optional.ofNullable(trinoToKairos.get(trinoSideName));
    }

    /**
     * Inverse of {@link #resolve(String)}: returns the Trino-side name for a
     * KairosDB original.  Empty if the original is not currently exposed
     * (e.g. mixed-case singleton with strict matching disabled).
     */
    java.util.Optional<String> trinoSideNameOf(String kairosOriginal)
    {
        for (Map.Entry<String, String> entry : trinoToKairos.entrySet()) {
            if (entry.getValue().equals(kairosOriginal)) {
                return java.util.Optional.of(entry.getKey());
            }
        }
        return java.util.Optional.empty();
    }

    /**
     * Returns the auto-generated table comment for a mangled KairosDB
     * metric, or empty if the metric is not part of a collision group.
     */
    java.util.Optional<String> commentFor(String kairosOriginal)
    {
        return java.util.Optional.ofNullable(commentByKairos.get(kairosOriginal));
    }

    /** True iff at least one mangling decision was made when this view was built. */
    boolean hasAnyCollision()
    {
        return !commentByKairos.isEmpty();
    }

    /**
     * Computes the {@value #SUFFIX_LENGTH_HEX}-hex-character SHA-256 prefix
     * of a metric name, used as the disambiguator inside a collision group.
     * Deterministic across JVMs.
     */
    static String hashSuffix(String original)
    {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(original.getBytes(StandardCharsets.UTF_8));
            StringBuilder hex = new StringBuilder(SUFFIX_LENGTH_HEX);
            for (int i = 0; hex.length() < SUFFIX_LENGTH_HEX; i++) {
                hex.append(format("%02x", digest[i] & 0xff));
            }
            return hex.substring(0, SUFFIX_LENGTH_HEX);
        }
        catch (NoSuchAlgorithmException e) {
            // SHA-256 is mandatory for every Java SE implementation.
            throw new IllegalStateException("SHA-256 not available", e);
        }
    }
}
