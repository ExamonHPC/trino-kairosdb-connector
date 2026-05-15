#!/usr/bin/env bash
# Build the plugin and stage the shaded jar into ./plugin/kairosdb/ where
# docker-compose bind-mounts it into the stock trinodb/trino image.
#
# Usage:
#   scripts/package.sh                    # full clean build
#   scripts/package.sh package            # incremental (no clean)
#
# After this:
#   docker compose restart trino          # picks up the new jar in ~5s

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

cd "$REPO_ROOT"

GOALS=("$@")
if [[ ${#GOALS[@]} -eq 0 ]]; then
    GOALS=("clean" "package")
fi

"$SCRIPT_DIR/build.sh" -q "${GOALS[@]}"

STAGE_DIR="plugin/kairosdb"
mkdir -p "$STAGE_DIR"
rm -f "$STAGE_DIR"/*.jar

# maven-shade replaces the regular artifact with the shaded one and keeps the
# un-shaded copy under target/original-*.jar.  Trino only wants the shaded one.
found=0
for jar in target/kairosdb-connector-*.jar; do
    [[ -f "$jar" ]] || continue
    [[ "$(basename "$jar")" == original-* ]] && continue
    cp "$jar" "$STAGE_DIR/"
    found=1
done

if [[ $found -eq 0 ]]; then
    echo "ERROR: no shaded kairosdb-connector jar found in target/" >&2
    exit 1
fi

echo "Plugin staged into ${STAGE_DIR}:"
ls -lh "$STAGE_DIR"
