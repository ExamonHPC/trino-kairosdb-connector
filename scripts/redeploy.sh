#!/usr/bin/env bash
# One-shot dev loop: rebuild the plugin, restart the dev trino container, and
# block until the cluster is fully scheduled (no NO_NODES_AVAILABLE race).
#
# Usage:
#   scripts/redeploy.sh                # clean + package + restart + wait
#   scripts/redeploy.sh package        # incremental build (no clean)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

"$SCRIPT_DIR/package.sh" "$@"

if ! docker compose ps --status running trino 2>/dev/null | grep -q trino; then
    echo "trino container is not running; starting full stack..."
    docker compose up -d
else
    docker compose restart trino
fi

echo -n "Waiting for /v1/info to leave starting state"
for _ in $(seq 1 60); do
    if docker compose exec -T trino sh -c \
            "curl -sS http://localhost:8080/v1/info 2>/dev/null | grep -q '\"starting\":false'" 2>/dev/null; then
        echo " - ok"
        break
    fi
    echo -n "."
    sleep 1
done

echo -n "Waiting for cluster scheduling to be live"
for _ in $(seq 1 30); do
    if docker compose exec -T trino trino --execute "SELECT 1" 2>/dev/null | grep -q '^"1"$'; then
        echo " - ok"
        echo "Trino is ready."
        exit 0
    fi
    echo -n "."
    sleep 1
done

echo
echo "WARNING: trino is up but SELECT 1 did not succeed within 30s" >&2
exit 1
