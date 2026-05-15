#!/usr/bin/env bash
# Run the @Tag("integration") tests inside the maven builder image, with
# the host's docker socket and network mounted in so Testcontainers can
# spawn KairosDB on the host engine and reach it on localhost.
#
# Usage:
#     scripts/test-integration.sh                # runs all integration tests
#     scripts/test-integration.sh -Dtest=ITKairosdbClient#columnsExposeLowercaseTagsPlusSyntheticAndHidden
#
# The first run pulls examonhpc/kairosdb:1.2.2 (~ a few hundred MB); subsequent
# runs reuse the cached image.

set -euo pipefail

readonly IMAGE="maven:3.9.10-eclipse-temurin-24-alpine"
readonly CACHE_VOLUME="kairosdb-connector-m2"
readonly REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

docker volume inspect "${CACHE_VOLUME}" >/dev/null 2>&1 \
    || docker volume create "${CACHE_VOLUME}" >/dev/null

# --network=host lets Testcontainers' getHost()/getMappedPort() resolve to a
# reachable address from inside the builder: the spawned KairosDB binds to a
# random port on the host, and the builder shares the host network namespace.
# /var/run/docker.sock gives Testcontainers control of the same engine that
# runs the builder itself.
exec docker run --rm \
    --network=host \
    -v /var/run/docker.sock:/var/run/docker.sock \
    -v "${REPO_ROOT}:/app" \
    -v "${CACHE_VOLUME}:/root/.m2" \
    -w /app \
    -e TESTCONTAINERS_RYUK_DISABLED=true \
    "${IMAGE}" \
    mvn -B -Pintegration verify "$@"
