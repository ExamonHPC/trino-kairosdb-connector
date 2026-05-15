#!/usr/bin/env bash
# Run any maven goal inside the same builder image the Dockerfile uses.
# Examples:
#   scripts/build.sh clean package
#   scripts/build.sh test
#   scripts/build.sh -Pintegration verify
#
# The host doesn't need Java or Maven installed.  Dependencies are cached in a
# named Docker volume (kairosdb-connector-m2) so iterative runs are fast.

set -euo pipefail

readonly IMAGE="maven:3.9.10-eclipse-temurin-24-alpine"
readonly CACHE_VOLUME="kairosdb-connector-m2"
readonly REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

docker volume inspect "${CACHE_VOLUME}" >/dev/null 2>&1 \
    || docker volume create "${CACHE_VOLUME}" >/dev/null

exec docker run --rm \
    -v "${REPO_ROOT}:/app" \
    -v "${CACHE_VOLUME}:/root/.m2" \
    -w /app \
    "${IMAGE}" \
    mvn -B "$@"
