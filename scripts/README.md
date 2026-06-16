# Developer setup

This folder bundles the Docker-based build, test and run-loop helpers used
during development. They are also a complete recipe for anyone who wants
to reproduce the dev environment without installing a JDK, Maven, Trino,
or KairosDB on the host - the host only needs Docker.

If you already have JDK 25 + Maven 3.9+ on the host, plain `mvn clean
package` from the repo root works equivalently for the build itself; the
extras here are about iterating against a live Trino + KairosDB stack.

## Prerequisites

- Docker Engine with the `compose` plugin (Docker Desktop on macOS /
  Windows works too).
- ~1 GB of disk for the Maven builder image, the Maven dependency
  cache volume, and the Trino + KairosDB images.
- Outbound HTTPS the first time, to pull images and Maven dependencies.

## Repo layout that matters here

```
.
├── Dockerfile                          production-style multi-stage image (build + runtime)
├── docker-compose.yml                  dev stack: Trino + KairosDB
├── pom.xml                             vanilla Maven project
├── trino-config/catalog/
│   └── kairosdb.properties             dev catalog, bind-mounted into the dev Trino
├── plugin/kairosdb/                    populated by package.sh; bind-mounted into the dev Trino
└── scripts/
    ├── build.sh                        run any Maven goal in the builder image
    ├── package.sh                      build + stage the shaded jar into plugin/kairosdb/
    ├── redeploy.sh                     package + restart Trino + wait for SQL to be live
    └── test-integration.sh             run the @Tag("integration") tests against a Testcontainers KairosDB
```

## Script reference

| Script                | What it does                                                                                  |
|-----------------------|-----------------------------------------------------------------------------------------------|
| `build.sh`            | Run any Maven goals inside the pinned `maven:3.9.11-eclipse-temurin-25-alpine` image.         |
| `package.sh`          | Build the shaded jar and stage it into `plugin/kairosdb/` so `docker-compose` picks it up.    |
| `redeploy.sh`         | `package.sh` + restart the dev Trino container + block until SQL is actually live.            |
| `test-integration.sh` | Run the `@Tag("integration")` tests; spawns a real KairosDB via Testcontainers.               |

All four scripts cache the Maven repo in a named Docker volume
(`kairosdb-connector-m2`) so iterative runs are fast - first invocation
populates it, subsequent invocations reuse it.

## Build and test

```bash
scripts/build.sh clean package    # full build, produces target/kairosdb-connector-*.jar
scripts/build.sh test             # in-process unit tests (a few seconds)
scripts/test-integration.sh       # Testcontainers-backed end-to-end tests
```

`build.sh` is a thin Maven wrapper: anything you'd pass to `mvn` works
(`scripts/build.sh -Pintegration verify`, `scripts/build.sh -DskipTests
package`, etc.). The first invocation pulls the builder image (~250 MB).

`test-integration.sh` mounts the host's Docker socket and shares its
network namespace so Testcontainers can spawn KairosDB on the same engine
that runs the builder. The first run pulls `examonhpc/kairosdb:1.2.2` (a
few hundred MB); subsequent runs reuse the cached image and finish in
seconds.

## Local dev loop against a live Trino + KairosDB

`docker-compose.yml` brings up a one-worker Trino and a self-contained
KairosDB (in-process H2 datastore, no external Cassandra). The plugin jar
and the catalog file are bind-mounted from the host:

```yaml
volumes:
  - ./plugin/kairosdb:/usr/lib/trino/plugin/kairosdb
  - ./trino-config/catalog:/etc/trino/catalog
```

That bind-mount is what makes the loop fast: rebuilding the jar locally
and restarting Trino is ~15 s end-to-end, no image rebuilds.

First-time setup:

```bash
docker compose up -d              # starts trino + kairosdb (cold start ~30 s)
scripts/redeploy.sh               # build + stage jar + restart trino + wait for SQL
docker compose exec trino trino   # opens the Trino CLI
```

Iterative loop:

```bash
scripts/redeploy.sh               # rebuilds, restarts, waits
# or, if you've only edited resources / want to skip "clean":
scripts/redeploy.sh package
```

From the Trino CLI:

```sql
trino> SHOW TABLES FROM kairosdb.kairosdb;
trino> SELECT * FROM kairosdb.kairosdb."your.metric" LIMIT 5;
```

`redeploy.sh` polls `/v1/info` and a `SELECT 1` query before returning,
so when it exits the cluster is genuinely ready - no `NO_NODES_AVAILABLE`
race on the very first query after a restart.

The dev Trino exposes its UI on the host at <http://localhost:8082>. The
dev KairosDB is internal-only by default; uncomment the `ports:` block in
`docker-compose.yml` to reach it from the host.

## Dev-stack configuration

The dev catalog (bind-mounted into the dev Trino) is
[`trino-config/catalog/kairosdb.properties`](../trino-config/catalog/kairosdb.properties).
It's a fully-commented copy of every option, with values pinned to
defaults except for `kairosdb.url=http://kairosdb:8080` (resolves over
`docker-compose`'s internal network). Adjust it locally for your
preferred timestamp format, split size, etc.; restart Trino with
`docker compose restart trino` (or just rerun `scripts/redeploy.sh`) to
pick up the change.

To point the dev Trino at a real KairosDB instead of the bundled
container: edit `kairosdb.url` in `trino-config/catalog/kairosdb.properties`,
remove the `kairosdb` service from `docker-compose.yml` (or just bring
only `trino` up: `docker compose up -d trino`), and `redeploy.sh`.

## Production-style image (optional)

For a self-contained image (no bind-mounts, plugin baked in) build the
project [`Dockerfile`](../Dockerfile):

```bash
docker build -t trino-kairosdb:local .
```

Two-stage build: the first stage compiles and shades the jar, the second
stage starts from `trinodb/trino:<TRINO_VERSION>` and copies the jar into
`/usr/lib/trino/plugin/kairosdb/`. The Trino version is a build arg
(default `479`); both the maven build and the runtime base image track it:

```bash
docker build --build-arg TRINO_VERSION=479 -t trino-kairosdb:trino479 .
```

Useful for CI artefacts and parity checks but unnecessary for the iterative
dev loop above.

## Releasing

A release is **one artifact built and tested against one exact Trino
version** - Trino guarantees no cross-version SPI stability, so each Trino
version needs its own build. Artifacts for a connector version `V` (from
`pom.xml`) and Trino version `N` are named:

| Artifact            | Pattern                               | Example                                   |
|---------------------|---------------------------------------|-------------------------------------------|
| GitHub Release tag  | `<V>-trino<N>`                        | `3.0.0-rc1-trino479`                      |
| Plugin jar          | `kairosdb-connector-<V>-trino<N>.jar` | `kairosdb-connector-3.0.0-rc1-trino479.jar` |
| GHCR image          | `…:trino<N>`, `…:<V>-trino<N>`, `…:latest` | `…:trino479`                         |

`:latest` (and the GitHub "Latest" flag) only ever points at the **newest**
released Trino version, so back-building an older one never demotes a newer
release.

### Via GitHub Actions (recommended)

The supported Trino version is the one pinned in `pom.xml` (`trino.version`)
on `master`. The normal flow is hands-off after review:

1. [`trino-watcher.yml`](../.github/workflows/trino-watcher.yml) polls
   `trinodb/trino` daily and, when a newer version appears, opens a **draft
   PR** bumping `trino.version` with a port checklist. It does not release.
2. Finish the port on that branch (deps / SPI / JDK as CI reveals) until CI
   is green, then merge.
3. The merge changes `pom.xml` on `master`, which triggers
   [`release.yml`](../.github/workflows/release.yml): it builds, runs
   unit + integration tests, and - only if green - publishes the jar to a
   GitHub Release and the image to GHCR. It is idempotent (skips if that
   `…-trino<N>` release already exists), so it never republishes a version.

To back-build a specific older Trino version (e.g. for a team that hasn't
upgraded), dispatch the Release workflow directly:

```bash
gh workflow run release.yml -f trino_version=479
# Empty trino_version uses the value pinned in pom.xml.
# Or: Actions tab -> "Release" -> Run workflow.
```

> Releases are immutable. To re-cut for the same Trino version after a fix,
> bump the connector version (the project `<version>` in `pom.xml`) so the
> `…-trino<N>` tag is new.

### Manual / local

When you need an artifact without Actions (air-gapped, ad-hoc), reproduce
what the workflow does:

```bash
# 1. Build + test the jar against the target Trino version.
scripts/build.sh -Dtrino.version=479 -Pintegration verify
#    Shaded jar lands at target/kairosdb-connector-<V>.jar.
#    (Add -Dtest=... or drop -Pintegration to skip the Testcontainers suite.)

# 2. Optional: build the self-contained runtime image for that version.
docker build --build-arg TRINO_VERSION=479 -t trino-kairosdb:trino479 .
```

To match the workflow's artifact naming, stamp the Trino version into the
version before packaging:

```bash
scripts/build.sh versions:set -DnewVersion=3.0.0-rc1-trino479 -DgenerateBackupPoms=false
scripts/build.sh -Dtrino.version=479 -Pintegration verify
# -> target/kairosdb-connector-3.0.0-rc1-trino479.jar
```

## Teardown

```bash
docker compose down                       # stop + remove containers
docker compose down -v                    # also drops the in-memory KairosDB H2 datastore
docker volume rm kairosdb-connector-m2    # clears the Maven dependency cache (~200 MB)
```
