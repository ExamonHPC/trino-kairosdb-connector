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

Each release is **one connector version built and tested against one exact
Trino version** — Trino has no cross-version SPI stability, so every
`(connector, Trino)` pair is its own build, published from its own branch.
For connector version `X` (the project `<version>` in `pom.xml`) and Trino
version `Y`:

| Thing                      | Form                                   | Example                                       |
|----------------------------|----------------------------------------|-----------------------------------------------|
| Release branch             | `release/v<X>-trino<Y>`                | `release/v3.0.0-rc1-trino479`                 |
| Git tag / GitHub Release   | `v<X>-trino<Y>`                        | `v3.0.0-rc1-trino479`                         |
| Plugin jar (release asset) | `kairosdb-connector-<X>-trino<Y>.jar`  | `kairosdb-connector-3.0.0-rc1-trino479.jar`   |

Each `release/v<X>-trino<Y>` branch pins everything for its cell in `pom.xml`:
`<version>` = `X`, `<trino.version>` = `Y`, and `<java.version>` to the JDK
that Trino needs (476 → 24, 479 → 25). The shaded-jar name is derived from
those via the maven-shade `finalName`, so a plain `mvn package` on the branch
emits the correctly-named jar. `master` is the **leading edge** (latest
connector × latest Trino) for development; it never publishes.

### Publishing a cell (GitHub Actions)

[`release.yml`](../.github/workflows/release.yml) runs on a push to any
`release/v*-trino*` branch. It reads `<java.version>` from the pom and sets up
that JDK, runs `mvn -Pintegration verify` (unit + Testcontainers integration
tests against the pinned Trino — KairosDB is pulled from the public
`examonhpc/kairosdb` Docker Hub image), then publishes a GitHub Release tagged
`v<X>-trino<Y>` (derived from the branch name) with the shaded jar attached.
It is idempotent: if that release already exists it skips, so a docs-only push
to the branch is a no-op. Jars only — no images. So publishing is just:

```bash
git push -u origin release/v<X>-trino<Y>
```

### Add support for a new Trino version `Z`

1. On `master`, set `<trino.version>=Z` and `<java.version>` to the JDK it needs.
2. Port until `mvn -Pintegration verify` is green. Deps are pinned in `pom.xml`;
   the only Trino-specific source is the `TypeDeserializerModule` call in
   `KairosdbConnectorFactory`; bump the JDK if you hit `UnsupportedClassVersionError`.
3. Cut and push the cell:
   ```bash
   git checkout -b release/v<X>-trino<Z>
   git push -u origin release/v<X>-trino<Z>      # release.yml publishes v<X>-trino<Z>
   ```

### Ship a fix to one or more Trino lines

A connector fix is Trino-agnostic, so it is one commit replayed per line, keeping
the **same** new connector version `X+1` across them (differing only by `-trino<Y>`):

```bash
# author the fix on master (or the newest cell), then for each Trino line:
git checkout release/v<X>-trino<Y>
git cherry-pick <fix-sha>
# bump pom <version> to X+1
git checkout -b release/v<X+1>-trino<Y>
git push -u origin release/v<X+1>-trino<Y>       # release.yml publishes v<X+1>-trino<Y>
```

Releases are immutable: to re-cut for the same Trino after a fix, bump the
connector version so the `v<X+1>-trino<Y>` tag is new. Cherry-pick conflicts are
rare and confined to that one Trino-specific file.

### Build a cell locally

```bash
# on a release/v<X>-trino<Y> branch (its pom already pins X, Y and the JDK):
scripts/build.sh clean package         # -> target/kairosdb-connector-<X>-trino<Y>.jar
scripts/test-integration.sh            # full unit + Testcontainers integration suite
```

## Teardown

```bash
docker compose down                       # stop + remove containers
docker compose down -v                    # also drops the in-memory KairosDB H2 datastore
docker volume rm kairosdb-connector-m2    # clears the Maven dependency cache (~200 MB)
```
