# trino-kairosdb-connector

[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
[![Latest release](https://img.shields.io/github/v/release/ExamonHPC/trino-kairosdb-connector?label=release)](https://github.com/ExamonHPC/trino-kairosdb-connector/releases/latest)
[![Trino](https://img.shields.io/badge/Trino-479+-blue.svg)](https://trino.io/)
[![JDK](https://img.shields.io/badge/JDK-25-blue.svg)](https://openjdk.org/)
[![KairosDB](https://img.shields.io/badge/KairosDB-1.2.x-blue.svg)](https://kairosdb.github.io/)

A Trino connector for the [KairosDB](https://kairosdb.github.io/) time-series
database. Built as a standalone shaded plugin you can drop into a [Trino](https://trino.io/) instance.

The connector exposes every KairosDB metric as a SQL table under a single
schema (`kairosdb`), with one column per tag plus the synthetic
`timestamp` / `value` columns. Predicates the time-series engine cares about
(timestamp window, tag filters, LIMIT, sampling aggregators) are pushed down
to KairosDB; Trino keeps the rest.

## Status

Status: public beta / release candidate.

## Features

- **Metric and tag discovery** with per-metric tag-schema caching.
- **Predicate pushdown** for `timestamp` (point, range, multi-range), tag
  equality and `IN(...)`, LIMIT, and chained sampling aggregators.
- **Configurable timestamp column type**: `BIGINT` (epoch ms),
  `TIMESTAMP(3)`, or `TIMESTAMP(3) WITH TIME ZONE`.
- **Per-session overrides** for the split-shaping knobs (`split_size_millis`
  and `default_start_hours`).
- **Multi-worker safe**.
- **Multiple catalogs** against different KairosDB server.

## Install

1. Download the release jar matching your Trino version from the
   [Releases page](https://github.com/ExamonHPC/trino-kairosdb-connector/releases)
   (artifacts are tagged `…-trino<N>`), or build the shaded jar yourself
   (see [Building from source](#building-from-source) below). A pre-built Trino
   image with the plugin baked in is also published to GHCR:
   ```bash
   docker pull ghcr.io/examonhpc/trino-kairosdb-connector:trino<N>
   ```
2. Drop the jar under `<trino>/plugin/kairosdb/`:
   ```text
   <trino>/plugin/kairosdb/kairosdb-connector-<version>.jar
   ```
3. Add a catalog file at `<trino>/etc/catalog/kairosdb.properties` -
   the [example](trino-config/catalog/kairosdb.properties) in this repo
   documents every option:
   ```properties
   connector.name=kairosdb
   kairosdb.url=http://kairosdb-host:8080
   ```
4. Restart Trino. The catalog `kairosdb` (named after the properties file)
   is now selectable from any SQL client.

## Configuration reference

| Property                                       | Default | Notes                                                                                                   |
|------------------------------------------------|---------|----------------------------------------------------------------------------------------------------------|
| `kairosdb.url`                                 | -       | Required. KairosDB HTTP base URL.                                                                        |
| `kairosdb.timestamp.format`                    | `BIGINT`| `BIGINT`, `TIMESTAMP_MILLIS`, or `TIMESTAMP_TZ`.                                                          |
| `kairosdb.timestamp.default-start-hours`       | `1`     | Look-back applied when the query has no lower-bound timestamp predicate (no `WHERE timestamp ...`, or upper-bound only). Sessionable as `default_start_hours`. |
| `kairosdb.split-size`                          | `1d`    | Width of one time-range split. Sessionable as `split_size_millis`.                                        |
| `kairosdb.metadata.cache-ttl`                  | `30s`   | Per-worker cache for metric names and per-metric tag schemas.                                             |
| `kairosdb.read-timeout`                        | `60s`   | OkHttp read timeout for calls to KairosDB.                                                                |
| `kairosdb.case-insensitive-name-matching`      | `true`  | When true, mixed-case KairosDB metrics are reachable via their lowercase form (recommended), dropped when false. Independently, KairosDB metrics that share a lowercase form get hash-suffixed Trino-side names; see [Case collisions](#case-collisions) below.|

Legacy unprefixed keys (`url`, `split.size.millis`, `timestamp.format`,
`timestamp.default.start.hours`) are still accepted as aliases so older
catalog files keep working without modification.

## Query

```sql
-- Every metric KairosDB knows about is a table under the `kairosdb` schema.
SHOW TABLES FROM kairosdb.kairosdb;

-- A typical time-window read.  `timestamp` is the synthetic column; tag
-- columns carry the original KairosDB tag values (lowercased on the SQL
-- side by Trino, case-preserving on the wire).
SELECT timestamp, value, host, zone
FROM   kairosdb.kairosdb."sys.load"
WHERE  timestamp BETWEEN 1750000000000 AND 1750000600000
  AND  host = 'h1'
ORDER  BY timestamp
LIMIT  100;
```

### Working with values

The `value` column is exposed as `VARCHAR`. KairosDB allows per-metric value
types (numeric, string, complex/histogram), so the connector echoes whatever
the storage layer returns rather than guessing a type per metric. For metrics
you know to be numeric, cast at query time:

```sql
SELECT timestamp, CAST(value AS DOUBLE) AS reading
FROM   kairosdb.kairosdb.cpu1_temp
WHERE  timestamp BETWEEN 1779049232000 AND 1779049832000;
```

If you find yourself writing the cast everywhere for a given metric, a SQL
view (`CREATE VIEW`) over the cast is the cleanest way to hide it from
downstream consumers without touching the connector.

### Timestamp literals

KairosDB stores datapoints as epoch milliseconds in UTC, so every
`WHERE timestamp …` predicate resolves to a UTC range under the hood.

With `kairosdb.timestamp.format=BIGINT` (default) write raw epoch
milliseconds, e.g. `WHERE timestamp BETWEEN 1750370400000 AND 1750456800000`

With `TIMESTAMP_MILLIS` or `TIMESTAMP_TZ` write SQL TIMESTAMP literals.
Four styles cover the common cases (target instant: midnight 2025-06-20
local Rome time, which is `2025-06-19T22:00:00Z`):

| Style | Example | Notes |
|---|---|---|
| Pinned literal | `timestamp '2025-06-20 00:00:00 Europe/Rome'` | Self-documenting, immune to session-zone changes. |
| Session-relative | `SET TIME ZONE 'Europe/Rome';` then `timestamp '2025-06-20 00:00:00'` | Bare literal picks up the session zone. |
| ISO-8601 with offset | `from_iso8601_timestamp('2025-06-20T00:00:00+02:00')` | Useful when the offset is a parameter. |
| UTC explicit | `timestamp '2025-06-20 00:00:00 UTC'` | Simplest for operational queries. |

`TIMESTAMP_TZ` results are packed in UTC; project with `AT TIME ZONE` to
display rows in a local zone (the predicate still pushes a UTC window to
KairosDB):

```sql
SELECT timestamp AT TIME ZONE 'Europe/Rome' AS ts, value
FROM   kairosdb.kairosdb."sys.load"
WHERE  timestamp >= timestamp '2025-06-20 00:00:00 Europe/Rome';
```

### Pushdown surface

Pushdown is *strict*: a predicate is claimed as fully handled only when
KairosDB can natively evaluate it.  Anything else stays in Trino's residual
filter (and may also be pushed as a wider bounding window, see below).

| SQL construct                                                                | Pushed to KairosDB?                                          |
|------------------------------------------------------------------------------|--------------------------------------------------------------|
| `timestamp = c` / `BETWEEN a AND b` / both-sides-bounded `> / < / >= / <=`   | Yes, exactly (1 ms shift applied for half-open bounds)       |
| `timestamp >= a` / `timestamp > a` (low-only bound)                          | Yes, exactly - high resolves to `now()` (KairosDB has no future data) |
| `timestamp <= b` / `timestamp < b` (high-only bound)                         | Pushed within `[b − default_start_hours, b]`; rows older than the look-back are not fetched (see below) |
| `timestamp IN (a, b, c)` / `BETWEEN ... AND ... AND != t` / disjoint OR-of-ranges | Convex-hull window pushed as a fetch hint; Trino re-applies the predicate above the connector |
| `timestamp != t` (no other timestamp bound)                                  | No hull (predicate spans all of time); falls back to `[now − default_start_hours, now]` with the predicate left residual |
| `tag = 'v'` / `tag IN ('v1', 'v2')`                                          | Yes, exactly                                                 |
| `tag > 'v'` / `tag != 'v'` / `tag NOT IN (...)` / `tag IS NULL` / `tag LIKE` | No, evaluated entirely by Trino (KairosDB has no native shape) |
| `LIMIT n`                                                                    | Yes (per metric series; Trino keeps its own LIMIT as backstop) |
| `sampling_aggregator = 'sum;1h;start_time'`                                  | Yes (see *Sampling aggregators* below)                       |
| joins, sub-queries, expressions                                              | No, evaluated by Trino                                       |

#### How "bounding window + residual" works

For multi-range timestamp predicates (`timestamp IN (...)`, disjoint
OR-of-ranges, `!=` combined with a bounded range) the connector pushes
the *convex hull* of the predicate as KairosDB's
`[start_absolute, end_absolute]` window and leaves the original
predicate in Trino's residual filter.  KairosDB scans the bounded window
only and avoid full table scan while Trino reduces the result above the
connector to exactly the rows the predicate admits.  The
anomaly-correlation shape `WHERE timestamp IN (t1, t2, t3)` ends up
scanning `[min(tᵢ), max(tᵢ)]` and returning at most three rows per series.

A standalone `timestamp != T` has no convex hull (it spans all of time)
and cannot be pushed as a window: the connector falls back to the
default look-back and leaves the `!=` predicate residual.  In practice,
combine `!=` with a bounded timestamp range so the window pushed to
KairosDB stays narrow.

When a query has no timestamp predicate at all, the connector applies a
default look-back of `kairosdb.timestamp.default-start-hours` (overridable
per session via `default_start_hours`).  When only an upper bound is given
(e.g. `WHERE timestamp <= old_T`) the default look-back is anchored to
that upper bound, never to `now()`, so historical queries return
`[old_T − default_start_hours, old_T]`.

### Sampling aggregators

KairosDB's downsampling aggregators are exposed through a hidden virtual
column called `sampling_aggregator` (`VARCHAR`). Push one or many through:

```sql
-- Single aggregator: sum into 1-hour buckets aligned to bucket start time
SELECT * FROM kairosdb.kairosdb."sys.load"
WHERE  sampling_aggregator = 'sum;1h;start_time'
  AND  timestamp BETWEEN 1750000000000 AND 1750086400000;

-- Chained aggregators: the output of the first feeds the input of the next.
-- Use a single VARCHAR value separated by `|` to preserve order.
WHERE  sampling_aggregator = 'sum;1h;start_time|avg;6h;sampling'
```

Spec format is `type;interval;alignment`:

- **type**: `avg sum min max count first last dev percentile rate sampler scale trim gaps histogram least_squares`
- **interval**: `<digits>[smhd]?` - bare digits default to minutes.
- **alignment**: `start_time`, `end_time`, `sampling`, or `none`.

`IN(...)` is accepted for backward compatibility but Trino delivers the
values in alphabetical order, so the chain order would be SQL-determined
rather than user-controlled. The connector logs a `WARN` when this form
is used with more than one value; prefer the `|`-separated single value.

### Case collisions

KairosDB is case-sensitive (`pue` and `Pue` are distinct metrics); Trino's
catalog API is not (it lowercases all identifiers). 

#### Current behavior
In the rare cases where two or more KairosDB metrics share the same lowercase form, 
the connector exposes *every* member of the collision group under a hash-suffixed name off the form
 `<lowercase>__<6 hex of sha256(original)>` and is fully deterministic across nodes and restarts.

`SHOW TABLES` lists the mangled names:

```text
trino> SHOW TABLES FROM kairosdb.kairosdb LIKE 'pue%';
    Table
-------------
 pue__b67def
 pue__ed3ffe
 pue__f90207
(3 rows)
```

`system.metadata.table_comments` reveals the mapping back to the original
KairosDB metric name:

```text
trino> SELECT table_name, comment
    -> FROM   system.metadata.table_comments
    -> WHERE  catalog_name = 'kairosdb'
    ->   AND  schema_name  = 'kairosdb'
    ->   AND  table_name LIKE 'pue%';
 table_name  |                                     comment
-------------+---------------------------------------------------------------------------------
 pue__f90207 | KairosDB metric "pue" (case-mangled: original case differs from another metric)
 pue__b67def | KairosDB metric "Pue" (case-mangled: original case differs from another metric)
 pue__ed3ffe | KairosDB metric "PUE" (case-mangled: original case differs from another metric)
(3 rows)
```

Querying the unmangled lowercase name fails loudly rather than silently
returning the wrong metric:

```text
trino> SELECT * FROM kairosdb.kairosdb.pue;
Query 20260101_000000_00000_xxxxx failed: line 1:15: Table 'kairosdb.kairosdb.pue' does not exist
```

Metrics that don't collide are unaffected: a single mixed-case metric
like `Sys.Mem` is reachable as `sys.mem` (controlled by
`kairosdb.case-insensitive-name-matching`).

### Per-query session overrides

```sql
-- Use smaller splits for this query only (more parallelism, more roundtrips)
SET SESSION kairosdb.split_size_millis = 3600000;        -- 1h

-- Look back 24h when the query has no timestamp predicate
SET SESSION kairosdb.default_start_hours = 24;
```

Both fall back to the catalog defaults (`kairosdb.split-size` and
`kairosdb.timestamp.default-start-hours`).

## Building from source

Requires JDK 25 and Maven 3.9+:

```bash
mvn clean package
```

The shaded jar lands at `target/kairosdb-connector-<version>.jar`.

Dev stack documented in [`scripts/README.md`](scripts/README.md).

## Continuous integration & releases

GitHub Actions workflows under [`.github/workflows/`](.github/workflows/):

| Workflow            | Trigger                                   | What it does                                                                                                   |
|---------------------|-------------------------------------------|----------------------------------------------------------------------------------------------------------------|
| `ci.yml`            | push / PR to `master`                     | Builds the jar and runs unit tests; integration tests run on `master` and on PRs labelled `integration`.       |
| `release.yml`       | push to `master` (version change), or `workflow_dispatch` | Builds + unit/integration-tests the pinned version, then publishes the jar to a GitHub Release and the image to GHCR. Idempotent: skips if that `…-trino<N>` release already exists. |
| `trino-watcher.yml` | daily cron, or `workflow_dispatch`         | Detects a newer `trinodb/trino` release and opens a **draft PR** bumping `trino.version`, with a port checklist. It does not release. |

The supported Trino version is the one pinned in `pom.xml` (`trino.version`) on
`master` — it is controlled explicitly, **not** auto-chased. Because Trino offers
no cross-version SPI stability (see [Compatibility](#compatibility)), adopting a
new release is a reviewed step: the watcher proposes the bump as a draft PR, CI
runs the port attempt, a human finishes it (deps / SPI / JDK as needed), and
**merging the PR** publishes the new `…-trino<N>` artifacts. To (re)build a
specific older version, dispatch the **Release** workflow with that
`trino_version`.

## Compatibility

| Component              | Tested with                                        | Notes                                                                                                                                                  |
|------------------------|----------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------|
| Trino                  | **479** (currently supported)                      | Trino guarantees no cross-version SPI stability, so each release is built and tested against one exact Trino version. The supported version is pinned in `pom.xml` and advanced deliberately via a reviewed PR (see [CI & releases](#continuous-integration--releases)). Each supported version is published as a `…-trino<N>` artifact (jar + GHCR image); pick the one matching your Trino. Older artifacts remain on the Releases page but only the pinned version receives fixes. |
| KairosDB               | **1.2.x** (1.2.2 in CI via `examonhpc/kairosdb`)   | Any KairosDB exposing the `/api/v1/metricnames`, `/api/v1/datapoints/query`, and `/api/v1/datapoints/query/tags` endpoints should work.                |
| JDK (build)            | **25**                                             | Required by `mvn package` (Trino 479+ targets Java 25).                                                                                                                             |
| OS (build / runtime)   | Linux x86_64, Linux arm64, macOS arm64             | Inherited from Trino and the (pure-Java, native-free) bundled dependencies.                                                                            |
| KairosDB storage layer | Cassandra 3.11+                                    | Any CQL-3 backend KairosDB itself can use is transparent to the connector and to SQL.                                                                  |

## License

Licensed under the [Apache License, Version 2.0](LICENSE). Third-party
attributions are listed in [`NOTICE`](NOTICE).
