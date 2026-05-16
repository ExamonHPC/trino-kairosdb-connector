# trino-kairosdb-connector

A Trino connector for the [KairosDB](https://kairosdb.github.io/) time-series
database. Built as a standalone shaded plugin you can drop into a [Trino](https://trino.io/) instance.

The connector exposes every KairosDB metric as a SQL table under a single
schema (`kairosdb`), with one column per tag plus the synthetic
`timestamp` / `value` columns. Predicates the time-series engine cares about
(timestamp window, tag filters, LIMIT, sampling aggregators) are pushed down
to KairosDB; Trino keeps the rest.

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

1. Build the shaded jar (see [Building from source](#building-from-source) below) or download a release jar.
2. Drop it under `<trino>/plugin/kairosdb/`:
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
| `kairosdb.timestamp.default-start-hours`       | `1`     | Look-back when no `WHERE timestamp ...` is present. Sessionable.                                          |
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

### Pushdown surface

| SQL construct                                    | Pushed to KairosDB?       |
|--------------------------------------------------|---------------------------|
| `timestamp = c` / `BETWEEN a AND b` / `IN(a,b,c)`| Yes (start/end absolute)  |
| `tag = 'v'` / `tag IN ('v1','v2')`               | Yes                       |
| `LIMIT n`                                        | Yes (per metric series)   |
| `sampling_aggregator = 'sum;1h;start_time'`      | Yes (see below)           |
| `tag <  'v'`, `tag LIKE '%x'`, joins, sub-queries| No, evaluated by Trino    |

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

#### Current behavior (v3.0.0+)
In the rare cases where two or more KairosDB metrics share the same lowercase form, 
the connector exposes *every* member of the collision group under a hash-suffixed name 
to `<lowercase>__<6 hex of sha256(original)>` and is fully deterministic across nodes and restarts.

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

Requires JDK 24 and Maven 3.9+:

```bash
mvn clean package
```

The shaded jar lands at `target/kairosdb-connector-<version>.jar`.

Dev stack documented in [`scripts/README.md`](scripts/README.md).

## Compatibility

- Trino: built and tested against **476**. The SPI surface used is stable
  back a few releases; rebuild against a different `trino.version` in
  `pom.xml` to target another version.
- KairosDB: tested against **1.2.x**. Any KairosDB whose `/api/v1` HTTP
  surface accepts the standard `metricnames` / `datapoints/query/tags` /
  `datapoints/query` endpoints will work.

## License

To be added by the maintainer before publishing.
