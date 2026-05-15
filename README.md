# trino-kairosdb-connector

A Trino connector for the [KairosDB](https://kairosdb.github.io/) time-series database.

> Status: work in progress (v3 series). The plugin is built as a standalone shaded JAR
> that can be dropped into `<trino>/plugin/kairosdb/`. Full documentation will land
> with the first feature release.

## Build

```bash
mvn -q clean package
```

The resulting `target/kairosdb-connector-<version>.jar` is the only artifact you need
to deploy. Unit tests run as part of `mvn package`; the integration test suite
(against a real KairosDB container) is gated behind the `integration` Maven profile:

```bash
mvn -q -Pintegration verify
```
