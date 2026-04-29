# exlo

Opinionated Scala 3 / ZIO framework for HTTP ingestion connectors. You describe what to
fetch and how state moves; the framework drives the loop, retries transient network
failures, lands records in Iceberg, and persists state durably.

## Modules

- `exlo` — the framework
- `examples` — runnable connector examples (`pokeapi`, more to come)

## Build & test

```bash
sbt compile
sbt test          # framework + examples
sbt exlo/test     # framework only
```

Iceberg integration tests run against an in-process Hadoop catalog and don't require
Docker.

## Documentation

- [Getting Started](./docs/getting-started.md) — the three connector shapes
  (`HttpExtract.fullPull`, `HttpExtract`, `HttpSlice`) walked through as small sets of
  questions
- [Configuration](./docs/configuration.md) — `EXLO_*` environment variables
- [Local Development](./docs/local-development.md) — run a connector locally
- [Integration Testing](./docs/integration-testing.md)

For framework internals, see `/context/DEVELOPER_GUIDE.md` and `/context/CONFIG.md`.

## Quick start

Run the pokeapi example with a logging destination (no AWS, no Iceberg):

```bash
EXLO_STREAM=kalos sbt 'examples/runMain examples.pokeapi.PokeApiApp'
```

Switch to a local Iceberg warehouse:

```bash
EXLO_DESTINATION=iceberg \
EXLO_CATALOG_TYPE=hadoop \
EXLO_CATALOG_WAREHOUSE=/tmp/exlo-warehouse \
EXLO_TABLE_NAMESPACE=exlo \
EXLO_TABLE_NAME=pokeapi_kalos \
EXLO_STREAM=kalos \
sbt 'examples/runMain examples.pokeapi.PokeApiApp'
```
