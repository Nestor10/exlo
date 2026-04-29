# Local Development

The fastest way to run a connector locally — no AWS, no servers.

## Option 1: Logging destination (no Iceberg at all)

For iterating on connector logic. Records and commits show up as INFO log lines:

```bash
EXLO_STREAM=kalos sbt 'examples/runMain examples.pokeapi.PokeApiApp'
```

Default `EXLO_DESTINATION=logging` means no catalog, no Parquet, no warehouse — just
log lines you can eyeball.

## Option 2: Local Iceberg (Hadoop catalog)

For validating end-to-end behavior including snapshots, schema, and resume. The Hadoop
catalog stores metadata as files on disk — no servers required:

```bash
EXLO_DESTINATION=iceberg \
EXLO_CATALOG_TYPE=hadoop \
EXLO_CATALOG_WAREHOUSE=/tmp/exlo-warehouse \
EXLO_TABLE_NAMESPACE=exlo \
EXLO_TABLE_NAME=pokeapi_kalos \
EXLO_STREAM=kalos \
sbt 'examples/runMain examples.pokeapi.PokeApiApp'
```

After the run:
- `/tmp/exlo-warehouse/exlo/pokeapi_kalos/metadata/` — Iceberg metadata JSON / Avro
- `/tmp/exlo-warehouse/exlo/pokeapi_kalos/data/` — Parquet data files

Re-running picks up state from the snapshot summary; you should see one new snapshot
per run.

## Verifying the table

Use any Iceberg-aware tool against the warehouse path. The CLI [`pyiceberg`](https://py.iceberg.apache.org/)
is the lightest option:

```bash
pip install pyiceberg
pyiceberg --catalog hadoop --uri file:///tmp/exlo-warehouse list exlo
pyiceberg --catalog hadoop --uri file:///tmp/exlo-warehouse describe exlo.pokeapi_kalos
```

Or query directly from a Spark / DuckDB session pointed at the same warehouse path.

## Inspecting state

Inside the table's `metadata/` directory you'll find versioned metadata JSON files. The
`current-snapshot-id` plus the snapshot's `summary` (containing `exlo.state`) tells you
exactly where the connector would resume from.

## Switching to AWS

When you're ready, swap the catalog config — connector code is unchanged. See
[Configuration](./configuration.md) for the S3 Tables / Glue recipes.

## Related

- [Configuration](./configuration.md)
- [Integration Testing](./integration-testing.md)
