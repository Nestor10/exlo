# Configuration

exlo is configured entirely through `EXLO_*` environment variables — no per-environment
code changes. The full reference lives at `/context/CONFIG.md`; this page is the quick
tour.

## Always required

| Variable | Example | Notes |
|---|---|---|
| `EXLO_STREAM` | `kalos` | Selects which stream to run from the connector's `StreamRegistry`. Required even for single-stream apps. |

## Destination

| Variable | Example | Notes |
|---|---|---|
| `EXLO_DESTINATION` | `iceberg` | `logging` (default — INFO logs only) or `iceberg` (durable) |

When `EXLO_DESTINATION=iceberg`, also set:

| Variable | Example | Notes |
|---|---|---|
| `EXLO_CATALOG_TYPE` | `s3tables` | `s3tables`, `glue`, or `hadoop` |
| `EXLO_CATALOG_WAREHOUSE` | `arn:aws:s3tables:...` / `s3://lake/warehouse` / `/tmp/wh` | Table-bucket ARN, S3 path, or local FS path |
| `EXLO_CATALOG_REGION` | `us-east-1` | Required for `s3tables`; optional for `glue`; ignored for `hadoop` |
| `EXLO_TABLE_NAMESPACE` | `exlo` | Iceberg namespace |
| `EXLO_TABLE_NAME` | `pokeapi_kalos` | Iceberg table name |

## Sink tuning (optional)

| Variable | Default | Notes |
|---|---|---|
| `EXLO_SINK_MAX_RECORDS` | `1000` | Flush after N records |
| `EXLO_SINK_MAX_INTERVAL` | `PT30S` | Flush after this duration (ISO-8601) |
| `EXLO_SINK_BUFFER_CAPACITY` | `10000` | Bounded TQueue size |

## Common recipes

**Logging destination (dev):**
```bash
EXLO_STREAM=kalos sbt 'examples/runMain examples.pokeapi.PokeApiApp'
```

**Local Iceberg (no AWS):**
```bash
EXLO_DESTINATION=iceberg \
EXLO_CATALOG_TYPE=hadoop \
EXLO_CATALOG_WAREHOUSE=/tmp/exlo-warehouse \
EXLO_TABLE_NAMESPACE=exlo \
EXLO_TABLE_NAME=pokeapi_kalos \
EXLO_STREAM=kalos \
sbt 'examples/runMain examples.pokeapi.PokeApiApp'
```

**AWS S3 Tables (managed Iceberg):**
```bash
EXLO_DESTINATION=iceberg \
EXLO_CATALOG_TYPE=s3tables \
EXLO_CATALOG_WAREHOUSE=arn:aws:s3tables:us-east-1:123456789012:bucket/my-table-bucket \
EXLO_CATALOG_REGION=us-east-1 \
EXLO_TABLE_NAMESPACE=exlo \
EXLO_TABLE_NAME=pokeapi_kalos \
EXLO_STREAM=kalos \
sbt 'examples/runMain examples.pokeapi.PokeApiApp'
```

**AWS Glue + S3:**
```bash
EXLO_DESTINATION=iceberg \
EXLO_CATALOG_TYPE=glue \
EXLO_CATALOG_WAREHOUSE=s3://my-data-lake/warehouse \
EXLO_TABLE_NAMESPACE=exlo \
EXLO_TABLE_NAME=pokeapi_kalos \
EXLO_STREAM=kalos \
AWS_REGION=us-east-1 \
sbt 'examples/runMain examples.pokeapi.PokeApiApp'
```

## Connector-specific config

For your own config (API keys, endpoints, etc.) use ZIO Config under your own namespace:

```scala
final case class MyApiConfig(apiKey: String, baseUrl: String)
object MyApiConfig:
  val config = deriveConfig[MyApiConfig].nested("myapi")
  val layer  = ZLayer.fromZIO(ZIO.config(config))
```

Set with `MYAPI_API_KEY=...` / `MYAPI_BASE_URL=...`. Provide the layer alongside
`Client.default` when you call `Exlo.run(...).provide(...)`.

## Related

- [Local Development](./local-development.md)
- [Integration Testing](./integration-testing.md)
- Full reference: `/context/CONFIG.md`
