# Configuration

EXLO configuration via environment variables.

## Required Variables

### Storage
- `EXLO_STORAGE_WAREHOUSE_PATH` - S3/GCS path
- `EXLO_STORAGE_BACKEND` - `S3`, `GCS`, `AZURE`, `LOCAL`

### Catalog
- `EXLO_STORAGE_CATALOG` - `NESSIE`, `GLUE`, `HIVE`, `JDBC`, `DATABRICKS`

### Stream
- `EXLO_STREAM_NAMESPACE` - Iceberg namespace (e.g., `raw`)
- `EXLO_STREAM_TABLE_NAME` - Table name

### State
- `EXLO_SYNC_STATE_VERSION` - Increment to force fresh start

## Backend-Specific

### S3
```bash
EXLO_STORAGE_BACKEND_S3_REGION="us-east-1"
EXLO_STORAGE_BACKEND_S3_ENDPOINT="http://localhost:9000"  # Optional
```

### Nessie Catalog
```bash
EXLO_STORAGE_CATALOG_NESSIE_URI="http://localhost:19120/api/v1"
EXLO_STORAGE_CATALOG_NESSIE_REF="main"  # Optional
```

## Local Development

See: `/docs/local-development.md`

## Custom Connector Config

Use `zio-config` for your own configuration:

```scala
case class MyConfig(apiKey: String, endpoint: String)

object MyConfig:
  val config = deriveConfig[MyConfig].nested("myconnector")
  val layer = ZLayer.fromZIO(ZIO.config(config))
```

Set via environment:
```bash
MYCONNECTOR_API_KEY="sk_test_123"
MYCONNECTOR_ENDPOINT="https://api.example.com"
```

## Related

- [Local Development](./local-development.md)
- Full config reference: `/context/CONFIGURATION.md`
