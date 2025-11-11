# Local Development

Run EXLO locally with MinIO and Nessie (no cloud services needed).

## Quick Start

### 1. Start Services

```bash
docker-compose up -d
```

(Assumes you have a docker-compose.yml with MinIO and Nessie)

### 2. Configure Environment

```bash
export EXLO_STORAGE_WAREHOUSE_PATH="s3://warehouse"
export EXLO_STORAGE_BACKEND="S3"
export EXLO_STORAGE_BACKEND_S3_REGION="us-east-1"
export EXLO_STORAGE_BACKEND_S3_ENDPOINT="http://localhost:9000"
export EXLO_STORAGE_BACKEND_S3_ACCESS_KEY_ID="minioadmin"
export EXLO_STORAGE_BACKEND_S3_SECRET_ACCESS_KEY="minioadmin"
export EXLO_STORAGE_CATALOG="NESSIE"
export EXLO_STORAGE_CATALOG_NESSIE_URI="http://localhost:19120/api/v1"
export EXLO_STREAM_NAMESPACE="raw"
export EXLO_STREAM_TABLE_NAME="test"
export EXLO_SYNC_STATE_VERSION="1"
```

### 3. Run Example

```bash
sbt "examples/runMain examples.SimpleConnector"
```

## Docker Compose Example

```yaml
services:
  nessie:
    image: projectnessie/nessie:latest
    ports:
      - "19120:19120"
  
  minio:
    image: minio/minio:latest
    ports:
      - "9000:9000"
      - "9001:9001"
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    command: server /data --console-address ":9001"
```

## Verify Data

Access MinIO console: http://localhost:9001
Access Nessie API: http://localhost:19120/api/v1

## Related

- [Configuration](./configuration.md)
- [Testing](./testing.md)
