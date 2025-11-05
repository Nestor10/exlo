package exlo.domain

/**
 * Storage backend configuration - defines WHERE data files are stored.
 *
 * This is separate from CatalogConfig which defines WHERE metadata is stored.
 * Iceberg uses this to configure its FileIO implementation for reading/writing Parquet files.
 */
sealed trait StorageBackend:
  /**
   * Convert to Iceberg properties map for catalog initialization.
   * This generates the properties that Iceberg needs to configure its FileIO.
   */
  def toIcebergProperties: Map[String, String]

object StorageBackend:

  /**
   * AWS S3 storage backend.
   *
   * Uses org.apache.iceberg.aws.s3.S3FileIO (requires iceberg-aws dependency).
   * Directly uses AWS SDK v2 - no Hadoop required.
   *
   * Authentication options (in priority order):
   * 1. IAM roles (preferred) - leave credentials as None
   * 2. Environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
   * 3. Explicit credentials (accessKeyId, secretAccessKey)
   *
   * @param region AWS region (e.g., "us-east-1")
   * @param endpoint Optional custom endpoint (for MinIO, LocalStack testing)
   * @param accessKeyId Optional access key (None = use IAM role or env vars)
   * @param secretAccessKey Optional secret key (None = use IAM role or env vars)
   * @param pathStyleAccess Use path-style access (required for MinIO, default false)
   */
  final case class S3(
    region: String,
    endpoint: Option[String] = None,
    accessKeyId: Option[String] = None,
    secretAccessKey: Option[String] = None,
    pathStyleAccess: Boolean = false
  ) extends StorageBackend:

    def toIcebergProperties: Map[String, String] =
      val base = Map(
        "io-impl"       -> "org.apache.iceberg.aws.s3.S3FileIO",
        "client.region" -> region
      )

      val withEndpoint = endpoint match
        case Some(ep) => base + ("s3.endpoint" -> ep)
        case None     => base

      val withPathStyle =
        if pathStyleAccess then withEndpoint + ("s3.path-style-access" -> "true")
        else withEndpoint

      val withCredentials = (accessKeyId, secretAccessKey) match
        case (Some(keyId), Some(secret)) =>
          withPathStyle ++ Map(
            "s3.access-key-id"     -> keyId,
            "s3.secret-access-key" -> secret
          )
        case _                           => withPathStyle

      withCredentials

  /**
   * Google Cloud Storage backend.
   *
   * Uses org.apache.iceberg.gcp.gcs.GCSFileIO (requires iceberg-gcp dependency).
   * Directly uses Google Cloud Storage client libraries.
   *
   * Authentication options (in priority order):
   * 1. Application Default Credentials (preferred) - leave credentials as None
   * 2. Service account JSON key (credentials field)
   *
   * @param projectId GCP project ID
   * @param credentials Optional service account JSON key (None = use ADC)
   */
  final case class Gcs(
    projectId: String,
    credentials: Option[String] = None
  ) extends StorageBackend:

    def toIcebergProperties: Map[String, String] =
      val base = Map(
        "io-impl"        -> "org.apache.iceberg.gcp.gcs.GCSFileIO",
        "gcs.project-id" -> projectId
      )

      credentials match
        case Some(creds) => base + ("gcs.oauth2.token" -> creds)
        case None        => base

  /**
   * Azure Blob Storage backend (ADLS Gen2).
   *
   * Uses org.apache.iceberg.azure.adlsv2.ADLSFileIO (requires iceberg-azure dependency).
   *
   * Authentication options:
   * 1. Connection string (connectionString)
   * 2. Managed Identity (leave connectionString as None)
   *
   * @param accountName Azure storage account name
   * @param connectionString Optional connection string (None = use Managed Identity)
   */
  final case class Azure(
    accountName: String,
    connectionString: Option[String] = None
  ) extends StorageBackend:

    def toIcebergProperties: Map[String, String] =
      val base = Map(
        "io-impl"           -> "org.apache.iceberg.azure.adlsv2.ADLSFileIO",
        "adls.account-name" -> accountName
      )

      connectionString match
        case Some(connStr) => base + ("adls.connection-string" -> connStr)
        case None          => base

  /**
   * Local filesystem storage backend.
   *
   * Uses default file:// implementation.
   * ONLY for testing - never use in production.
   *
   * @param basePath Base directory path (will be used as warehouse root)
   */
  final case class Local(
    basePath: String
  ) extends StorageBackend:

    def toIcebergProperties: Map[String, String] = Map.empty
