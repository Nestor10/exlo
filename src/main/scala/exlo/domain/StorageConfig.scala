package exlo.domain

import zio.*

/**
 * Discriminator for catalog type selection.
 *
 * Users specify exactly one catalog type in their configuration.
 * EXLO automatically wires up the correct Iceberg catalog implementation.
 */
enum CatalogType:
  case Nessie, Glue, Hive, JDBC, Databricks

/**
 * Nessie catalog configuration.
 *
 * Nessie is the CATALOG (metadata layer) - it stores table schemas, snapshots, and partitions.
 * The actual DATA FILES (Parquet) are stored wherever you configure via the FileIO implementation.
 *
 * Nessie supports ANY storage backend that Iceberg supports:
 * - AWS S3 (via S3FileIO)
 * - Google Cloud Storage (via GcsFileIO)
 * - Azure Blob Storage (via AzureFileIO)
 * - HDFS (via HadoopFileIO)
 * - Local filesystem (for testing)
 *
 * Configuration is done via the `properties` map. Iceberg auto-detects FileIO based on:
 * 1. The warehouse path scheme (s3://, gs://, wasb://, hdfs://, file://)
 * 2. The "io-impl" property if explicitly set
 * 3. Available FileIO implementations on the classpath
 *
 * @param uri Nessie server URI (e.g., "http://nessie:19120/api/v1")
 * @param defaultBranch Git-like branch name (default: "main")
 * @param authToken Optional bearer token for OIDC auth
 * @param properties FileIO and cloud credentials configuration
 *
 * Example configurations:
 *
 * AWS S3 with S3FileIO (recommended for S3):
 * ```scala
 * properties = Map(
 *   "io-impl" -> "org.apache.iceberg.aws.s3.S3FileIO",
 *   "s3.endpoint" -> "https://s3.us-east-1.amazonaws.com",
 *   "s3.access-key-id" -> "...",     // Or use IAM roles (preferred)
 *   "s3.secret-access-key" -> "...", // Or use IAM roles (preferred)
 *   "client.region" -> "us-east-1"
 * )
 * ```
 *
 * Google Cloud Storage with GcsFileIO:
 * ```scala
 * properties = Map(
 *   "io-impl" -> "org.apache.iceberg.gcp.gcs.GCSFileIO",
 *   "gcs.project-id" -> "my-gcp-project",
 *   "gcs.oauth2.token" -> "..."  // Or use ADC (Application Default Credentials)
 * )
 * ```
 *
 * Azure Blob Storage with AzureFileIO:
 * ```scala
 * properties = Map(
 *   "io-impl" -> "org.apache.iceberg.azure.adlsv2.ADLSFileIO",
 *   "adls.connection-string" -> "...",
 *   "adls.account-name" -> "..."
 * )
 * ```
 *
 * Dependencies required:
 * - S3: iceberg-aws
 * - GCS: iceberg-gcp
 * - Azure: iceberg-azure
 * - HDFS: hadoop-common, hadoop-hdfs
 */
case class NessieConfig(
  uri: String,
  defaultBranch: String = "main",
  authToken: Option[String] = None,
  properties: Map[String, String] = Map.empty
)

/**
 * AWS Glue catalog configuration.
 *
 * AWS Glue Data Catalog is an AWS-managed metadata service.
 * Unlike Nessie (which supports any storage backend), Glue is AWS-specific.
 *
 * Supported storage backends:
 * - AWS S3 (primary use case)
 * - AWS Lake Formation (S3 with governance)
 *
 * NOT supported:
 * - Google Cloud Storage
 * - Azure Blob Storage
 * - HDFS
 *
 * Glue catalogs typically use IAM roles/policies for S3 access (preferred over static credentials).
 * The warehouse path MUST be an S3 URI (s3://bucket/path).
 *
 * @param region AWS region where Glue catalog resides (e.g., "us-east-1")
 * @param catalogId Optional Glue catalog ID (defaults to AWS account ID)
 * @param properties Additional catalog properties (e.g., S3 endpoint overrides for testing)
 *
 * Example:
 * ```scala
 * GlueConfig(
 *   region = "us-east-1",
 *   catalogId = Some("123456789012"),
 *   properties = Map(
 *     "io-impl" -> "org.apache.iceberg.aws.s3.S3FileIO",
 *     // IAM role credentials automatically discovered via AWS SDK
 *     // Or override with explicit credentials (not recommended):
 *     // "s3.access-key-id" -> "...",
 *     // "s3.secret-access-key" -> "..."
 *   )
 * )
 * ```
 *
 * Dependencies required: iceberg-aws
 */
case class GlueConfig(
  region: String,
  catalogId: Option[String] = None,
  properties: Map[String, String] = Map.empty
)

/**
 * Hive Metastore catalog configuration.
 *
 * Apache Hive Metastore is a widely-used, open-source metadata service.
 * Like Nessie, it supports ANY storage backend that Iceberg supports.
 *
 * Supported storage backends:
 * - AWS S3 (via S3FileIO or HadoopFileIO with S3A)
 * - Google Cloud Storage (via GcsFileIO)
 * - Azure Blob Storage (via ADLSFileIO)
 * - HDFS (via HadoopFileIO)
 * - Local filesystem (testing)
 *
 * Note: Hive Metastore does NOT provide Git-like versioning or ACID guarantees
 * on metadata operations like Nessie does. Use Nessie for production cloud deployments.
 *
 * @param uri Thrift metastore URI (e.g., "thrift://metastore:9083")
 * @param properties FileIO and storage credentials (same as NessieConfig.properties)
 *
 * Example with S3:
 * ```scala
 * HiveConfig(
 *   uri = "thrift://hive-metastore:9083",
 *   properties = Map(
 *     "io-impl" -> "org.apache.iceberg.aws.s3.S3FileIO",
 *     "s3.access-key-id" -> "...",
 *     "s3.secret-access-key" -> "..."
 *   )
 * )
 * ```
 */
case class HiveConfig(
  uri: String,
  properties: Map[String, String] = Map.empty
)

/**
 * JDBC catalog configuration.
 *
 * JDBC catalog stores Iceberg metadata in a relational database (Postgres, MySQL, etc.).
 * Like Nessie and Hive, it supports ANY storage backend for data files.
 *
 * Supported storage backends:
 * - AWS S3 (via S3FileIO)
 * - Google Cloud Storage (via GcsFileIO)
 * - Azure Blob Storage (via ADLSFileIO)
 * - HDFS (via HadoopFileIO)
 * - Local filesystem (testing)
 *
 * The JDBC database only stores metadata - data files go to the configured storage.
 *
 * @param uri JDBC connection string (e.g., "jdbc:postgresql://db:5432/iceberg")
 * @param username Database username
 * @param password Database password
 * @param properties FileIO and storage credentials (same as NessieConfig.properties)
 *
 * Example with S3:
 * ```scala
 * JdbcConfig(
 *   uri = "jdbc:postgresql://postgres:5432/iceberg_catalog",
 *   username = "iceberg",
 *   password = "secret",
 *   properties = Map(
 *     "io-impl" -> "org.apache.iceberg.aws.s3.S3FileIO",
 *     "s3.access-key-id" -> "...",
 *     "s3.secret-access-key" -> "..."
 *   )
 * )
 * ```
 */
case class JdbcConfig(
  uri: String,
  username: String,
  password: String,
  properties: Map[String, String] = Map.empty
)

/**
 * Databricks Unity Catalog configuration.
 *
 * @param uri Databricks workspace URI
 * @param token Access token
 * @param warehouseId SQL warehouse ID
 * @param properties Additional catalog properties
 */
case class DatabricksConfig(
  uri: String,
  token: String,
  warehouseId: String,
  properties: Map[String, String] = Map.empty
)

/**
 * Configuration for Iceberg storage backend.
 *
 * Exactly one catalog config must be provided. EXLO uses pattern matching
 * to wire up the correct catalog implementation at runtime.
 *
 * @param warehousePath Base path for Iceberg warehouse (e.g., "s3://bucket/warehouse")
 * @param nessie Optional Nessie configuration
 * @param glue Optional Glue configuration
 * @param hive Optional Hive configuration
 * @param jdbc Optional JDBC configuration
 * @param databricks Optional Databricks configuration
 */
case class StorageConfig(
  warehousePath: String,
  nessie: Option[NessieConfig] = None,
  glue: Option[GlueConfig] = None,
  hive: Option[HiveConfig] = None,
  jdbc: Option[JdbcConfig] = None,
  databricks: Option[DatabricksConfig] = None
):

  /**
   * Determine which catalog type is configured.
   *
   * Validates that exactly one catalog is specified.
   *
   * @return Catalog type and its configuration
   * @throws IllegalArgumentException if zero or multiple catalogs configured
   */
  def catalogType: Either[String, CatalogType] =
    val configured = List(
      nessie.map(_ => CatalogType.Nessie),
      glue.map(_ => CatalogType.Glue),
      hive.map(_ => CatalogType.Hive),
      jdbc.map(_ => CatalogType.JDBC),
      databricks.map(_ => CatalogType.Databricks)
    ).flatten

    configured match
      case single :: Nil => Right(single)
      case Nil => Left("No catalog configured - must specify exactly one of: nessie, glue, hive, jdbc, or databricks")
      case multiple => Left(s"Multiple catalogs configured: ${multiple.mkString(", ")} - must specify exactly one")

object StorageConfig:

  /**
   * Layer for loading StorageConfig from configuration.
   *
   * TODO: Implement with zio-config in Phase 3
   */
  val layer: ZLayer[Any, Throwable, StorageConfig] =
    ZLayer.succeed(
      StorageConfig(
        warehousePath = "/tmp/warehouse",
        nessie = Some(
          NessieConfig(
            uri = "memory://test",
            defaultBranch = "main"
          )
        )
      )
    )
