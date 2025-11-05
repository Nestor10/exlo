package exlo.domain

/**
 * Catalog configuration - defines WHERE metadata is stored.
 *
 * This is separate from StorageBackend which defines WHERE data files are stored.
 * A catalog can work with any compatible storage backend.
 */
sealed trait CatalogConfig

object CatalogConfig:

  /**
   * Nessie catalog - Git-like version control for Iceberg metadata.
   *
   * Features:
   * - ACID metadata operations with isolation
   * - Git-like branching and tagging
   * - Multi-table transactions
   * - Works with ANY storage backend (S3, GCS, Azure, HDFS, local)
   *
   * Recommended for production cloud deployments.
   *
   * @param uri Nessie server URI (e.g., "http://nessie:19120/api/v1")
   * @param branch Git-like branch name (default: "main")
   * @param authToken Optional bearer token for OIDC authentication
   */
  final case class Nessie(
    uri: String,
    branch: String = "main",
    authToken: Option[String] = None
  ) extends CatalogConfig

  /**
   * AWS Glue Data Catalog - AWS-managed metadata service.
   *
   * Features:
   * - Fully managed (no server to run)
   * - Integrated with AWS ecosystem
   * - IAM-based access control
   * - ONLY works with S3Storage (AWS-specific)
   *
   * Use when you're all-in on AWS and want a managed service.
   *
   * @param region AWS region where Glue catalog resides (e.g., "us-east-1")
   * @param catalogId Optional Glue catalog ID (defaults to AWS account ID)
   */
  final case class Glue(
    region: String,
    catalogId: Option[String] = None
  ) extends CatalogConfig

  /**
   * Apache Hive Metastore catalog - traditional Hadoop metadata service.
   *
   * Features:
   * - Widely supported, mature
   * - Works with ANY storage backend
   * - No ACID metadata operations (unlike Nessie)
   * - Requires running Hive Metastore service
   *
   * Use for legacy Hadoop environments or if you already have Hive Metastore.
   *
   * @param uri Thrift metastore URI (e.g., "thrift://metastore:9083")
   */
  final case class Hive(
    uri: String
  ) extends CatalogConfig

  /**
   * JDBC catalog - stores metadata in a relational database.
   *
   * Features:
   * - Lightweight, no special service required
   * - Works with ANY storage backend
   * - Metadata stored in Postgres, MySQL, SQLite, etc.
   * - Good for simple deployments
   *
   * Use when you want simple metadata management without external services.
   *
   * @param uri JDBC connection string (e.g., "jdbc:postgresql://db:5432/iceberg")
   * @param username Database username
   * @param password Database password
   */
  final case class Jdbc(
    uri: String,
    username: String,
    password: String
  ) extends CatalogConfig

  /**
   * Databricks Unity Catalog - Databricks-managed metadata service.
   *
   * Features:
   * - Integrated with Databricks platform
   * - Built-in governance and lineage
   * - Works with Databricks-managed storage
   *
   * Use when running on Databricks platform.
   *
   * @param catalogName Unity Catalog name
   * @param workspace Databricks workspace URL
   */
  final case class Databricks(
    catalogName: String,
    workspace: String
  ) extends CatalogConfig
