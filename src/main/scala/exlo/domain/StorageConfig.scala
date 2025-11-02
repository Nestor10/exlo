package exlo.domain

import zio.*

/**
 * Configuration for Iceberg storage and catalog.
 *
 * Cleanly separates two orthogonal concerns:
 * 1. Storage backend = WHERE data files (Parquet) are stored
 * 2. Catalog = WHERE metadata (schemas, snapshots) is stored
 *
 * This design provides:
 * - Type safety: Compiler catches invalid storage/catalog combinations
 * - Reusability: Same S3 storage works with Nessie, JDBC, or Hive catalog
 * - Clarity: Explicit separation of storage vs catalog concerns
 * - Validation: Can enforce constraints (e.g., Glue requires S3)
 *
 * @param warehousePath Base URI for Iceberg warehouse
 *                      - Must match storage backend scheme (s3://, gs://, wasb://, file://)
 *                      - Example: "s3://my-bucket/warehouse", "gs://my-bucket/iceberg"
 * @param storage Storage backend configuration (WHERE data files go)
 * @param catalog Catalog configuration (WHERE metadata is stored)
 */
final case class StorageConfig(
  warehousePath: String,
  storage: StorageBackend,
  catalog: CatalogConfig
):

  /**
   * Validate that warehouse path scheme matches storage backend.
   *
   * Examples:
   * - s3://bucket/path requires S3 storage
   * - gs://bucket/path requires GCS storage
   * - wasb://container@account.blob.core.windows.net/path requires Azure storage
   */
  def validate: Either[String, Unit] =
    val scheme = warehousePath.split("://").headOption.getOrElse("")

    val expectedScheme = storage match
      case _: StorageBackend.S3    => "s3"
      case _: StorageBackend.Gcs   => "gs"
      case _: StorageBackend.Azure => "wasb" // or "abfs" for ADLS Gen2
      case _: StorageBackend.Local => "file"

    if scheme == expectedScheme then Right(())
    else
      Left(
        s"Warehouse path scheme '$scheme://' doesn't match storage backend '$expectedScheme://'. " +
          s"Check that warehousePath and storage configuration are compatible."
      )

  /**
   * Validate that catalog supports the storage backend.
   *
   * Constraint: AWS Glue catalog ONLY works with S3 storage.
   * All other catalogs (Nessie, Hive, JDBC, Databricks) support any storage.
   */
  def validateCatalogStorage: Either[String, Unit] =
    (catalog, storage) match
      case (_: CatalogConfig.Glue, _: StorageBackend.S3) =>
        Right(()) // Glue + S3 = OK
      case (glue: CatalogConfig.Glue, other) =>
        Left(
          s"AWS Glue catalog only supports S3 storage, but ${other.getClass.getSimpleName} was configured. " +
            "Use Nessie, Hive, or JDBC catalog for non-S3 storage."
        )
      case _                                 =>
        Right(()) // All other catalog/storage combinations are valid

  /**
   * Get merged Iceberg properties for catalog initialization.
   *
   * Combines storage backend properties with warehouse path.
   */
  def icebergProperties: Map[String, String] =
    storage.toIcebergProperties + ("warehouse" -> warehousePath)

object StorageConfig:

  /**
   * Smart constructor that validates configuration.
   *
   * @return Either validation error or valid StorageConfig
   */
  def validated(
    warehousePath: String,
    storage: StorageBackend,
    catalog: CatalogConfig
  ): Either[String, StorageConfig] =
    val config = StorageConfig(warehousePath, storage, catalog)
    for
      _ <- config.validate
      _ <- config.validateCatalogStorage
    yield config

  /**
   * Layer for loading StorageConfig from configuration.
   *
   * TODO: Implement with zio-config in Phase 4 (Kubernetes Integration)
   * For now, this is a placeholder that provides a test configuration.
   */
  val layer: ZLayer[Any, Throwable, StorageConfig] =
    ZLayer.succeed(
      StorageConfig(
        warehousePath = "file:///tmp/warehouse",
        storage = StorageBackend.Local("/tmp/warehouse"),
        catalog = CatalogConfig.Nessie(
          uri = "memory://test",
          branch = "main"
        )
      )
    )
