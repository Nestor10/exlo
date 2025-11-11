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
   * @return
   *   Either validation error or valid StorageConfig
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
   * Load storage config from ConfigProvider with full validation.
   *
   * Validation happens in three stages:
   * 1. Config parsing - zio-config validates structure and types (in
   *    StorageConfigData.config)
   * 2. Cross-field validation - mapOrFail checks catalog/backend
   *    compatibility
   * 3. Domain validation - StorageConfig.validate checks path schemes
   *
   * Fails fast if:
   * - Required config missing (EXLO_STORAGE_WAREHOUSE_PATH, etc.)
   * - Invalid combinations (Glue + non-S3 backend)
   * - Domain rules violated (incompatible catalog/storage pairs)
   *
   * Usage:
   * {{{
   * for
   *   storageConfig <- StorageConfig.load
   *   // Use storageConfig...
   * yield ()
   * }}}
   */
  def load: ZIO[Any, Config.Error, StorageConfig] =
    ZIO
      .config(exlo.config.StorageConfigData.config)
      .flatMap(data =>
        ZIO
          .fromEither(toDomainValidated(data))
          .mapError(err => Config.Error.InvalidData(message = err))
      )

  /** Convert config to domain with full validation. */
  private def toDomainValidated(
    data: exlo.config.StorageConfigData
  ): Either[String, StorageConfig] =
    for
      backend <- convertBackend(data.backend)
      catalog <- convertCatalog(data.catalog)

      config = StorageConfig(
        warehousePath = data.warehousePath,
        storage = backend,
        catalog = catalog
      )

      // Apply domain validation rules
      _ <- config.validate
      _ <- config.validateCatalogStorage
    yield config

  private def convertBackend(
    backend: exlo.config.StorageBackendConfig
  ): Either[String, StorageBackend] =
    import exlo.config.StorageBackendConfig as SBC
    backend match
      case s: SBC.S3    =>
        Right(
          StorageBackend.S3(
            region = s.region,
            endpoint = s.endpoint,
            accessKeyId = s.accessKeyId,
            secretAccessKey = s.secretAccessKey,
            pathStyleAccess = s.pathStyleAccess
          )
        )
      case g: SBC.Gcs   =>
        Right(
          StorageBackend.Gcs(
            projectId = g.projectId,
            credentials = g.credentials
          )
        )
      case a: SBC.Azure =>
        Right(
          StorageBackend.Azure(
            accountName = a.accountName,
            connectionString = a.connectionString
          )
        )
      case l: SBC.Local =>
        Right(StorageBackend.Local(basePath = l.basePath))

  private def convertCatalog(
    catalog: exlo.config.CatalogConfigData
  ): Either[String, CatalogConfig] =
    import exlo.config.CatalogConfigData as CCD
    catalog match
      case n: CCD.Nessie     =>
        Right(
          CatalogConfig.Nessie(
            uri = n.uri,
            branch = n.branch,
            authToken = n.authToken
          )
        )
      case g: CCD.Glue       =>
        Right(
          CatalogConfig.Glue(
            region = g.region,
            catalogId = g.catalogId
          )
        )
      case h: CCD.Hive       =>
        Right(CatalogConfig.Hive(uri = h.uri))
      case j: CCD.Jdbc       =>
        Right(
          CatalogConfig.Jdbc(
            uri = j.uri,
            username = j.username,
            password = j.password
          )
        )
      case d: CCD.Databricks =>
        Right(
          CatalogConfig.Databricks(
            catalogName = d.catalogName,
            workspace = d.workspace
          )
        )
