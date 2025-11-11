package exlo.config

import zio.Config
import zio.config.magnolia.*

/**
 * Storage backend configuration - sealed trait for type-safe discriminated union.
 *
 * Maps to domain's StorageBackend sealed trait. zio-config's automatic
 * derivation handles sealed traits as discriminated unions.
 */
sealed trait StorageBackendConfig

object StorageBackendConfig:

  final case class S3(
    region: String,
    endpoint: Option[String] = None,
    accessKeyId: Option[String] = None,
    secretAccessKey: Option[String] = None,
    pathStyleAccess: Boolean = false
  ) extends StorageBackendConfig

  final case class Gcs(
    projectId: String,
    credentials: Option[String] = None
  ) extends StorageBackendConfig

  final case class Azure(
    accountName: String,
    connectionString: Option[String] = None
  ) extends StorageBackendConfig

  final case class Local(
    basePath: String
  ) extends StorageBackendConfig

  val config: Config[StorageBackendConfig] =
    deriveConfig[StorageBackendConfig]

/**
 * Catalog configuration - sealed trait for different catalog types.
 *
 * Maps to domain's CatalogConfig sealed trait.
 */
sealed trait CatalogConfigData

object CatalogConfigData:

  final case class Nessie(
    uri: String,
    branch: String = "main",
    authToken: Option[String] = None
  ) extends CatalogConfigData

  final case class Glue(
    region: String,
    catalogId: Option[String] = None
  ) extends CatalogConfigData

  final case class Hive(
    uri: String
  ) extends CatalogConfigData

  final case class Jdbc(
    uri: String,
    username: String,
    password: String
  ) extends CatalogConfigData

  final case class Databricks(
    catalogName: String,
    workspace: String
  ) extends CatalogConfigData

  val config: Config[CatalogConfigData] =
    deriveConfig[CatalogConfigData]

/**
 * Storage configuration combining warehouse path, backend, and catalog.
 *
 * This is the config representation that gets parsed from environment
 * variables or HOCON files. It mirrors the domain StorageConfig but is
 * optimized for parsing.
 */
final case class StorageConfigData(
  warehousePath: String,
  backend: StorageBackendConfig,
  catalog: CatalogConfigData
)

object StorageConfigData:

  private val baseConfig: Config[StorageConfigData] =
    deriveConfig[StorageConfigData].nested("exlo", "storage")

  /**
   * Config with cross-field validation.
   *
   * Validates:
   *   - Catalog/backend compatibility (Glue requires S3, etc.)
   *   - Region matching (Glue region must match S3 region)
   *   - Required credentials based on catalog type
   *   - HTTPS endpoints require auth tokens
   */
  val config: Config[StorageConfigData] =
    baseConfig.mapOrFail { data =>
      for
        _ <- validateCatalogBackendCompatibility(data)
        _ <- validateRequiredCredentials(data)
        _ <- validateRegionConsistency(data)
      yield data
    }

  /**
   * Validate catalog and backend are compatible.
   *
   * Rules:
   *   - AWS Glue → must use S3 backend
   *   - Databricks → must use S3 or Azure backend
   *   - Nessie → works with any backend
   */
  private def validateCatalogBackendCompatibility(
    data: StorageConfigData
  ): Either[Config.Error, Unit] =
    (data.catalog, data.backend) match
      // AWS Glue requires S3
      case (_: CatalogConfigData.Glue, _: StorageBackendConfig.S3) =>
        Right(())

      case (_: CatalogConfigData.Glue, backend) =>
        Left(
          Config.Error.InvalidData(
            message = s"AWS Glue catalog requires S3 backend, got: ${backend.getClass.getSimpleName}"
          )
        )

      // Databricks requires S3 or Azure
      case (
             _: CatalogConfigData.Databricks,
             _: StorageBackendConfig.S3 | _: StorageBackendConfig.Azure
           ) =>
        Right(())

      case (_: CatalogConfigData.Databricks, backend) =>
        Left(
          Config.Error.InvalidData(
            message = s"Databricks catalog requires S3 or Azure backend, got: ${backend.getClass.getSimpleName}"
          )
        )

      // Nessie, Hive, JDBC work with any backend
      case _ => Right(())

  /** Validate required credentials are provided based on catalog type. */
  private def validateRequiredCredentials(
    data: StorageConfigData
  ): Either[Config.Error, Unit] =
    data.catalog match
      // Nessie with HTTPS requires auth token
      case nessie: CatalogConfigData.Nessie if nessie.uri.startsWith("https://") =>
        if nessie.authToken.isDefined then Right(())
        else
          Left(
            Config.Error.InvalidData(
              message = "Nessie HTTPS endpoints require auth-token (set EXLO_STORAGE_CATALOG_NESSIE_AUTH_TOKEN)"
            )
          )

      // JDBC requires username and password
      case jdbc: CatalogConfigData.Jdbc =>
        if jdbc.username.nonEmpty && jdbc.password.nonEmpty then Right(())
        else
          Left(
            Config.Error.InvalidData(
              message = "JDBC catalog requires username and password"
            )
          )

      case _ => Right(())

  /** Validate region consistency between catalog and backend. */
  private def validateRegionConsistency(
    data: StorageConfigData
  ): Either[Config.Error, Unit] =
    (data.catalog, data.backend) match
      // AWS Glue and S3 must be in same region
      case (glue: CatalogConfigData.Glue, s3: StorageBackendConfig.S3) =>
        if glue.region == s3.region then Right(())
        else
          Left(
            Config.Error.InvalidData(
              message = s"AWS Glue region (${glue.region}) must match S3 region (${s3.region})"
            )
          )

      case _ => Right(())
