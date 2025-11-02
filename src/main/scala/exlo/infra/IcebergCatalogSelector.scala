package exlo.infra

import exlo.domain.*
import exlo.infra.catalog.*
import zio.*

/**
 * Catalog selector for dynamic IcebergCatalog layer creation.
 *
 * Implements the Zionomicon Ch 18 pattern for dynamic service selection:
 * - Uses ZLayer.fromZIO to compute the service at layer construction time
 * - Pattern matches on CatalogType to delegate to the appropriate Live implementation
 * - Each catalog Live implementation is in a separate file for maintainability
 *
 * This pattern keeps catalog selection logic centralized while allowing each
 * catalog implementation to grow independently in its own module.
 */
object IcebergCatalogSelector:

  /**
   * Create a ZLayer that dynamically selects the catalog implementation based on StorageConfig.
   *
   * @param namespace Iceberg namespace for this table
   * @param tableName Iceberg table name
   * @return ZLayer that provides IcebergCatalog for any R that provides StorageConfig
   */
  def layer(
    namespace: String,
    tableName: String
  ): ZLayer[StorageConfig, Throwable, IcebergCatalog] =
    ZLayer.fromZIO {
      for {
        config <- ZIO.service[StorageConfig]

        catalogType <- ZIO
          .fromEither(config.catalogType)
          .mapError(msg => new IllegalStateException(s"Invalid catalog configuration: $msg"))

        catalog <- catalogType match {
          case CatalogType.Nessie =>
            config.nessie match {
              case Some(nessieConfig) =>
                NessieCatalogLive.make(namespace, tableName, config.warehousePath, nessieConfig)
              case None               =>
                ZIO.fail(new IllegalStateException("Nessie catalog type selected but configuration missing"))
            }

          case CatalogType.Glue =>
            config.glue match {
              case Some(glueConfig) =>
                GlueCatalogLive.make(namespace, tableName, config.warehousePath, glueConfig)
              case None             =>
                ZIO.fail(new IllegalStateException("Glue catalog type selected but configuration missing"))
            }

          case CatalogType.Hive =>
            config.hive match {
              case Some(hiveConfig) =>
                HiveCatalogLive.make(namespace, tableName, config.warehousePath, hiveConfig)
              case None             =>
                ZIO.fail(new IllegalStateException("Hive catalog type selected but configuration missing"))
            }

          case CatalogType.JDBC =>
            config.jdbc match {
              case Some(jdbcConfig) =>
                JdbcCatalogLive.make(namespace, tableName, config.warehousePath, jdbcConfig)
              case None             =>
                ZIO.fail(new IllegalStateException("JDBC catalog type selected but configuration missing"))
            }

          case CatalogType.Databricks =>
            config.databricks match {
              case Some(databricksConfig) =>
                DatabricksCatalogLive.make(namespace, tableName, config.warehousePath, databricksConfig)
              case None                   =>
                ZIO.fail(new IllegalStateException("Databricks catalog type selected but configuration missing"))
            }
        }
      } yield catalog
    }
