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

        catalog <- config.catalog match {
          case nessie: CatalogConfig.Nessie =>
            NessieCatalogLive.make(namespace, tableName, config.warehousePath, config.storage, nessie)

          case glue: CatalogConfig.Glue =>
            GlueCatalogLive.make(namespace, tableName, config.warehousePath, config.storage, glue)

          case hive: CatalogConfig.Hive =>
            HiveCatalogLive.make(namespace, tableName, config.warehousePath, config.storage, hive)

          case jdbc: CatalogConfig.Jdbc =>
            JdbcCatalogLive.make(namespace, tableName, config.warehousePath, config.storage, jdbc)

          case databricks: CatalogConfig.Databricks =>
            DatabricksCatalogLive.make(namespace, tableName, config.warehousePath, config.storage, databricks)
        }
      } yield catalog
    }
