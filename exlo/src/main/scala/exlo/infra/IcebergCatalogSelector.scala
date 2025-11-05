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
 *
 * The layer pulls config directly to keep coupling low.
 */
object IcebergCatalogSelector:

  import exlo.config.StreamConfig

  /**
   * Create a ZLayer that dynamically selects the catalog implementation based on StorageConfig.
   *
   * Pulls both StorageConfig and StreamConfig from the config provider.
   *
   * @return ZLayer that provides IcebergCatalog
   */
  val layer: ZLayer[StorageConfig, Throwable, IcebergCatalog] =
    ZLayer.fromZIO {
      for {
        storageConfig <- ZIO.service[StorageConfig]
        streamConfig <- ZIO.config(StreamConfig.config)

        catalog <- storageConfig.catalog match {
          case nessie: CatalogConfig.Nessie =>
            NessieCatalogLive.make(streamConfig.namespace, streamConfig.tableName, storageConfig.warehousePath, storageConfig.storage, nessie)

          case glue: CatalogConfig.Glue =>
            GlueCatalogLive.make(streamConfig.namespace, streamConfig.tableName, storageConfig.warehousePath, storageConfig.storage, glue)

          case hive: CatalogConfig.Hive =>
            HiveCatalogLive.make(streamConfig.namespace, streamConfig.tableName, storageConfig.warehousePath, storageConfig.storage, hive)

          case jdbc: CatalogConfig.Jdbc =>
            JdbcCatalogLive.make(streamConfig.namespace, streamConfig.tableName, storageConfig.warehousePath, storageConfig.storage, jdbc)

          case databricks: CatalogConfig.Databricks =>
            DatabricksCatalogLive.make(streamConfig.namespace, streamConfig.tableName, storageConfig.warehousePath, storageConfig.storage, databricks)
        }
      } yield catalog
    }
