package exlo.infra

import exlo.domain.*
import exlo.infra.catalog.*
import zio.*

/**
 * Catalog selector for dynamic IcebergCatalog layer creation.
 *
 * Implements the Zionomicon Ch 17-18 pattern for service composition: - Dynamically selects
 * CatalogOps based on catalog type (Nessie, Glue, Hive, JDBC, Databricks) - Combines with shared
 * IcebergWriter layer - Composes both into IcebergCatalog.Live
 *
 * This eliminates code duplication - all catalog types share the same ~80% Iceberg SDK code
 * (Parquet writing, transactions) via IcebergWriter. Only catalog-specific metadata operations
 * vary.
 *
 * Architecture: StorageConfig → CatalogOps (varies) ++ IcebergWriter (shared) → IcebergCatalog
 * (composed)
 */
object IcebergCatalogSelector:

  import exlo.config.StreamConfig

  /**
   * Create a ZLayer that dynamically selects the catalog implementation based on StorageConfig.
   *
   * Pulls both StorageConfig and StreamConfig from the config provider.
   *
   * @return
   *   ZLayer that provides IcebergCatalog
   */
  val layer: ZLayer[StorageConfig, Throwable, IcebergCatalog] =
    ZLayer.makeSome[StorageConfig, IcebergCatalog](
      // Shared IcebergWriter layer (used by all catalog types)
      IcebergWriter.Live.layer,
      // Dynamic CatalogOps selection based on catalog type
      catalogOpsLayer,
      // Compose CatalogOps + IcebergWriter → IcebergCatalog
      IcebergCatalog.Live.layer
    )

  /**
   * Select the appropriate CatalogOps layer based on catalog type.
   *
   * Pattern matches on CatalogConfig to delegate to the correct catalog implementation. Each
   * catalog has its own CatalogOps implementation (NessieCatalogOps, GlueCatalogOps, etc.).
   */
  private val catalogOpsLayer: ZLayer[StorageConfig, Throwable, CatalogOps] =
    ZLayer.scoped {
      for {
        storageConfig <- ZIO.service[StorageConfig]

        catalogOps <- storageConfig.catalog match {
          case nessie: CatalogConfig.Nessie =>
            ZIO
              .service[CatalogOps]
              .provideLayer(
                NessieCatalogOps.layer(storageConfig.warehousePath, storageConfig.storage, nessie)
              )

          case glue: CatalogConfig.Glue =>
            ZIO.dieMessage("GlueCatalogOps not yet implemented")

          case hive: CatalogConfig.Hive =>
            ZIO.dieMessage("HiveCatalogOps not yet implemented")

          case jdbc: CatalogConfig.Jdbc =>
            ZIO.dieMessage("JdbcCatalogOps not yet implemented")

          case databricks: CatalogConfig.Databricks =>
            ZIO.dieMessage("DatabricksCatalogOps not yet implemented")
        }
      } yield catalogOps
    }
