package exlo.domain

import zio.*

/**
 * Configuration for Iceberg storage backend.
 *
 * @param catalogUri
 *   URI for Iceberg catalog (e.g., "jdbc:postgresql://localhost:5432/iceberg")
 * @param warehousePath
 *   Base path for Iceberg warehouse (e.g., "s3://bucket/warehouse")
 * @param namespace
 *   Default namespace for tables (e.g., "production")
 */
case class StorageConfig(
  catalogUri: String,
  warehousePath: String,
  namespace: String
)

object StorageConfig:

  /**
   * Layer for loading StorageConfig from configuration.
   *
   * TODO: Implement with zio-config in Phase 3
   */
  val layer: ZLayer[Any, Throwable, StorageConfig] =
    ZLayer.succeed(
      StorageConfig(
        catalogUri = "memory://test",
        warehousePath = "/tmp/warehouse",
        namespace = "default"
      )
    )
