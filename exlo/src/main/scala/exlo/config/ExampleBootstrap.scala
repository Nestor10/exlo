package exlo.config

import zio.*
import zio.config.typesafe.TypesafeConfigProvider

/**
 * Example application bootstrap showing ExloConfigProvider usage.
 *
 * This demonstrates the recommended pattern for production deployments using
 * environment variables with proper SCREAMING_SNAKE_CASE naming.
 */
object ExampleBootstrap:

  /**
   * Production bootstrap with environment variables.
   *
   * Environment variables expected:
   *   - EXLO_STORAGE_WAREHOUSE_PATH=s3://bucket/warehouse
   *   - EXLO_STORAGE_BACKEND_S3_REGION=us-east-1
   *   - EXLO_STORAGE_CATALOG_NESSIE_URI=http://nessie:19120/api/v1
   *   - EXLO_STREAM_NAMESPACE=raw
   *   - EXLO_STREAM_TABLE_NAME=orders
   */
  val productionBootstrap: ZLayer[Any, Nothing, Unit] =
    Runtime.setConfigProvider(ExloConfigProvider.envProvider)

  /**
   * Development bootstrap with HOCON fallback.
   *
   * Tries environment variables first (using SCREAMING_SNAKE_CASE), falls
   * back to src/main/resources/application.conf (using camelCase).
   */
  val developmentBootstrap: ZLayer[Any, Nothing, Unit] =
    Runtime.setConfigProvider(
      ExloConfigProvider.envProvider.orElse(
        TypesafeConfigProvider.fromResourcePath()
      )
    )

  /**
   * Testing bootstrap with explicit config map.
   *
   * Useful for integration tests that need specific config values.
   */
  def testBootstrap(config: Map[String, String]): ZLayer[Any, Nothing, Unit] =
    Runtime.setConfigProvider(
      ExloConfigProvider.fromMap(config)
    )

/** Example application using the bootstrap. */
object ExampleApp extends ZIOAppDefault:

  override val bootstrap = ExampleBootstrap.developmentBootstrap

  def run =
    for
      // Config is pulled independently by each service - fails fast on missing values
      streamConfig  <- ZIO.config(StreamConfig.config)
      storageConfig <- ZIO.config(StorageConfigData.config)
      _             <- Console.printLine(s"Stream: ${streamConfig.namespace}.${streamConfig.tableName}")
      _             <- Console.printLine(s"Warehouse: ${storageConfig.warehousePath}")
      _             <- Console.printLine(s"Backend: ${storageConfig.backend}")
      _             <- Console.printLine(s"Catalog: ${storageConfig.catalog}")
    yield ()
