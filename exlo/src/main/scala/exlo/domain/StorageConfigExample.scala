package exlo.domain

import zio.*

/**
 * Example showing StorageConfig usage with the new config-pull pattern.
 *
 * OLD PATTERN (removed):
 * {{{
 * // Layer-based - config was a dependency
 * def myService = ZLayer {
 *   for
 *     storageConfig <- ZIO.service[StorageConfig]
 *     // Use storageConfig...
 *   yield MyService(storageConfig)
 * }
 *
 * // Usage in app
 * myProgram.provide(StorageConfig.layer, OtherLayer, ...)
 * }}}
 *
 * NEW PATTERN (current):
 * {{{
 * // Services pull config directly - no layer needed
 * def myService =
 *   for
 *     storageConfig <- StorageConfig.load // Pull config on-demand
 *     // Use storageConfig...
 *   yield result
 *
 * // Bootstrap sets ConfigProvider, then services just work
 * object MyApp extends ZIOAppDefault:
 *   override val bootstrap = ExampleBootstrap.developmentBootstrap
 *   def run = myService
 * }}}
 */
object StorageConfigExample:

  /** Example: Initialize Iceberg catalog using loaded config. */
  def initializeCatalog: ZIO[Any, Throwable, String] =
    for
      config <- StorageConfig.load
      properties = config.icebergProperties
      _ <- Console.printLine(s"Initializing catalog: ${config.catalog}")
      _ <- Console.printLine(s"Warehouse: ${config.warehousePath}")
      _ <- Console.printLine(s"Properties: $properties")
    yield s"Catalog initialized at ${config.warehousePath}"

  /** Example: Validate and display configuration. */
  def displayConfig: ZIO[Any, Throwable, Unit] =
    for
      config <- StorageConfig.load
      _      <- Console.printLine("=== Storage Configuration ===")
      _      <- Console.printLine(s"Warehouse: ${config.warehousePath}")
      _      <- Console.printLine(s"Storage: ${config.storage}")
      _      <- Console.printLine(s"Catalog: ${config.catalog}")
      _      <- Console.printLine(
        s"Validation: ${config.validate.merge} | ${config.validateCatalogStorage.merge}"
      )
    yield ()
