package exlo.runtime

import exlo.runtime.iceberg.{Catalogs, CatalogConfig, IcebergDestination, StateStore, TableConfig}
import zio.*
import zio.json.JsonCodec

/**
 * Env-driven destination construction. Reads `EXLO_DESTINATION`:
 *   - `logging` (default) — [[LoggingDestination]]; logs each record + commit, no
 *     persistence. Local dev.
 *   - `iceberg` — [[IcebergDestination]] backed by a `Catalog` resolved from
 *     `EXLO_CATALOG_*` env vars (Glue or Hadoop) and a table identified by
 *     `EXLO_TABLE_NAMESPACE` / `EXLO_TABLE_NAME`.
 *
 * The whole chain (catalog → table → destination) is built inside one `Scope` so the
 * catalog's `close()` (especially for Glue's HTTP connection pool) runs at framework
 * shutdown.
 */
object DestinationFactory:

  private val destinationKind: Config[String] =
    Config.string("destination").nested("exlo").withDefault("logging")

  /**
   * Build a `Destination[S]` according to the env. `connectorId` is used for the
   * logging mode's prefix; iceberg mode ignores it (the table identifier supplies
   * identity).
   */
  def layer[S: Tag: JsonCodec](connectorId: String): ZLayer[Any, Throwable, Destination[S]] =
    ZLayer.scoped {
      ZIO.config(destinationKind).flatMap {
        case "logging" =>
          LoggingDestination.make[S](connectorId).asInstanceOf[ZIO[Scope, Throwable, Destination[S]]]
        case "iceberg" =>
          for
            catalogConfig <- CatalogConfig.fromEnv
            tableConfig   <- TableConfig.fromEnv
            catalog       <- Catalogs.make(catalogConfig)
            table         <- Catalogs.loadOrCreateTable(catalog, tableConfig.namespace, tableConfig.name)
            stateStore    = new StateStore.Live(catalog)
            dest          <- IcebergDestination.fromTable[S](table, stateStore)
          yield dest
        case other =>
          ZIO.fail(
            new RuntimeException(
              s"Unknown EXLO_DESTINATION '$other' (expected 'logging' or 'iceberg')"
            )
          )
      }
    }
