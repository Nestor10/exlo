package exlo.runtime.iceberg

import zio.*

/**
 * Iceberg table identity (namespace + name). Loaded from env:
 *   - `EXLO_TABLE_NAMESPACE`
 *   - `EXLO_TABLE_NAME`
 */
final case class TableConfig(namespace: String, name: String)

object TableConfig:
  val config: Config[TableConfig] =
    (Config.string("namespace") ++ Config.string("name"))
      .nested("table")
      .nested("exlo")
      .map { case (ns, n) => TableConfig(ns, n) }

  def fromEnv: ZIO[Any, Config.Error, TableConfig] = ZIO.config(config)
