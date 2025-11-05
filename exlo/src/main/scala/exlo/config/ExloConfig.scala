package exlo.config

import zio.Config
import zio.config.magnolia.*

/**
 * Stream-specific configuration (table location).
 *
 * @param namespace
 *   Iceberg namespace for this stream's table
 * @param tableName
 *   Iceberg table name for this stream
 */
final case class StreamConfig(
  namespace: String,
  tableName: String
)

object StreamConfig:

  val config: Config[StreamConfig] =
    deriveConfig[StreamConfig].nested("exlo", "stream")

/**
 * Sync execution configuration.
 *
 * @param stateVersion
 *   State version number. Increment to trigger fresh start.
 */
final case class SyncConfig(
  stateVersion: Long
)

object SyncConfig:

  val config: Config[SyncConfig] =
    deriveConfig[SyncConfig].nested("exlo", "sync")

/**
 * Top-level EXLO framework configuration.
 *
 * This is primarily for documentation - services pull config subsets
 * directly. Uses nested structures that preserve domain model type safety.
 */
final case class ExloConfig(
  stream: StreamConfig,
  storage: StorageConfigData,
  sync: SyncConfig
)

object ExloConfig:

  val config: Config[ExloConfig] =
    deriveConfig[ExloConfig].nested("exlo")
