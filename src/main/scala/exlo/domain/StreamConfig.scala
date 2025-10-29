package exlo.domain

import zio.*

/**
 * Configuration for stream execution.
 *
 * @param namespace
 *   Iceberg namespace for this stream's table
 * @param tableName
 *   Iceberg table name for this stream
 * @param batchSize
 *   Number of records to accumulate before committing
 */
case class StreamConfig(
  namespace: String,
  tableName: String,
  batchSize: Int
)

object StreamConfig:

  /**
   * Layer for loading StreamConfig from configuration.
   *
   * TODO: Implement with zio-config in Phase 3
   */
  val layer: ZLayer[Any, Throwable, StreamConfig] =
    ZLayer.succeed(
      StreamConfig(
        namespace = "default",
        tableName = "test_stream",
        batchSize = 100
      )
    )
