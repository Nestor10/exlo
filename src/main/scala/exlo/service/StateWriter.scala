package exlo.service

import exlo.domain.ExloError
import exlo.domain.ExloRecord
import zio.*

/**
 * Service for writing state atomically with data commits.
 *
 * State is written to Iceberg snapshot summary properties along with the data
 * commit. This ensures exactly-once semantics - state only advances if data
 * write succeeds.
 */
trait StateWriter:

  /**
   * Write records and state atomically to Iceberg.
   *
   * @param namespace
   *   Iceberg namespace
   * @param tableName
   *   Iceberg table name
   * @param stateVersion
   *   State version to write
   * @param records
   *   Data records to commit
   * @param state
   *   State as JSON string to write to snapshot summary
   */
  def writeState(
    namespace: String,
    tableName: String,
    stateVersion: Long,
    records: Chunk[ExloRecord],
    state: String
  ): IO[ExloError, Unit]

object StateWriter:

  /**
   * Accessor method for writeState.
   *
   * Use: `StateWriter.writeState(namespace, tableName, stateVersion, records,
   * state)`
   */
  def writeState(
    namespace: String,
    tableName: String,
    stateVersion: Long,
    records: Chunk[ExloRecord],
    state: String
  ): ZIO[StateWriter, ExloError, Unit] =
    ZIO.serviceWithZIO[StateWriter](
      _.writeState(namespace, tableName, stateVersion, records, state)
    )

  /**
   * Live implementation - writes to Iceberg.
   *
   * TODO: Implement in Phase 2
   */
  case class Live() extends StateWriter:

    def writeState(
      namespace: String,
      tableName: String,
      stateVersion: Long,
      records: Chunk[ExloRecord],
      state: String
    ): IO[ExloError, Unit] =
      ZIO.fail(
        ExloError.IcebergWriteError(
          new NotImplementedError("Live implementation pending Phase 2")
        )
      )

  val live: ZLayer[Any, Nothing, StateWriter] =
    ZLayer.succeed(Live())

  /** Stub implementation for testing - tracks writes in memory. */
  case class Stub(
    var lastState: String = "",
    var recordCount: Int = 0
  ) extends StateWriter:

    def writeState(
      namespace: String,
      tableName: String,
      stateVersion: Long,
      records: Chunk[ExloRecord],
      state: String
    ): IO[ExloError, Unit] =
      ZIO.succeed {
        lastState = state
        recordCount += records.size
      }

  object Stub:

    val layer: ZLayer[Any, Nothing, StateWriter] =
      ZLayer.succeed(Stub())
