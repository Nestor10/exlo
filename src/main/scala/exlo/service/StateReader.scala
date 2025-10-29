package exlo.service

import exlo.domain.ExloError
import zio.*

/**
 * Service for reading state from Iceberg snapshot summaries.
 *
 * Handles state version checking - if version doesn't match, returns empty
 * string to trigger fresh start.
 */
trait StateReader:

  /**
   * Read state from Iceberg snapshot summary.
   *
   * @param namespace
   *   Iceberg namespace
   * @param tableName
   *   Iceberg table name
   * @param stateVersion
   *   Expected state version
   * @return
   *   State as JSON string, or empty string if version mismatch or no state
   *   exists
   */
  def readState(
    namespace: String,
    tableName: String,
    stateVersion: Long
  ): IO[ExloError, String]

object StateReader:

  /**
   * Accessor method for readState.
   *
   * Use: `StateReader.readState(namespace, tableName, stateVersion)`
   */
  def readState(
    namespace: String,
    tableName: String,
    stateVersion: Long
  ): ZIO[StateReader, ExloError, String] =
    ZIO.serviceWithZIO[StateReader](
      _.readState(namespace, tableName, stateVersion)
    )

  /**
   * Live implementation - reads from Iceberg.
   *
   * TODO: Implement in Phase 2
   */
  case class Live() extends StateReader:

    def readState(
      namespace: String,
      tableName: String,
      stateVersion: Long
    ): IO[ExloError, String] =
      ZIO.fail(
        ExloError.StateReadError(
          new NotImplementedError("Live implementation pending Phase 2")
        )
      )

  val live: ZLayer[Any, Nothing, StateReader] =
    ZLayer.succeed(Live())

  /** Stub implementation for testing - returns mock state. */
  case class Stub(mockState: String) extends StateReader:

    def readState(
      namespace: String,
      tableName: String,
      stateVersion: Long
    ): IO[ExloError, String] =
      ZIO.succeed(mockState)

  object Stub:

    val layer: ZLayer[Any, Nothing, StateReader] =
      ZLayer.succeed(Stub(""))

    def withState(state: String): ZLayer[Any, Nothing, StateReader] =
      ZLayer.succeed(Stub(state))
