package exlo.service

import exlo.domain.DataFile
import exlo.domain.ExloError
import exlo.domain.ExloRecord
import exlo.infra.IcebergCatalog
import zio.*

/**
 * Application service for table operations.
 *
 * Defines business logic for reading state, writing records, and committing transactions.
 * This is an application-level service (business logic), not infrastructure.
 *
 * The Live implementation will depend on infrastructure services
 * (IcebergCatalog from exlo.infra).
 */
trait Table:

  /**
   * Read state from the table's latest snapshot.
   *
   * Handles state version checking - if version doesn't match, returns empty
   * string to trigger fresh start.
   *
   * @param stateVersion
   *   Expected state version
   * @return
   *   State as JSON string, or empty string if version mismatch or no state exists
   */
  def readState(stateVersion: Long): IO[ExloError, String]

  /**
   * Append a batch of records to the table.
   *
   * Records are written to storage and added to a pending transaction.
   * Does NOT commit - call commitWithState when checkpoint arrives.
   *
   * @param records
   *   Batch of ExloRecords to append
   * @return
   *   DataFile metadata for the written batch
   */
  def appendRecords(records: Chunk[ExloRecord]): IO[ExloError, DataFile]

  /**
   * Commit all accumulated records with state metadata atomically.
   *
   * All records appended since last commit become visible atomically,
   * along with the state metadata in snapshot summary.
   *
   * @param state
   *   State as JSON string
   * @param stateVersion
   *   State version for invalidation tracking
   * @return
   *   Effect that succeeds if commit succeeds
   */
  def commitWithState(state: String, stateVersion: Long): IO[ExloError, Unit]

object Table:

  /**
   * Accessor for readState.
   *
   * Use: `Table.readState(stateVersion)`
   */
  def readState(stateVersion: Long): ZIO[Table, ExloError, String] =
    ZIO.serviceWithZIO[Table](_.readState(stateVersion))

  /**
   * Accessor for appendRecords.
   *
   * Use: `Table.appendRecords(records)`
   */
  def appendRecords(records: Chunk[ExloRecord]): ZIO[Table, ExloError, DataFile] =
    ZIO.serviceWithZIO[Table](_.appendRecords(records))

  /**
   * Accessor for commitWithState.
   *
   * Use: `Table.commitWithState(state, stateVersion)`
   */
  def commitWithState(state: String, stateVersion: Long): ZIO[Table, ExloError, Unit] =
    ZIO.serviceWithZIO[Table](_.commitWithState(state, stateVersion))

  /**
   * Live implementation - depends on infrastructure services.
   *
   * Dependencies (via constructor):
   * - IcebergCatalog (from exlo.infra): Manages Iceberg catalog operations and transactions
   *
   * Pure business logic - delegates all infrastructure concerns to IcebergCatalog:
   * - IcebergCatalog handles transaction management internally
   * - IcebergCatalog owns all Iceberg SDK types (no leakage to application layer)
   * - Table.Live focuses solely on "when" to write/commit, not "how"
   *
   * @param namespace Iceberg namespace (pulled from StreamConfig in layer)
   * @param tableName Iceberg table name (pulled from StreamConfig in layer)
   * @param catalog Infrastructure service for Iceberg operations
   */
  case class Live(
    namespace: String,
    tableName: String,
    streamName: String,
    catalog: IcebergCatalog
  ) extends Table:

    def readState(stateVersion: Long): IO[ExloError, String] =
      for {
        // Read snapshot summary from Iceberg
        maybeSummary <- catalog.readSnapshotSummary(namespace, tableName)

        // Validate stream name matches (prevent cross-stream contamination)
        state <- maybeSummary match {
          case Some((storedState, storedVersion, storedStreamName)) =>
            if (storedStreamName != streamName) {
              // Stream name mismatch - different stream wrote to this table!
              ZIO.fail(
                ExloError.ConfigurationError(
                  s"Stream name mismatch: table $namespace.$tableName has state from stream '$storedStreamName' but current execution is for stream '$streamName'. " +
                  s"This prevents accidental cross-stream contamination. Each stream should write to its own table."
                )
              )
            } else if (storedVersion == stateVersion) {
              // Stream name and version match → use stored state
              ZIO.succeed(storedState)
            } else {
              // Stream name matches but version mismatch → trigger fresh start
              ZIO.succeed("")
            }
          case None =>
            // No snapshot → trigger fresh start
            ZIO.succeed("")
        }
      } yield state

    def appendRecords(records: Chunk[ExloRecord]): IO[ExloError, DataFile] =
      // Delegate to infrastructure - it handles both physical write AND transaction staging
      catalog.writeAndStageRecords(namespace, tableName, records)

    def commitWithState(state: String, stateVersion: Long): IO[ExloError, Unit] =
      // Delegate to infrastructure - pass stream name for validation
      catalog.commitTransaction(namespace, tableName, state, stateVersion, streamName)

  object Live:

    import exlo.config.StreamConfig

    /**
     * Create a Live Table layer with dependencies.
     *
     * Follows Zionomicon pattern (Chapter 17-18):
     * - ZLayer automatically gets IcebergCatalog from environment
     * - Pulls namespace and tableName from config directly
     * - Passes them to Live constructor along with catalog
     *
     * @return ZLayer requiring IcebergCatalog and providing Table
     */
    val layer: ZLayer[IcebergCatalog, Throwable, Table] =
      ZLayer {
        for {
          catalog      <- ZIO.service[IcebergCatalog]
          streamConfig <- ZIO.config(StreamConfig.config)
        } yield Live(streamConfig.namespace, streamConfig.tableName, streamConfig.streamName, catalog)
      }

  /**
   * Stub implementation for testing.
   *
   * Simulates table operations in memory without real infrastructure.
   */
  case class Stub(
    var currentState: String = "",
    var currentStateVersion: Long = 0L,
    var writtenFiles: Chunk[DataFile] = Chunk.empty,
    var commitCount: Int = 0,
    var lastState: String = "",
    var lastStateVersion: Long = 0L
  ) extends Table:

    def readState(stateVersion: Long): IO[ExloError, String] =
      ZIO.succeed {
        // Return empty string if version mismatch (triggers fresh start)
        if (currentStateVersion != stateVersion) ""
        else currentState
      }

    def appendRecords(records: Chunk[ExloRecord]): IO[ExloError, DataFile] =
      ZIO.succeed {
        val file = DataFile(
          path = s"s3://test-bucket/data/file-${java.util.UUID.randomUUID()}.parquet",
          recordCount = records.size.toLong,
          sizeBytes = records.map(_.payload.length).sum.toLong * 2
        )
        writtenFiles = writtenFiles :+ file
        file
      }

    def commitWithState(state: String, stateVersion: Long): IO[ExloError, Unit] =
      ZIO.succeed {
        commitCount += 1
        lastState = state
        lastStateVersion = stateVersion
        // Update current state to simulate what would happen in real Iceberg
        currentState = state
        currentStateVersion = stateVersion
      }

  object Stub:

    val layer: ZLayer[Any, Nothing, Table] =
      ZLayer.succeed(Stub())
