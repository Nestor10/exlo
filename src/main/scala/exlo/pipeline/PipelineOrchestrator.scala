package exlo.pipeline

import exlo.domain.ExloError
import exlo.domain.ExloRecord
import exlo.domain.StateConfig
import exlo.domain.SyncMetadata
import exlo.infra.IcebergCatalog
import exlo.service.Connector
import exlo.service.Table
import zio.*
import zio.stream.*

/**
 * Orchestrates the complete EXLO pipeline from state read to commit.
 *
 * This is not a service (no trait abstraction) - it's the framework's
 * main orchestration logic that wires together:
 * - Table (read state, append records, commit)
 * - Connector (extract data)
 * - StreamProcessor (batching and grouping)
 * - IcebergCatalog (table creation)
 *
 * Flow:
 * 1. Read initial state from table (or empty if fresh start)
 * 2. Ensure table exists (create if needed)
 * 3. Extract data from connector starting at state
 * 4. Batch records into ExloRecords with metadata
 * 5. Append batches to table (write Parquet files)
 * 6. Commit atomically when checkpoints arrive
 * 7. Handle graceful shutdown on SIGTERM
 */
object PipelineOrchestrator:

  /**
   * Run the complete EXLO pipeline.
   *
   * @param namespace
   *   Iceberg namespace for the table
   * @param tableName
   *   Iceberg table name
   * @param stateVersion
   *   State version for invalidation (increment to force fresh start)
   * @param syncMetadata
   *   Metadata about this sync run
   * @return
   *   Effect that runs until stream completes or fails
   */
  def run(
    namespace: String,
    tableName: String,
    stateVersion: Long,
    syncMetadata: SyncMetadata
  ): ZIO[Connector & Table & IcebergCatalog & StateConfig, ExloError, Unit] =
    for
      // Services
      table     <- ZIO.service[Table]
      connector <- ZIO.service[Connector]
      catalog   <- ZIO.service[IcebergCatalog]
      config    <- ZIO.service[StateConfig]

      // 1. Ensure table exists before trying to read state or append
      tableExists  <- catalog.tableExists(namespace, tableName)
      _            <- ZIO.unless(tableExists) {
        ZIO.logInfo(s"Table $namespace.$tableName does not exist, creating...") *>
          catalog.createTable(namespace, tableName, customLocation = None)
      }

      // 2. Read initial state (empty string if version mismatch or no state)
      //    Safe to call now that we know table exists
      initialState <- table.readState(stateVersion)
      _ <- ZIO.logInfo(s"Starting pipeline with state: ${if (initialState.isEmpty) "FRESH START" else initialState}")

      // 3. Run the pipeline
      _ <- connector
        .extract(initialState)
        .tap(element => ZIO.logDebug(s"Connector emitted: $element"))
        .via(batchRecordsByCheckpoint(syncMetadata, connector.connectorId, syncMetadata.connectorVersion))
        .mapZIO {
          case (records, checkpointState) =>
            // Append batch to table (writes Parquet, adds to transaction)
            table.appendRecords(records).as(checkpointState)
        }
        .mapZIO { checkpointState =>
          // Commit all accumulated records with checkpoint state atomically
          ZIO.logInfo(s"Checkpoint reached, committing with state: $checkpointState") *>
            table.commitWithState(checkpointState, stateVersion)
        }
        .runDrain

      _ <- ZIO.logInfo("Pipeline completed successfully")
    yield ()

  /**
   * Batch user records by checkpoint and wrap with ExloRecord metadata.
   *
   * Groups Data elements until Checkpoint arrives, wraps the batch with
   * framework metadata, and emits (records, checkpointState) tuples.
   *
   * This preserves the checkpoint state so we can commit it atomically
   * with the records.
   */
  private def batchRecordsByCheckpoint(
    syncMetadata: SyncMetadata,
    connectorId: String,
    connectorVersion: String
  ): ZPipeline[StateConfig, Nothing, exlo.domain.StreamElement, (Chunk[ExloRecord], String)] =
    // First: group records by checkpoint (yields (Chunk[String], state))
    val batchByCheckpoint = ZPipeline
      .mapAccum(Chunk.empty[String]) { (accumulated, element) =>
        element match
          case exlo.domain.StreamElement.Data(record) =>
            // Accumulate record
            (accumulated :+ record, None)

          case exlo.domain.StreamElement.Checkpoint(state) =>
            // Emit accumulated records with checkpoint state, reset
            (Chunk.empty, if (accumulated.nonEmpty) Some((accumulated, state)) else None)
      }
      .collect { case Some(batch) => batch }

    // Then: wrap with metadata (preserves checkpoint state)
    val wrapMetadata = MetadataWrapper.wrapBatchWithMetadata(syncMetadata, connectorId, connectorVersion)

    // Compose: batch then wrap (result is (Chunk[ExloRecord], String))
    batchByCheckpoint >>> wrapMetadata

  /**
   * Group record batches by checkpoint boundaries.
   *
   * Accumulates all record chunks until we hit the next checkpoint signal.
   * This allows us to commit all records written since last checkpoint atomically.
   */
  private def groupByCheckpoints: ZPipeline[Any, Nothing, Chunk[ExloRecord], (Chunk[Chunk[ExloRecord]], String)] =
    // TODO: This needs to track checkpoints properly
    // For now, placeholder that would need checkpoint state from the stream
    ZPipeline.identity.map(chunk => (Chunk(chunk), ""))

  /**
   * Handle graceful shutdown.
   *
   * On SIGTERM:
   * 1. Stop accepting new elements from connector
   * 2. Flush any buffered records via table.appendRecords
   * 3. Commit with last known checkpoint state
   *
   * TODO: Implement in Phase 1 after basic pipeline works
   */
  def shutdown: ZIO[Table, ExloError, Unit] =
    ZIO.logInfo("Graceful shutdown initiated") *>
      ZIO.service[Table].flatMap { table =>
        // Would flush pending records and commit
        ZIO.unit
      }
