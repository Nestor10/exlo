package exlo.pipeline

import exlo.domain.{ExloRecord, SyncMetadata, StateConfig}
import zio.*
import zio.stream.*

import java.time.Instant
import java.util.UUID

/** Wraps user record strings with ExloRecord metadata.
  *
  * Takes (records, checkpointState) batches from groupByCheckpoint and converts each user string
  * record into a complete ExloRecord with framework metadata:
  * - commitId: unique identifier for this batch commit
  * - syncId: shared identifier for the entire sync run
  * - timestamps: ingestion time
  * - stateVersion: from StreamConfig for invalidation tracking
  * - configHashes: for detecting config changes
  * - payload: the user's original string record
  *
  * The wrapped records are ready to be written to Iceberg by RecordWriter.
  */
object MetadataWrapper:

  /** Wrap user records with framework metadata.
    *
    * @param syncMetadata
    *   Shared sync execution metadata (syncId, startTime)
    * @param connectorId
    *   User's connector identifier (e.g., "shopify.orders")
    * @param connectorVersion
    *   User's connector version string
    * @return
    *   Pipeline that transforms (records, checkpointState) into ExloRecords
    */
  def wrapWithMetadata(
      syncMetadata: SyncMetadata,
      connectorId: String,
      connectorVersion: String
  ): ZPipeline[StateConfig, Nothing, (Chunk[String], String), ExloRecord] =
    ZPipeline.mapChunksZIO { batches =>
      for
        stateConfig <- ZIO.service[StateConfig]
        now <- Clock.instant
        records = batches.flatMap { case (recordStrings, checkpointState) =>
          // Each batch gets a unique commitId (all records in batch share same commitId)
          val commitId = UUID.randomUUID()

          recordStrings.map { payload =>
            ExloRecord(
              commitId = commitId,
              connectorId = connectorId,
              syncId = syncMetadata.syncId,
              committedAt = now, // Will be updated by TableCommitter when actually committed
              recordedAt = now, // Approximation - could be passed from user record if needed
              connectorVersion = connectorVersion,
              connectorConfigHash = "TODO", // Placeholder - implement in Phase 2
              streamConfigHash = "TODO", // Placeholder - implement in Phase 2
              stateVersion = stateConfig.version,
              payload = payload
            )
          }
        }
      yield records
    }

  /** Alternative implementation: wrap batches with metadata but preserve (records, state) structure.
    *
    * This variant emits (Chunk[ExloRecord], state) instead of flattening to individual records. Useful
    * if downstream operations need to maintain batch boundaries with their checkpoint states.
    */
  def wrapBatchWithMetadata(
      syncMetadata: SyncMetadata,
      connectorId: String,
      connectorVersion: String
  ): ZPipeline[StateConfig, Nothing, (Chunk[String], String), (Chunk[ExloRecord], String)] =
    ZPipeline.mapZIO { case (recordStrings, checkpointState) =>
      for
        stateConfig <- ZIO.service[StateConfig]
        now <- Clock.instant
        commitId = UUID.randomUUID() // All records in this batch share the same commitId
        records = recordStrings.map { payload =>
          ExloRecord(
            commitId = commitId,
            connectorId = connectorId,
            syncId = syncMetadata.syncId,
            committedAt = now, // Will be updated by TableCommitter when actually committed
            recordedAt = now, // Approximation - could be passed from user record if needed
            connectorVersion = connectorVersion,
            connectorConfigHash = "TODO", // Placeholder - implement in Phase 2
            streamConfigHash = "TODO", // Placeholder - implement in Phase 2
            stateVersion = stateConfig.version,
            payload = payload
          )
        }
      yield (records, checkpointState)
    }
