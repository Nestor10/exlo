package exlo.service

import exlo.domain.DataFile
import exlo.domain.ExloError
import exlo.domain.ExloRecord
import exlo.domain.PipelineElement
import exlo.domain.StreamElement
import exlo.domain.SyncMetadata
import zio.*
import zio.stream.*

import java.util.UUID

/**
 * Service for writing records to Parquet files in cloud storage.
 *
 * Implements the file-based MVP architecture:
 * - Batches records with groupedWithin (memory-bounded)
 * - Wraps user records with ExloRecord metadata
 * - Writes Parquet files immediately to cloud storage
 * - Returns DataFile metadata (path, recordCount, sizeBytes)
 *
 * This prevents OOM by writing files immediately instead of accumulating
 * records in memory until checkpoint.
 */
trait RecordWriter:

  /**
   * Transform user stream elements into file writes.
   *
   * Pipeline that:
   * 1. Batches Data elements with groupedWithin (maxRecords, maxDuration)
   * 2. Wraps records with ExloRecord metadata
   * 3. Writes Parquet files to cloud storage
   * 4. Emits DataFileWritten with file metadata
   * 5. Passes through StateCheckpoint unchanged
   *
   * @param syncMetadata
   *   Sync execution metadata (syncId, startTime)
   * @param connectorId
   *   Connector identifier
   * @param connectorVersion
   *   Connector version
   * @param maxRecordsPerFile
   *   Maximum records per Parquet file
   * @param maxDuration
   *   Maximum time to wait before flushing partial file
   * @return
   *   Pipeline transforming StreamElements to PipelineElements
   */
  def writeFiles(
    syncMetadata: SyncMetadata,
    connectorId: String,
    connectorVersion: String,
    maxRecordsPerFile: Int,
    maxDuration: Duration
  ): ZPipeline[Any, ExloError, StreamElement, PipelineElement]

object RecordWriter:

  /**
   * Accessor method for writeFiles pipeline.
   *
   * Use: `stream.via(RecordWriter.writeFiles(...))`
   */
  def writeFiles(
    syncMetadata: SyncMetadata,
    connectorId: String,
    connectorVersion: String,
    maxRecordsPerFile: Int,
    maxDuration: Duration
  ): ZPipeline[RecordWriter, ExloError, StreamElement, PipelineElement] =
    ZPipeline.fromChannel(
      ZChannel.service[RecordWriter].flatMap { writer =>
        writer.writeFiles(syncMetadata, connectorId, connectorVersion, maxRecordsPerFile, maxDuration).channel
      }
    )

  /**
   * Live implementation - writes real Parquet files to cloud storage.
   *
   * Dependencies:
   * - Cloud storage client (S3/GCS/Azure)
   * - Iceberg writer APIs
   * - StorageConfig for bucket/path configuration
   *
   * TODO: Implement in Phase 2
   */
  case class Live() extends RecordWriter:

    def writeFiles(
      syncMetadata: SyncMetadata,
      connectorId: String,
      connectorVersion: String,
      maxRecordsPerFile: Int,
      maxDuration: Duration
    ): ZPipeline[Any, ExloError, StreamElement, PipelineElement] =
      ZPipeline.mapZIO(_ =>
        ZIO.fail(
          ExloError.IcebergWriteError(
            new NotImplementedError("Live implementation pending Phase 2")
          )
        )
      )

  val live: ZLayer[Any, Nothing, RecordWriter] =
    ZLayer.succeed(Live())

  /**
   * Stub implementation for testing - simulates file writes in memory.
   *
   * Batches records and creates mock DataFile metadata without actually
   * writing to cloud storage. Useful for testing pipeline logic.
   */
  case class Stub(var writtenFiles: Chunk[DataFile] = Chunk.empty) extends RecordWriter:

    def writeFiles(
      syncMetadata: SyncMetadata,
      connectorId: String,
      connectorVersion: String,
      maxRecordsPerFile: Int,
      maxDuration: Duration
    ): ZPipeline[Any, ExloError, StreamElement, PipelineElement] =
      ZPipeline.mapChunks { chunk =>
        chunk
          .foldLeft((Chunk.empty[StreamElement.Data], Chunk.empty[PipelineElement])) {
            case ((accumulated, output), element) =>
              element match
                case data @ StreamElement.Data(_) =>
                  val newAccumulated = accumulated :+ data
                  // Flush if we hit max records
                  if newAccumulated.size >= maxRecordsPerFile then
                    val records = newAccumulated.map(_.record)
                    val file    = DataFile(
                      path = s"s3://test-bucket/data/file-${UUID.randomUUID()}.parquet",
                      recordCount = records.size.toLong,
                      sizeBytes = records.map(_.length).sum.toLong * 2 // Rough estimate
                    )
                    writtenFiles = writtenFiles :+ file
                    (Chunk.empty, output :+ PipelineElement.DataFileWritten(file))
                  else (newAccumulated, output)

                case StreamElement.Checkpoint(state) =>
                  // Flush any accumulated records before checkpoint
                  val output2 = if accumulated.nonEmpty then
                    val records = accumulated.map(_.record)
                    val file    = DataFile(
                      path = s"s3://test-bucket/data/file-${UUID.randomUUID()}.parquet",
                      recordCount = records.size.toLong,
                      sizeBytes = records.map(_.length).sum.toLong * 2
                    )
                    writtenFiles = writtenFiles :+ file
                    output :+ PipelineElement.DataFileWritten(file)
                  else output

                  (Chunk.empty, output2 :+ PipelineElement.StateCheckpoint(state))
          }
          ._2
      }

  object Stub:

    val layer: ZLayer[Any, Nothing, RecordWriter] =
      ZLayer.succeed(Stub())
