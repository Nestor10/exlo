package exlo.integration

import exlo.domain.DataFile
import exlo.domain.ExloError
import exlo.domain.PipelineElement
import exlo.domain.StreamElement
import exlo.domain.SyncMetadata
import exlo.pipeline.StreamProcessor
import exlo.service.*
import zio.*
import zio.stream.*
import zio.test.*
import zio.test.Assertion.*

import java.util.UUID

/**
 * Integration tests for the file-based checkpoint grouping pipeline.
 *
 * These tests verify the MVP architecture where:
 * 1. Records are written to Parquet files via RecordWriter.Stub
 * 2. File paths accumulate in memory (tiny footprint)
 * 3. Checkpoints trigger Iceberg commits of accumulated files
 *
 * Key insight: We accumulate file PATHS, not records (memory-efficient).
 */
object StreamProcessingSpec extends ZIOSpecDefault:

  val testSyncMetadata = SyncMetadata(
    syncId = UUID.randomUUID(),
    startedAt = java.time.Instant.now(),
    connectorVersion = "1.0.0-test"
  )

  def spec = suite("Stream Processing Pipeline")(
    test("groups data files between checkpoints") {
      // BEHAVIOR:
      // Stream: Data, Data, Checkpoint, Data, Checkpoint
      // Files written: file1 (2 records), file2 (1 record)
      // Batches: [file1] with state "page_2", [file2] with state "page_3"

      for
        connector    <- ZIO.service[Connector]
        recordWriter <- ZIO.service[RecordWriter]
        batches      <- connector
          .extract("")
          .via(
            recordWriter
              .writeFiles(testSyncMetadata, "test-connector", "1.0.0", maxRecordsPerFile = 10, maxDuration = 1.minute)
          )
          .via(StreamProcessor.groupByCheckpoint)
          .runCollect
      yield assertTrue(
        batches.size == 2,
        batches(0)._1.size == 1,                    // First batch has 1 file (2 records)
        batches(0)._1(0).recordCount == 2,
        batches(0)._2 == """{"cursor":"page_2"}""", // First checkpoint state
        batches(1)._1.size == 1,                    // Second batch has 1 file (1 record)
        batches(1)._1(0).recordCount == 1,
        batches(1)._2 == """{"cursor":"page_3"}"""  // Second checkpoint state
      )
    }.provide(StubConnector.layer, RecordWriter.Stub.layer),
    test("checkpoint advances state with file commits") {
      // BEHAVIOR:
      // Files are accumulated as paths, checkpoint triggers "commit"
      // State advances only after files are logged

      for
        connector    <- ZIO.service[Connector]
        recordWriter <- ZIO.service[RecordWriter]
        stateWriter  <- ZIO.service[StateWriter]

        // Process stream: write files, group by checkpoint, log state
        _ <- connector
          .extract("")
          .via(
            recordWriter
              .writeFiles(testSyncMetadata, "test-connector", "1.0.0", maxRecordsPerFile = 10, maxDuration = 1.minute)
          )
          .via(StreamProcessor.groupByCheckpoint)
          .mapZIO {
            case (files, state) =>
              // In production: Iceberg commit with file paths
              // In test: just log the state
              val totalRecords = files.map(_.recordCount).sum
              StateWriter
                .writeState("test", "table", 1L, Chunk.empty, state)
                .as(totalRecords)
          }
          .runSum

        // Verify final state
        stub = stateWriter.asInstanceOf[StateWriter.Stub]
      yield assertTrue(
        stub.lastState == """{"cursor":"page_3"}""" // Final checkpoint
      )
    }.provide(
      StubConnector.layer,
      RecordWriter.Stub.layer,
      StateWriter.Stub.layer
    ),
    test("empty stream produces no file batches") {
      // Edge case: connector emits no elements
      val emptyConnector = new Connector {
        def connectorId            = "empty"
        def extract(state: String) = ZStream.empty
      }

      for
        recordWriter <- ZIO.service[RecordWriter]
        batches      <- emptyConnector
          .extract("")
          .via(
            recordWriter.writeFiles(testSyncMetadata, "empty", "1.0.0", maxRecordsPerFile = 10, maxDuration = 1.minute)
          )
          .via(StreamProcessor.groupByCheckpoint)
          .runCollect
      yield assertTrue(batches.isEmpty)
    }.provide(RecordWriter.Stub.layer),
    test("stream with only data (no checkpoints) produces no file batches") {
      // Edge case: user forgets to emit checkpoints
      // Files get written but never committed (no checkpoint to trigger)
      // This documents current behavior - might want to fail loudly instead
      val noCheckpointConnector = new Connector {
        def connectorId            = "no-checkpoint"
        def extract(state: String) = ZStream(
          StreamElement.Data("record1"),
          StreamElement.Data("record2")
        )
      }

      for
        recordWriter <- ZIO.service[RecordWriter]
        batches      <- noCheckpointConnector
          .extract("")
          .via(
            recordWriter
              .writeFiles(testSyncMetadata, "no-checkpoint", "1.0.0", maxRecordsPerFile = 10, maxDuration = 1.minute)
          )
          .via(StreamProcessor.groupByCheckpoint)
          .runCollect
      yield assertTrue(
        batches.isEmpty // No checkpoint = files written but not committed
      )
    }.provide(RecordWriter.Stub.layer)
  )
