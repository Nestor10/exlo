package exlo.integration

import exlo.config.StreamConfig
import exlo.domain.ExloError
import exlo.domain.StateConfig
import exlo.domain.StreamElement
import exlo.domain.SyncMetadata
import exlo.infra.IcebergCatalog
import exlo.pipeline.PipelineOrchestrator
import exlo.service.Connector
import exlo.service.Table
import zio.*
import zio.stream.*
import zio.test.*
import zio.test.Assertion.*

import java.util.UUID

/**
 * Integration tests for the complete EXLO pipeline.
 *
 * Tests verify the end-to-end flow using PipelineOrchestrator:
 * 1. Read state from table (or fresh start)
 * 2. Extract data from connector
 * 3. Batch records and wrap with metadata
 * 4. Append records to table
 * 5. Commit atomically when checkpoint arrives
 */
object StreamProcessingSpec extends ZIOSpecDefault:

  val testSyncMetadata = SyncMetadata(
    syncId = UUID.randomUUID(),
    startedAt = java.time.Instant.now(),
    connectorVersion = "1.0.0-test"
  )

  val testStateConfig = StateConfig(version = 1L)
  
  val testConfigProvider = exlo.config.ExloConfigProvider.fromMap(Map(
    "EXLO_STREAM_NAMESPACE" -> "test",
    "EXLO_STREAM_TABLE_NAME" -> "test_table"
  ))

  def spec = suite("Pipeline Integration")(
    test("processes connector stream and commits at checkpoints") {
      // BEHAVIOR:
      // Connector emits: Data, Data, Checkpoint, Data, Checkpoint
      // Pipeline batches records, appends to table, commits at each checkpoint

      for
        table <- ZIO.service[Table]

        // Run the pipeline
        _ <- PipelineOrchestrator.run(
          syncMetadata = testSyncMetadata,
          stateVersion = 1L
        ).withConfigProvider(testConfigProvider)

        // Verify results
        stub = table.asInstanceOf[Table.Stub]
      yield assertTrue(
        stub.commitCount == 2,                       // Two checkpoints = two commits
        stub.lastState == """{"cursor":"page_3"}""", // Final checkpoint state
        stub.writtenFiles.size == 2                  // Two batches written
      )
    }.provide(
      StubConnector.layer,
      Table.Stub.layer,
      IcebergCatalog.Stub.layer,
      ZLayer.succeed(testStateConfig)
    ),
    test("empty stream produces no commits") {
      // Edge case: connector emits no elements
      for
        table <- ZIO.service[Table]

        _ <- PipelineOrchestrator.run(
          syncMetadata = testSyncMetadata,
          stateVersion = 1L
        ).withConfigProvider(testConfigProvider)

        stub = table.asInstanceOf[Table.Stub]
      yield assertTrue(
        stub.commitCount == 0,    // No checkpoints = no commits
        stub.writtenFiles.isEmpty // No data written
      )
    }.provide(
      ZLayer.succeed[Connector](new Connector {
        def connectorId            = "empty"
        def extract(state: String) = ZStream.empty
      }),
      Table.Stub.layer,
      IcebergCatalog.Stub.layer,
      ZLayer.succeed(testStateConfig)
    ),
    test("stream with only data (no checkpoints) produces no commits") {
      // Edge case: user forgets to emit checkpoints
      // Records are appended but never committed (no checkpoint to trigger)
      for
        table <- ZIO.service[Table]

        _ <- PipelineOrchestrator.run(
          syncMetadata = testSyncMetadata,
          stateVersion = 1L
        ).withConfigProvider(testConfigProvider)

        stub = table.asInstanceOf[Table.Stub]
      yield assertTrue(
        stub.commitCount == 0,    // No checkpoint = no commit
        stub.writtenFiles.isEmpty // No batches emitted without checkpoint
      )
    }.provide(
      ZLayer.succeed[Connector](new Connector {
        def connectorId            = "no-checkpoint"
        def extract(state: String) = ZStream(
          StreamElement.Data("record1"),
          StreamElement.Data("record2")
        )
      }),
      Table.Stub.layer,
      IcebergCatalog.Stub.layer,
      ZLayer.succeed(testStateConfig)
    ),
    test("reads initial state and passes to connector") {
      // Verify that pipeline reads state from table and passes to connector
      for
        stateRef <- Ref.make[Option[String]](None)

        _ <- PipelineOrchestrator
          .run(
            syncMetadata = testSyncMetadata,
            stateVersion = 1L
          )
          .withConfigProvider(testConfigProvider)
          .provide(
            ZLayer.fromZIO(ZIO.succeed[Connector](new Connector {
              def connectorId            = "state-tracker"
              def extract(state: String) = {
                ZStream.fromZIO(stateRef.set(Some(state))) *>
                  ZStream(
                    StreamElement.Data("record1"),
                    StreamElement.Checkpoint("""{"cursor":"next"}""")
                  )
              }
            })),
            ZLayer.succeed[Table](
              Table.Stub(
                currentState = """{"cursor":"previous"}""",
                currentStateVersion = 1L
              )
            ),
            IcebergCatalog.Stub.layer,
            ZLayer.succeed(testStateConfig)
          )

        capturedState <- stateRef.get
      yield assertTrue(
        capturedState.contains("""{"cursor":"previous"}""")
      )
    }
  )

/** Stub connector for testing - emits static test data. */
case class StubConnector() extends Connector:

  def connectorId: String = "stub.test"

  def extract(state: String): ZStream[Any, ExloError, exlo.domain.StreamElement] =
    ZStream(
      exlo.domain.StreamElement.Data("""{"id":1,"name":"Alice"}"""),
      exlo.domain.StreamElement.Data("""{"id":2,"name":"Bob"}"""),
      exlo.domain.StreamElement.Checkpoint("""{"cursor":"page_2"}"""),
      exlo.domain.StreamElement.Data("""{"id":3,"name":"Charlie"}"""),
      exlo.domain.StreamElement.Checkpoint("""{"cursor":"page_3"}""")
    )

object StubConnector:

  val layer: ZLayer[Any, Nothing, Connector] =
    ZLayer.succeed(StubConnector())
