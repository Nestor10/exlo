package exlo.infra

import exlo.domain.DataFile
import exlo.domain.ExloRecord
import org.apache.iceberg.Table
import zio.*
import zio.test.*
import zio.test.Assertion.*

import java.time.Instant
import java.util.UUID

/**
 * Tests for IcebergWriter.InMemory test implementation.
 *
 * Verifies that the in-memory writer correctly captures write and commit
 * operations for testing purposes.
 *
 * The InMemory writer is perfect for testing connectors (especially YAML
 * connectors) without needing a real Iceberg catalog. It captures all
 * operations in Refs that can be inspected in tests.
 *
 * Example usage in YAML connector tests:
 * {{{
 * test("yaml connector extracts users") {
 *   for {
 *     writer <- ZIO.service[IcebergWriter.InMemory]
 *
 *     // Run your connector extraction...
 *     _ <- YamlConnector.extract("").runDrain
 *
 *     // Assert on captured operations
 *     writes   <- writer.getWrites
 *     commits  <- writer.getCommits
 *     payloads <- writer.getAllPayloads
 *
 *     _ <- assertTrue(writes.length == 1)
 *     _ <- assertTrue(payloads.exists(_.contains("user-123")))
 *     _ <- assertTrue(commits.head.state.contains("cursor"))
 *   } yield ()
 * }.provide(IcebergWriter.InMemory.layer, ...)
 * }}}
 */
object IcebergWriterInMemorySpec extends ZIOSpecDefault:

  def spec = suite("IcebergWriter.InMemory")(
    test("captures single write operation") {
      for
        writer <- ZIO.service[IcebergWriter.InMemory]
        records = createTestRecords(5)

        // Perform write
        _ <- writer.writeAndStageRecords(null, records)

        // Assert on captured write
        writes <- writer.getWrites
      yield assertTrue(
        writes.length == 1,
        writes.head.recordCount == 5,
        writes.head.payloads.length == 5,
        writes.head.payloads.head == "payload-0"
      )
    },
    test("captures multiple write operations") {
      for
        writer <- ZIO.service[IcebergWriter.InMemory]

        // Perform multiple writes
        _ <- writer.writeAndStageRecords(null, createTestRecords(3))
        _ <- writer.writeAndStageRecords(null, createTestRecords(2))
        _ <- writer.writeAndStageRecords(null, createTestRecords(4))

        // Assert on captured writes
        writes <- writer.getWrites
        total  <- writer.getTotalRecordCount
      yield assertTrue(
        writes.length == 3,
        writes(0).recordCount == 3,
        writes(1).recordCount == 2,
        writes(2).recordCount == 4,
        total == 9
      )
    },
    test("captures commit operation") {
      for
        writer <- ZIO.service[IcebergWriter.InMemory]
        records = createTestRecords(5)

        // Write and commit
        _ <- writer.writeAndStageRecords(null, records)
        _ <- writer.commitTransaction(null, """{"cursor":"abc"}""", 1L, "test-stream")

        // Assert on captured commit
        commits <- writer.getCommits
      yield assertTrue(
        commits.length == 1,
        commits.head.state == """{"cursor":"abc"}""",
        commits.head.stateVersion == 1L,
        commits.head.writeCount == 1
      )
    },
    test("clears staged writes after commit") {
      for
        writer <- ZIO.service[IcebergWriter.InMemory]

        // Write, commit, check staged is cleared
        _       <- writer.writeAndStageRecords(null, createTestRecords(3))
        staged1 <- writer.getStagedWrites
        _       <- writer.commitTransaction(null, """{}""", 1L, "test-stream")
        staged2 <- writer.getStagedWrites
      yield assertTrue(
        staged1.length == 1,
        staged2.isEmpty
      )
    },
    test("tracks multiple write-commit cycles") {
      for
        writer <- ZIO.service[IcebergWriter.InMemory]

        // First cycle
        _ <- writer.writeAndStageRecords(null, createTestRecords(2))
        _ <- writer.writeAndStageRecords(null, createTestRecords(3))
        _ <- writer.commitTransaction(null, """{"cursor":"first"}""", 1L, "test-stream")

        // Second cycle
        _ <- writer.writeAndStageRecords(null, createTestRecords(4))
        _ <- writer.commitTransaction(null, """{"cursor":"second"}""", 2L, "test-stream")

        // Assert
        writes  <- writer.getWrites
        commits <- writer.getCommits
      yield assertTrue(
        writes.length == 3,
        commits.length == 2,
        commits(0).writeCount == 2,
        commits(1).writeCount == 1,
        commits(0).state.contains("first"),
        commits(1).state.contains("second")
      )
    },
    test("updates snapshot state after commit") {
      for
        writer <- ZIO.service[IcebergWriter.InMemory]

        // Initial state is None
        state1 <- writer.readSnapshotSummary(null)

        // Commit updates state
        _      <- writer.commitTransaction(null, """{"cursor":"abc"}""", 1L, "test-stream")
        state2 <- writer.readSnapshotSummary(null)

        // Another commit updates state again
        _      <- writer.commitTransaction(null, """{"cursor":"xyz"}""", 2L, "test-stream")
        state3 <- writer.readSnapshotSummary(null)
      yield assertTrue(
        state1.isEmpty,
        state2 == Some(("""{"cursor":"abc"}""", 1L, "test-stream")),
        state3 == Some(("""{"cursor":"xyz"}""", 2L, "test-stream"))
      )
    },
    test("getAllPayloads returns all user data") {
      for
        writer <- ZIO.service[IcebergWriter.InMemory]

        _ <- writer.writeAndStageRecords(null, createTestRecords(2))
        _ <- writer.writeAndStageRecords(null, createTestRecords(3))

        payloads <- writer.getAllPayloads
      yield assertTrue(
        payloads.length == 5,
        payloads == Chunk(
          "payload-0",
          "payload-1",
          "payload-0",
          "payload-1",
          "payload-2"
        )
      )
    },
    test("reset clears all state") {
      for
        writer <- ZIO.service[IcebergWriter.InMemory]

        // Perform some operations
        _ <- writer.writeAndStageRecords(null, createTestRecords(3))
        _ <- writer.commitTransaction(null, """{}""", 1L, "test-stream")
        _ <- writer.writeAndStageRecords(null, createTestRecords(2))

        // Reset
        _ <- writer.reset

        // Check everything is cleared
        writes  <- writer.getWrites
        commits <- writer.getCommits
        staged  <- writer.getStagedWrites
        state   <- writer.readSnapshotSummary(null)
        total   <- writer.getTotalRecordCount
      yield assertTrue(
        writes.isEmpty,
        commits.isEmpty,
        staged.isEmpty,
        state.isEmpty,
        total == 0
      )
    },
    test("commit with no staged writes creates empty commit") {
      for
        writer <- ZIO.service[IcebergWriter.InMemory]

        // Commit without any writes
        _ <- writer.commitTransaction(null, """{"initial":"state"}""", 1L, "test-stream")

        commits <- writer.getCommits
        writes  <- writer.getWrites
        state   <- writer.readSnapshotSummary(null)
      yield assertTrue(
        commits.length == 1,
        commits.head.writeCount == 0,
        writes.isEmpty,
        state == Some(("""{"initial":"state"}""", 1L, "test-stream"))
      )
    }
  ).provide(
    ZLayer {
      for
        writesRef       <- Ref.make(Chunk.empty[IcebergWriter.WriteOperation])
        commitsRef      <- Ref.make(Chunk.empty[IcebergWriter.CommitOperation])
        stagedWritesRef <- Ref.make(Chunk.empty[IcebergWriter.WriteOperation])
        currentStateRef <- Ref.make[Option[(String, Long, String)]](None)
      yield IcebergWriter.InMemory(writesRef, commitsRef, stagedWritesRef, currentStateRef)
    }
  )

  /** Create test ExloRecords with sequential payloads. */
  private def createTestRecords(count: Int): Chunk[ExloRecord] =
    Chunk.fromIterable(
      (0 until count).map { i =>
        ExloRecord(
          commitId = UUID.randomUUID(),
          connectorId = "test-connector",
          syncId = UUID.randomUUID(),
          committedAt = Instant.now(),
          recordedAt = Instant.now(),
          connectorVersion = "1.0.0",
          connectorConfigHash = "config-hash",
          streamConfigHash = "stream-hash",
          streamName = "test-stream",
          stateVersion = 1L,
          payload = s"payload-$i"
        )
      }
    )
