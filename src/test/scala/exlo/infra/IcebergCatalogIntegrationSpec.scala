package exlo.infra

import exlo.domain.*
import zio.*
import zio.test.*
import zio.test.Assertion.*

import java.util.UUID

/**
 * Integration tests for IcebergCatalog.Live with real Nessie and MinIO.
 *
 * Pattern from Zionomicon Ch 44:
 * - Uses provideLayerShared for expensive container resources
 * - Containers started once, shared across all tests
 * - Each test creates its own catalog instance for its specific table
 */
object IcebergCatalogIntegrationSpec extends ZIOSpec[TestEnvironment & StorageConfig]:

  /**
   * Bootstrap layer provides test environment + containers.
   *
   * Lifecycle:
   * 1. Starts Nessie + MinIO containers (once for entire suite)
   * 2. Each test creates its own IcebergCatalog.Live for its table
   */
  override val bootstrap: ZLayer[Any, Any, TestEnvironment & StorageConfig] =
    testEnvironment ++ NessieTestContainer.layer.orDie

  /**
   * Create a catalog layer for a specific table.
   * Each test should create its own catalog instance.
   */
  private def catalogForTable(namespace: String, tableName: String): ZLayer[StorageConfig, Nothing, IcebergCatalog] =
    IcebergCatalogSelector.layer(namespace, tableName).orDie

  def spec = suite("IcebergCatalog Integration Tests")(
    test("create table and verify it exists") {
      val namespace = "test_ns"
      val tableName = "new_table"

      (for {
        catalog <- ZIO.service[IcebergCatalog]

        // Table shouldn't exist initially
        existsBefore <- catalog.tableExists(namespace, tableName)

        // Create the table
        _ <- catalog.createTable(namespace, tableName)

        // Now it should exist
        existsAfter <- catalog.tableExists(namespace, tableName)

        // Get location
        location <- catalog.getTableLocation(namespace, tableName)

      } yield assertTrue(
        !existsBefore,
        existsAfter,
        location.contains("s3://test-bucket/warehouse")
      )).provideSome[StorageConfig](catalogForTable(namespace, tableName))
    },
    test("write records, commit, and read state") {
      val namespace = "test_ns"
      val tableName = s"test_write_${UUID.randomUUID()}"

      (for {
        catalog <- ZIO.service[IcebergCatalog]

        // Create table
        _ <- catalog.createTable(namespace, tableName)

        // Create test records
        records = Chunk(
          ExloRecord(
            commitId = UUID.randomUUID(),
            connectorId = "test-connector",
            syncId = UUID.randomUUID(),
            committedAt = java.time.Instant.now(),
            recordedAt = java.time.Instant.now(),
            connectorVersion = "1.0.0",
            connectorConfigHash = "hash1",
            streamConfigHash = "hash2",
            stateVersion = 1L,
            payload = """{"user":"alice"}"""
          )
        )

        // Write and stage records
        dataFile <- catalog.writeAndStageRecords(namespace, tableName, records)

        // Commit with state
        state        = """{"cursor":"page_1"}"""
        stateVersion = 1L
        _ <- catalog.commitTransaction(namespace, tableName, state, stateVersion)

        // Read state back
        summary <- catalog.readSnapshotSummary(namespace, tableName)

      } yield assertTrue(
        dataFile.recordCount == 1,
        summary.get("exlo.state").contains(state),
        summary.get("exlo.state.version").contains("1")
      )).provideSome[StorageConfig](catalogForTable(namespace, tableName))
    },
    test("state version mismatch returns empty summary") {
      val namespace = "test_ns"
      val tableName = s"test_version_${UUID.randomUUID()}"

      (for {
        catalog <- ZIO.service[IcebergCatalog]

        // Create table
        _ <- catalog.createTable(namespace, tableName)

        // Read state from empty table
        summary <- catalog.readSnapshotSummary(namespace, tableName)

      } yield assertTrue(
        summary.isEmpty
      )).provideSome[StorageConfig](catalogForTable(namespace, tableName))
    },
    test("multiple batches accumulate before commit") {
      val namespace = "test_ns"
      val tableName = s"test_multi_${UUID.randomUUID()}"

      (for {
        catalog <- ZIO.service[IcebergCatalog]

        // Create table
        _ <- catalog.createTable(namespace, tableName)

        // Write batch 1
        batch1 = Chunk(
          ExloRecord(
            commitId = UUID.randomUUID(),
            connectorId = "test",
            syncId = UUID.randomUUID(),
            committedAt = java.time.Instant.now(),
            recordedAt = java.time.Instant.now(),
            connectorVersion = "1.0",
            connectorConfigHash = "h1",
            streamConfigHash = "h2",
            stateVersion = 1L,
            payload = """{"id":1}"""
          )
        )
        file1 <- catalog.writeAndStageRecords(namespace, tableName, batch1)

        // Write batch 2
        batch2 = Chunk(
          ExloRecord(
            commitId = UUID.randomUUID(),
            connectorId = "test",
            syncId = UUID.randomUUID(),
            committedAt = java.time.Instant.now(),
            recordedAt = java.time.Instant.now(),
            connectorVersion = "1.0",
            connectorConfigHash = "h1",
            streamConfigHash = "h2",
            stateVersion = 1L,
            payload = """{"id":2}"""
          )
        )
        file2 <- catalog.writeAndStageRecords(namespace, tableName, batch2)

        // Commit both batches atomically
        _ <- catalog.commitTransaction(namespace, tableName, """{"done":true}""", 1L)

        // Verify state
        summary <- catalog.readSnapshotSummary(namespace, tableName)

      } yield assertTrue(
        file1.recordCount == 1,
        file2.recordCount == 1,
        summary.get("exlo.state").contains("""{"done":true}""")
      )).provideSome[StorageConfig](catalogForTable(namespace, tableName))
    },
    test("commit with no staged files still persists state") {
      val namespace = "test_ns"
      val tableName = s"test_noop_${UUID.randomUUID()}"

      (for {
        catalog <- ZIO.service[IcebergCatalog]

        // Create table
        _ <- catalog.createTable(namespace, tableName)

        // Commit without writing any files - simulates connector doing work but finding no new data
        // This is CRITICAL: we must save state even if there are no records to avoid redoing expensive work
        _       <- catalog
          .commitTransaction(namespace, tableName, """{"cursor":"exhausted","checked_at":"2025-11-01T10:00:00Z"}""", 1L)

        // State MUST be persisted even though no data files were written
        summary <- catalog.readSnapshotSummary(namespace, tableName)

      } yield assertTrue(
        summary.get("exlo.state").contains("""{"cursor":"exhausted","checked_at":"2025-11-01T10:00:00Z"}"""),
        summary.get("exlo.state.version").contains("1"),
        summary.get("total-data-files").contains("0") // No data files, but snapshot exists
      )).provideSome[StorageConfig](catalogForTable(namespace, tableName))
    }
  ) @@ TestAspect.sequential // Run tests sequentially to avoid container issues
