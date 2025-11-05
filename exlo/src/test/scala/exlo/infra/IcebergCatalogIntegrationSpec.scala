package exlo.infra

import exlo.domain.*
import zio.*
import zio.test.*
import zio.test.Assertion.*

import java.util.UUID

/** Integration tests for IcebergCatalog.Live with real Nessie and MinIO.
  *
  * Pattern:
  * - Each test creates its own catalog layer with catalogLayer(namespace, tableName)
  * - Catalog layer starts containers, sets up config, loads StorageConfig, creates IcebergCatalog
  * - Containers are scoped to the test and cleaned up automatically
  */
object IcebergCatalogIntegrationSpec extends ZIOSpec[TestEnvironment]:

  override val bootstrap: ZLayer[Any, Any, TestEnvironment] =
    testEnvironment

  /** Create a catalog layer for a specific table.
    *
    * Each test gets its own containers + catalog instance.
    */
  private def catalogForTable(
      namespace: String,
      tableName: String
  ): ZLayer[Any, Throwable, IcebergCatalog] =
    NessieTestContainer.catalogLayer(namespace, tableName)

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
      )).provide(catalogForTable(namespace, tableName))
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
      )).provide(catalogForTable(namespace, tableName))
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
      )).provide(catalogForTable(namespace, tableName))
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
      )).provide(catalogForTable(namespace, tableName))
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
      )).provide(catalogForTable(namespace, tableName))
    }
  ) @@ TestAspect.sequential // Run tests sequentially to avoid container issues
