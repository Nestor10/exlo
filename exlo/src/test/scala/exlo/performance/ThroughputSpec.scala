package exlo.performance

import exlo.domain.*
import exlo.infra.{IcebergCatalog, NessieTestContainer}
import exlo.pipeline.PipelineOrchestrator
import exlo.service.{Connector, Table}
import zio.*
import zio.stream.*
import zio.test.*
import zio.test.TestAspect.*

import java.time.Instant
import java.util.UUID

/**
 * Performance and throughput tests for EXLO framework.
 *
 * These tests measure: - Records per second written to Iceberg - Data volume (MB/s) throughput -
 * Parquet file counts and sizes - Memory efficiency with large datasets - Commit overhead
 *
 * Run with: sbt "testOnly exlo.performance.ThroughputSpec"
 *
 * These are integration tests requiring Docker containers (Nessie + MinIO).
 */
object ThroughputSpec extends ZIOSpecDefault:

  def spec = suite("Performance & Throughput Tests")(
    test("write 10K records - measure throughput") {
      val namespace   = "perf_test"
      val tableName   = s"throughput_10k_${UUID.randomUUID()}"
      val recordCount = 10000

      (for {
        catalog <- ZIO.service[IcebergCatalog]
        _       <- catalog.createTable(namespace, tableName)

        // Generate test data
        connector = createHighVolumeConnector(recordCount, payloadSizeBytes = 1024)

        // Create sync metadata
        syncMetadata   = SyncMetadata(
          syncId = UUID.randomUUID(),
          startedAt = Instant.now(),
          connectorVersion = "1.0.0"
        )

        // Create config provider for this test
        configProvider = exlo.config.ExloConfigProvider.fromMap(
          Map(
            "EXLO_STREAM_NAMESPACE"  -> namespace,
            "EXLO_STREAM_TABLE_NAME" -> tableName
          )
        )

        // Measure execution time
        startTime <- Clock.instant
        _       <- PipelineOrchestrator
          .run(syncMetadata, stateVersion = 1L)
          .withConfigProvider(configProvider)
          .provideSome[IcebergCatalog](
            ZLayer.succeed(connector),
            tableLayer(namespace, tableName, "test-stream"),
            buildStreamConfig(namespace, tableName, "test-stream"),
            ZLayer.succeed(StateConfig(version = 1L))
          )
        endTime <- Clock.instant

        // Calculate metrics
        durationMs = endTime.toEpochMilli - startTime.toEpochMilli
        throughput = (recordCount.toDouble / durationMs) * 1000 // records/second
        dataMB     = (recordCount * 1024.0) / (1024 * 1024)     // total data in MB
        mbPerSec   = (dataMB / durationMs) * 1000               // MB/second

        // Read back to verify
        summary <- catalog.readSnapshotSummary(namespace, tableName)

        _ <- ZIO.logInfo(s"""
          |Performance Results (10K records):
          |  Duration: ${durationMs}ms
          |  Throughput: ${throughput.round} records/sec
          |  Data volume: ${dataMB.round}MB
          |  Speed: ${mbPerSec.round}MB/sec
          |  State: ${summary.map(_._1).getOrElse("none")}
          |""".stripMargin)

      } yield assertTrue(
        durationMs > 0,
        throughput > 0,
        summary.isDefined,
        summary.get._1 == """{"processed":10000}"""
      )).provide(catalogForTable(namespace, tableName))
    },
    test("write 100K records - verify memory efficiency") {
      val namespace   = "perf_test"
      val tableName   = s"throughput_100k_${UUID.randomUUID()}"
      val recordCount = 100000

      (for {
        catalog <- ZIO.service[IcebergCatalog]
        _       <- catalog.createTable(namespace, tableName)

        // Check initial memory (use Java Runtime)
        initialMem <- ZIO.succeed(
          java.lang.Runtime.getRuntime.totalMemory() - java.lang.Runtime.getRuntime.freeMemory()
        )

        // Generate test data with larger payloads
        connector = createHighVolumeConnector(recordCount, payloadSizeBytes = 2048)

        // Create config provider for this test
        configProvider = exlo.config.ExloConfigProvider.fromMap(
          Map(
            "EXLO_STREAM_NAMESPACE"  -> namespace,
            "EXLO_STREAM_TABLE_NAME" -> tableName
          )
        )

        // Create sync metadata
        syncMetadata   = SyncMetadata(
          syncId = UUID.randomUUID(),
          startedAt = Instant.now(),
          connectorVersion = "1.0.0"
        )

        // Measure execution time
        startTime <- Clock.instant
        _         <- PipelineOrchestrator
          .run(syncMetadata, stateVersion = 1L)
          .withConfigProvider(configProvider)
          .provideSome[IcebergCatalog](
            ZLayer.succeed(connector),
            tableLayer(namespace, tableName, "test-stream"),
            buildStreamConfig(namespace, tableName, "test-stream"),
            ZLayer.succeed(StateConfig(version = 1L))
          )
        endTime   <- Clock.instant

        // Check final memory (should be similar - constant memory usage)
        finalMem <- ZIO.succeed(java.lang.Runtime.getRuntime.totalMemory() - java.lang.Runtime.getRuntime.freeMemory())

        // Calculate metrics
        durationMs    = endTime.toEpochMilli - startTime.toEpochMilli
        throughput    = (recordCount.toDouble / durationMs) * 1000 // records/second
        dataMB        = (recordCount * 2048.0) / (1024 * 1024)     // total data in MB
        mbPerSec      = (dataMB / durationMs) * 1000               // MB/second
        memIncreaseMB = (finalMem - initialMem).toDouble / (1024 * 1024)

        summary <- catalog.readSnapshotSummary(namespace, tableName)

        _ <- ZIO.logInfo(s"""
          |Performance Results (100K records):
          |  Duration: ${durationMs}ms
          |  Throughput: ${throughput.round} records/sec
          |  Data volume: ${dataMB.round}MB
          |  Speed: ${mbPerSec.round}MB/sec
          |  Memory increase: ${memIncreaseMB.round}MB (should be ~constant)
          |  State: ${summary.map(_._1).getOrElse("none")}
          |""".stripMargin)

      } yield assertTrue(
        durationMs > 0,
        throughput > 0,
        summary.isDefined,
        summary.get._1 == """{"processed":100000}""",
        memIncreaseMB < 500 // Memory increase should be modest (< 500MB for 100K records)
      )).provide(catalogForTable(namespace, tableName))
    },
    test("batching performance - 1K vs 10K checkpoint intervals") {
      val namespace   = "perf_test"
      val recordCount = 50000

      (for {
        catalog <- ZIO.service[IcebergCatalog]

        // Test with 1K checkpoint interval
        tableName1k = s"batch_1k_${UUID.randomUUID()}"
        _ <- catalog.createTable(namespace, tableName1k)
        configProvider1k = exlo.config.ExloConfigProvider.fromMap(
          Map(
            "EXLO_STREAM_NAMESPACE"  -> namespace,
            "EXLO_STREAM_TABLE_NAME" -> tableName1k
          )
        )
        connector1k      = createHighVolumeConnector(recordCount, payloadSizeBytes = 512, checkpointEvery = 1000)
        syncMetadata1k   = SyncMetadata(UUID.randomUUID(), Instant.now(), "1.0.0")
        start1k <- Clock.instant
        _       <- PipelineOrchestrator
          .run(syncMetadata1k, stateVersion = 1L)
          .withConfigProvider(configProvider1k)
          .provideSome[IcebergCatalog](
            ZLayer.succeed(connector1k),
            tableLayer(namespace, tableName1k, "test-stream"),
            buildStreamConfig(namespace, tableName1k, "test-stream"),
            ZLayer.succeed(StateConfig(version = 1L))
          )
        end1k   <- Clock.instant
        duration1k = end1k.toEpochMilli - start1k.toEpochMilli

        // Test with 10K checkpoint interval
        tableName10k = s"batch_10k_${UUID.randomUUID()}"
        _ <- catalog.createTable(namespace, tableName10k)
        configProvider10k = exlo.config.ExloConfigProvider.fromMap(
          Map(
            "EXLO_STREAM_NAMESPACE"  -> namespace,
            "EXLO_STREAM_TABLE_NAME" -> tableName10k
          )
        )
        connector10k      = createHighVolumeConnector(recordCount, payloadSizeBytes = 512, checkpointEvery = 10000)
        syncMetadata10k   = SyncMetadata(UUID.randomUUID(), Instant.now(), "1.0.0")
        start10k <- Clock.instant
        _        <- PipelineOrchestrator
          .run(syncMetadata10k, stateVersion = 1L)
          .withConfigProvider(configProvider10k)
          .provideSome[IcebergCatalog](
            ZLayer.succeed(connector10k),
            tableLayer(namespace, tableName10k, "test-stream"),
            buildStreamConfig(namespace, tableName10k, "test-stream"),
            ZLayer.succeed(StateConfig(version = 1L))
          )
        end10k   <- Clock.instant
        duration10k = end10k.toEpochMilli - start10k.toEpochMilli

        throughput1k  = (recordCount.toDouble / duration1k) * 1000
        throughput10k = (recordCount.toDouble / duration10k) * 1000

        _ <- ZIO.logInfo(s"""
          |Batching Performance (50K records):
          |  1K checkpoints:  ${duration1k}ms (${throughput1k.round} records/sec)
          |  10K checkpoints: ${duration10k}ms (${throughput10k.round} records/sec)
          |  Speedup: ${((throughput10k / throughput1k) * 100).round}%
          |""".stripMargin)

      } yield assertTrue(
        duration1k > 0,
        duration10k > 0,
        throughput10k > throughput1k // Larger checkpoint intervals should be faster
      )).provide(catalogForTable(namespace, "batch_test"))
    }
  ) @@ sequential @@ withLiveClock // Use real clock for timing

  /**
   * Create a high-volume test connector that generates many records.
   *
   * @param totalRecords
   *   Total number of records to generate
   * @param payloadSizeBytes
   *   Size of each record's payload in bytes
   * @param checkpointEvery
   *   Emit checkpoint every N records
   */
  private def createHighVolumeConnector(
    totalRecords: Int,
    payloadSizeBytes: Int,
    checkpointEvery: Int = 10000
  ): Connector =
    new Connector:
      val connectorId      = "high-volume-test"
      val connectorVersion = "1.0.0"

      def extract(state: String): ZStream[Any, ExloError, StreamElement] =
        // Generate payload of specified size (filled with 'x')
        val payload = "x" * payloadSizeBytes

        ZStream
          .fromIterable(1 to totalRecords)
          .mapZIO { i =>
            val data = StreamElement.Data(s"""{"id":$i,"payload":"$payload"}""")

            // Emit checkpoint every N records
            if (i % checkpointEvery == 0 && i < totalRecords) {
              ZIO.succeed(Chunk(data, StreamElement.Checkpoint(s"""{"processed":$i}""")))
            } else if (i == totalRecords) {
              // Final checkpoint
              ZIO.succeed(Chunk(data, StreamElement.Checkpoint(s"""{"processed":$i}""")))
            } else {
              ZIO.succeed(Chunk(data))
            }
          }
          .flattenChunks

  /** Build Table layer for testing (doesn't load config from environment). */
  private def tableLayer(namespace: String, tableName: String, streamName: String) =
    ZLayer.fromZIO(
      ZIO.service[IcebergCatalog].map(catalog => Table.Live(namespace, tableName, streamName, catalog))
    )

  /** Build StreamConfig layer for testing. */
  private def buildStreamConfig(namespace: String, tableName: String, streamName: String) =
    ZLayer.succeed(
      exlo.config.StreamConfig(
        namespace = namespace,
        tableName = tableName,
        streamName = streamName
      )
    )

  /** Create catalog layer for a specific table (uses NessieTestContainer). */
  private def catalogForTable(namespace: String, tableName: String) =
    NessieTestContainer.catalogLayer(namespace, tableName)
