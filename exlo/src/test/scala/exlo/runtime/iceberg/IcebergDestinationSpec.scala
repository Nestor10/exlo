package exlo.runtime.iceberg

import exlo.runtime.RunContext
import org.apache.hadoop.conf.Configuration
import org.apache.iceberg.data.Record
import org.apache.iceberg.data.parquet.GenericParquetReaders
import org.apache.iceberg.hadoop.HadoopTables
import org.apache.iceberg.parquet.Parquet
import org.apache.iceberg.{PartitionSpec, Table}
import zio.*
import zio.json.*
import zio.test.*

import java.nio.file.{Files, Path}
import scala.jdk.CollectionConverters.*

/**
 * Integration test for IcebergDestination.
 *
 * Uses `HadoopTables` against a unique temp directory per test (Iceberg caches metadata
 * by path in the JVM, so reusing the same path within one process can serve stale data —
 * unique paths sidestep that). Cleanup via `ZIO.acquireRelease` is best-effort.
 */
object IcebergDestinationSpec extends ZIOSpecDefault:

  /** Test connector state. JsonCodec is required by IcebergDestination. */
  final case class TestState(cursor: String, count: Int)
  given JsonCodec[TestState] = DeriveJsonCodec.gen[TestState]

  /** Acquire/release a unique temp directory for each test. */
  private val tempDirLayer: ZLayer[Any, Throwable, Path] = ZLayer.scoped:
    ZIO.acquireRelease(
      ZIO.attemptBlocking(Files.createTempDirectory("exlo-iceberg-test-"))
    )(path => ZIO.attemptBlocking(deleteRecursively(path.toFile)).orDie)

  private def deleteRecursively(file: java.io.File): Unit =
    if file.isDirectory then Option(file.listFiles).foreach(_.foreach(deleteRecursively))
    file.delete()
    ()

  /**
   * Create a fresh Iceberg table at the given path under the temp dir. Returns the loaded
   * `Table` instance ready for use by the destination.
   */
  private def freshTable(rootPath: Path, tableName: String): Task[Table] =
    ZIO.attemptBlocking {
      val tables  = new HadoopTables(new Configuration())
      val tablePath = rootPath.resolve(tableName).toAbsolutePath.toUri.toString
      tables.create(IcebergDestination.schema, IcebergDestination.partitionSpec, tablePath)
    }

  /** Reload an existing table (simulates a separate process resuming on the same warehouse). */
  private def loadTable(rootPath: Path, tableName: String): Task[Table] =
    ZIO.attemptBlocking {
      val tables    = new HadoopTables(new Configuration())
      val tablePath = rootPath.resolve(tableName).toAbsolutePath.toUri.toString
      tables.load(tablePath)
    }

  def spec = suite("IcebergDestination")(
    test("commit lands records as a new snapshot AND state in snapshot summary properties") {
      for
        path  <- ZIO.service[Path]
        table <- freshTable(path, "tbl1")
        dest  <- IcebergDestination.fromTable[TestState](table)
        _     <- dest.writeRecords(Chunk("rec-a", "rec-b", "rec-c"))
        _     <- dest.commit(TestState(cursor = "after-batch-1", count = 3))
        // Inspect the table directly to verify the commit shape.
        _     <- ZIO.attemptBlocking(table.refresh())
        snapshots = table.snapshots().asScala.toList
        latest    = table.currentSnapshot()
        stateJson = Option(latest).flatMap(s => Option(s.summary().get("exlo.state")))
        decoded   = stateJson.flatMap(s => s.fromJson[TestState].toOption)
      yield assertTrue(
        snapshots.length == 1,
        decoded == Some(TestState("after-batch-1", 3))
      )
    },
    test("readState returns the latest committed state, None on empty table") {
      for
        path  <- ZIO.service[Path]
        table <- freshTable(path, "tbl2")
        dest  <- IcebergDestination.fromTable[TestState](table)
        before <- dest.readState
        _      <- dest.writeRecords(Chunk("only"))
        _      <- dest.commit(TestState("done", 1))
        after  <- dest.readState
      yield assertTrue(before == None, after == Some(TestState("done", 1)))
    },
    test("two destinations over the same warehouse path: second sees first's state (resume)") {
      for
        path <- ZIO.service[Path]
        // First "run": create + write + commit
        table1 <- freshTable(path, "tbl3")
        dest1  <- IcebergDestination.fromTable[TestState](table1)
        _      <- dest1.writeRecords(Chunk("a", "b"))
        _      <- dest1.commit(TestState("page-1-done", 2))
        // Second "run": load same table (different Table instance), read state back
        table2 <- loadTable(path, "tbl3")
        dest2  <- IcebergDestination.fromTable[TestState](table2)
        resumed <- dest2.readState
      yield assertTrue(resumed == Some(TestState("page-1-done", 2)))
    },
    test("multiple commits each produce a snapshot; each snapshot has its own state") {
      for
        path  <- ZIO.service[Path]
        table <- freshTable(path, "tbl4")
        dest  <- IcebergDestination.fromTable[TestState](table)
        _     <- dest.writeRecords(Chunk("r1"))
        _     <- dest.commit(TestState("c1", 1))
        _     <- dest.writeRecords(Chunk("r2"))
        _     <- dest.commit(TestState("c2", 2))
        _     <- ZIO.attemptBlocking(table.refresh())
        snapshots = table.snapshots().asScala.toList
        // Each snapshot's summary holds the state at the time of that commit.
        states = snapshots.map(s =>
          Option(s.summary().get("exlo.state")).flatMap(_.fromJson[TestState].toOption)
        )
      yield assertTrue(
        snapshots.length == 2,
        states == List(Some(TestState("c1", 1)), Some(TestState("c2", 2)))
      )
    },
    test("records carry framework-managed metadata columns (sync_id, connector, version, recorded_at)") {
      for
        path  <- ZIO.service[Path]
        table <- freshTable(path, "tbl-metadata")
        dest  <- IcebergDestination.fromTable[TestState](table)
        // RunContext sets the FiberRefs that IcebergDestination reads when stamping records.
        _ <- RunContext.withRun(
               syncIdValue           = "test-sync-id-abc",
               connectorIdValue      = "metadata-test",
               connectorVersionValue = "9.9.9"
             ) {
               dest.writeRecords(Chunk("hello-world")) *> dest.commit(TestState("c1", 1))
             }
        _ <- ZIO.attemptBlocking(table.refresh())
        records <- ZIO.attemptBlocking[List[Record]] {
          // Read all data files for the table, project the full schema, return raw records.
          val task = table.newScan().planFiles().iterator().next()
          val inputFile = table.io().newInputFile(task.file().location())
          val reader = Parquet
            .read(inputFile)
            .project(table.schema())
            .createReaderFunc((fileSchema: org.apache.parquet.schema.MessageType) =>
              GenericParquetReaders.buildReader(table.schema(), fileSchema)
            )
            .build()
          try reader.iterator().asScala.map(_.asInstanceOf[Record]).toList
          finally reader.close()
        }
        record: Record = records.head
      yield assertTrue(
        records.length == 1,
        record.getField("payload")                == "hello-world",
        record.getField("exlo_sync_id")           == "test-sync-id-abc",
        record.getField("exlo_connector")         == "metadata-test",
        record.getField("exlo_connector_version") == "9.9.9",
        record.getField("exlo_recorded_at") != null
      )
    },
    test("commit with no staged records still advances state (state-only commit)") {
      // A state-only commit creates a snapshot with no DataFiles but does set the state
      // property. Useful when the connector advances state without producing records.
      for
        path  <- ZIO.service[Path]
        table <- freshTable(path, "tbl5")
        dest  <- IcebergDestination.fromTable[TestState](table)
        _     <- dest.commit(TestState("just-state", 0))
        snap  <- dest.readState
      yield assertTrue(snap == Some(TestState("just-state", 0)))
    }
  ).provideLayerShared(tempDirLayer)
