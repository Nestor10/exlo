package exlo.runtime.iceberg

import exlo.runtime.RunContext
import org.apache.hadoop.conf.Configuration
import org.apache.iceberg.Table
import org.apache.iceberg.hadoop.HadoopTables
import zio.*
import zio.json.*
import zio.test.*

import java.nio.file.{Files, Path}

/**
 * Tests for the parent→child handoff: parent writes via IcebergDestination, child reads
 * via IcebergDataSource using `readIncremental(fromSnapshot)`.
 */
object IcebergDataSourceSpec extends ZIOSpecDefault:

  final case class TestState(cursor: String)
  given JsonCodec[TestState] = DeriveJsonCodec.gen[TestState]

  private val tempDirLayer: ZLayer[Any, Throwable, Path] = ZLayer.scoped:
    ZIO.acquireRelease(
      ZIO.attemptBlocking(Files.createTempDirectory("exlo-iceberg-source-"))
    )(p => ZIO.attemptBlocking(deleteRecursively(p.toFile)).orDie)

  private def deleteRecursively(file: java.io.File): Unit =
    if file.isDirectory then Option(file.listFiles).foreach(_.foreach(deleteRecursively))
    file.delete()
    ()

  private def freshTable(rootPath: Path, name: String): Task[Table] =
    ZIO.attemptBlocking {
      val tables    = new HadoopTables(new Configuration())
      val tablePath = rootPath.resolve(name).toAbsolutePath.toUri.toString
      tables.create(IcebergDestination.schema, IcebergDestination.partitionSpec, tablePath)
    }

  /** Write a batch of records as one commit; return the resulting snapshot ID. */
  private def writeBatch(
      table: Table,
      records: Chunk[String],
      state: TestState
  ): IO[exlo.domain.ExloError, Long] =
    for
      stateStore <- StateStore.InMemory.make
      dest <- IcebergDestination.fromTable[TestState](table, stateStore)
      _    <- dest.writeRecords(records)
      // RunContext is required by commit (reads connectorId/stream for the sidecar).
      _ <- RunContext.withRun("s", "ds-connector", "1.0") {
             RunContext.streamName.locally("ds-stream") {
               dest.commit(state)
             }
           }
      snap <- ZIO.attemptBlocking {
                table.refresh()
                table.currentSnapshot().snapshotId()
              }.mapError(t => exlo.domain.ExloError.StorageError("snapshot fetch", t))
    yield snap

  def spec = suite("IcebergDataSource")(
    test("latestSnapshot: None on empty table, Some(id) after a commit") {
      for
        path  <- ZIO.service[Path]
        table <- freshTable(path, "ds-1")
        source = IcebergDataSource.fromTable(table)
        beforeAny <- source.latestSnapshot
        _         <- writeBatch(table, Chunk("x"), TestState("c1"))
        afterOne  <- source.latestSnapshot
      yield assertTrue(beforeAny == None, afterOne.isDefined)
    },
    test("readIncremental(None) on empty table: empty stream") {
      for
        path  <- ZIO.service[Path]
        table <- freshTable(path, "ds-2")
        source = IcebergDataSource.fromTable(table)
        records <- source.readIncremental(None).runCollect
      yield assertTrue(records.isEmpty)
    },
    test("readIncremental(None) returns all records (full bootstrap scan)") {
      for
        path  <- ZIO.service[Path]
        table <- freshTable(path, "ds-3")
        _     <- writeBatch(table, Chunk("a", "b"), TestState("c1"))
        _     <- writeBatch(table, Chunk("c", "d"), TestState("c2"))
        source = IcebergDataSource.fromTable(table)
        records <- source.readIncremental(None).runCollect
      yield assertTrue(records.toSet == Set("a", "b", "c", "d"))
    },
    test("readIncremental(snapshot1) returns only records added after snapshot1") {
      for
        path  <- ZIO.service[Path]
        table <- freshTable(path, "ds-4")
        snap1 <- writeBatch(table, Chunk("first-a", "first-b"), TestState("c1"))
        _     <- writeBatch(table, Chunk("second-a", "second-b"), TestState("c2"))
        _     <- writeBatch(table, Chunk("third-a"), TestState("c3"))
        source = IcebergDataSource.fromTable(table)
        records <- source.readIncremental(Some(snap1)).runCollect
      yield assertTrue(
        records.toSet == Set("second-a", "second-b", "third-a"),
        !records.contains("first-a"),
        !records.contains("first-b")
      )
    },
    test("readIncremental(latestSnapshot) returns empty (caller is already caught up)") {
      for
        path   <- ZIO.service[Path]
        table  <- freshTable(path, "ds-5")
        latest <- writeBatch(table, Chunk("a", "b"), TestState("c1"))
        source = IcebergDataSource.fromTable(table)
        records <- source.readIncremental(Some(latest)).runCollect
      yield assertTrue(records.isEmpty)
    },
    test("readIncremental fails with SnapshotExpired when from-snapshot isn't an ancestor") {
      // Simulate by passing a fake snapshot ID that was never an ancestor of this table.
      // Iceberg surfaces this as an IllegalArgumentException with "not an ancestor" — we
      // refine into ExloError.SnapshotExpired.
      for
        path  <- ZIO.service[Path]
        table <- freshTable(path, "ds-6")
        _     <- writeBatch(table, Chunk("x"), TestState("c1"))
        source = IcebergDataSource.fromTable(table)
        result <- source.readIncremental(Some(99999999L)).runCollect.either
      yield assertTrue(
        result.left.exists(_.isInstanceOf[exlo.domain.ExloError.SnapshotExpired])
      )
    }
  ).provideLayerShared(tempDirLayer)
