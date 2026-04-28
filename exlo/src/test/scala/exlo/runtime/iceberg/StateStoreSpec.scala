package exlo.runtime.iceberg

import org.apache.hadoop.conf.Configuration
import org.apache.iceberg.hadoop.HadoopCatalog
import org.apache.iceberg.{AppendFiles, Table}
import org.apache.iceberg.catalog.{Catalog, Namespace, TableIdentifier}
import zio.*
import zio.test.*

import java.nio.file.{Files, Path}
import java.time.{Instant, ZoneOffset}

/**
 * Tests for [[StateStore]].
 *
 * Two suites:
 *   - `InMemory` — pure unit tests against the in-memory implementation; no I/O.
 *   - `Live`     — integration tests against a real [[HadoopCatalog]] over a local temp
 *                  directory, verifying Parquet write/read and sidecar table auto-creation.
 *
 * Bootstrap migration is tested in [[IcebergDestinationSpec]] (it lives in
 * `IcebergDestination.readState`, not inside `StateStore` itself).
 */
object StateStoreSpec extends ZIOSpecDefault:

  // ---- helpers ---------------------------------------------------------------------------

  private val tempDirLayer: ZLayer[Any, Throwable, Path] = ZLayer.scoped:
    ZIO.acquireRelease(
      ZIO.attemptBlocking(Files.createTempDirectory("exlo-statestore-test-"))
    )(p => ZIO.attemptBlocking(deleteRecursively(p.toFile)).orDie)

  private def deleteRecursively(file: java.io.File): Unit =
    if file.isDirectory then Option(file.listFiles).foreach(_.foreach(deleteRecursively))
    file.delete()
    ()

  private def hadoopCatalog(warehouse: Path): Task[HadoopCatalog] =
    ZIO.attemptBlocking(new HadoopCatalog(new Configuration(), warehouse.toAbsolutePath.toString))

  private def makeRow(
      connector: String,
      stream: String,
      state: String,
      committedAt: Instant = Instant.now()
  ): StateRow =
    StateRow(
      syncId              = Ulid.generate(),
      connector           = connector,
      stream              = stream,
      stateVersion        = 0L,
      connectorConfigHash = "",
      streamConfigHash    = "",
      state               = state,
      committedAt         = committedAt
    )

  // ---- InMemory suite --------------------------------------------------------------------

  private val inMemorySuite = suite("InMemory")(
    test("cold start: readLatest returns None when store is empty") {
      for
        store  <- StateStore.InMemory.make
        result <- store.readLatest("my-connector", "my-stream")
      yield assertTrue(result.isEmpty)
    },
    test("append then readLatest returns the appended row") {
      for
        store <- StateStore.InMemory.make
        row   = makeRow("c", "s", """{"cursor":"p1"}""")
        _     <- store.append(row)
        found <- store.readLatest("c", "s")
      yield assertTrue(found.exists(_.state == """{"cursor":"p1"}"""))
    },
    test("multiple appends: readLatest returns the row with the latest committedAt") {
      val t0 = Instant.parse("2024-01-01T00:00:00Z")
      val t1 = Instant.parse("2024-01-01T00:01:00Z")
      val t2 = Instant.parse("2024-01-01T00:02:00Z")
      for
        store <- StateStore.InMemory.make
        _     <- store.append(makeRow("c", "s", """{"cursor":"first"}""",  t0))
        _     <- store.append(makeRow("c", "s", """{"cursor":"second"}""", t1))
        _     <- store.append(makeRow("c", "s", """{"cursor":"third"}""",  t2))
        found <- store.readLatest("c", "s")
      yield assertTrue(found.exists(_.state == """{"cursor":"third"}"""))
    },
    test("readLatest is scoped to connector + stream (isolation across streams)") {
      for
        store <- StateStore.InMemory.make
        _     <- store.append(makeRow("conn-a", "stream-x", """{"cursor":"ax"}"""))
        _     <- store.append(makeRow("conn-a", "stream-y", """{"cursor":"ay"}"""))
        _     <- store.append(makeRow("conn-b", "stream-x", """{"cursor":"bx"}"""))
        ax    <- store.readLatest("conn-a", "stream-x")
        ay    <- store.readLatest("conn-a", "stream-y")
        bx    <- store.readLatest("conn-b", "stream-x")
        by    <- store.readLatest("conn-b", "stream-y")
      yield assertTrue(
        ax.exists(_.state == """{"cursor":"ax"}"""),
        ay.exists(_.state == """{"cursor":"ay"}"""),
        bx.exists(_.state == """{"cursor":"bx"}"""),
        by.isEmpty
      )
    },
    test("syncId tiebreaker: same committedAt, later syncId wins") {
      // Two rows at the same millisecond — the lexicographically larger ULID should win.
      val t = Instant.parse("2024-06-01T12:00:00Z")
      val rowA = makeRow("c", "s", """{"cursor":"a"}""", t).copy(syncId = "01HZ000000000000A")
      val rowB = makeRow("c", "s", """{"cursor":"b"}""", t).copy(syncId = "01HZ000000000000B")
      for
        store <- StateStore.InMemory.make
        _     <- store.append(rowA)
        _     <- store.append(rowB)
        found <- store.readLatest("c", "s")
      yield assertTrue(found.exists(_.state == """{"cursor":"b"}"""))
    }
  )

  // ---- Live (Iceberg) suite --------------------------------------------------------------

  private val liveSuite = suite("Live (Iceberg)")(
    test("cold start: readLatest returns None when sidecar table does not exist") {
      for
        warehouse <- ZIO.service[Path]
        catalog   <- hadoopCatalog(warehouse)
        store     = new StateStore.Live(catalog)
        result    <- store.readLatest("no-connector", "no-stream")
      yield assertTrue(result.isEmpty)
    },
    test("append auto-creates the exlo_state namespace and sidecar table") {
      for
        warehouse <- ZIO.service[Path]
        catalog   <- hadoopCatalog(warehouse)
        store     = new StateStore.Live(catalog)
        row       = makeRow("zen", "tickets", """{"page":1}""")
        _         <- store.append(row)
        // Table should now exist.
        exists <- ZIO.attemptBlocking {
                    catalog.tableExists(
                      TableIdentifier.of(Namespace.of("exlo_state"), "zen")
                    )
                  }
      yield assertTrue(exists)
    },
    test("append then readLatest round-trips the state payload") {
      for
        warehouse <- ZIO.service[Path]
        catalog   <- hadoopCatalog(warehouse)
        store     = new StateStore.Live(catalog)
        row       = makeRow("rtt", "stream1", """{"cursor":"round-trip"}""")
        _         <- store.append(row)
        found     <- store.readLatest("rtt", "stream1")
      yield assertTrue(found.exists(_.state == """{"cursor":"round-trip"}"""))
    },
    test("multiple appends: readLatest returns the row with the latest committedAt") {
      val t0 = Instant.parse("2024-03-01T00:00:00Z")
      val t1 = Instant.parse("2024-03-01T01:00:00Z")
      val t2 = Instant.parse("2024-03-01T02:00:00Z")
      for
        warehouse <- ZIO.service[Path]
        catalog   <- hadoopCatalog(warehouse)
        store     = new StateStore.Live(catalog)
        _         <- store.append(makeRow("latest", "s", """{"cursor":"first"}""",  t0))
        _         <- store.append(makeRow("latest", "s", """{"cursor":"second"}""", t1))
        _         <- store.append(makeRow("latest", "s", """{"cursor":"third"}""",  t2))
        found     <- store.readLatest("latest", "s")
      yield assertTrue(found.exists(_.state == """{"cursor":"third"}"""))
    },
    test("readLatest is partitioned by stream (only sees rows for the requested stream)") {
      for
        warehouse <- ZIO.service[Path]
        catalog   <- hadoopCatalog(warehouse)
        store     = new StateStore.Live(catalog)
        _         <- store.append(makeRow("multi", "tickets",  """{"page":10}"""))
        _         <- store.append(makeRow("multi", "comments", """{"page":5}"""))
        tickets   <- store.readLatest("multi", "tickets")
        comments  <- store.readLatest("multi", "comments")
        missing   <- store.readLatest("multi", "users")
      yield assertTrue(
        tickets.exists(_.state  == """{"page":10}"""),
        comments.exists(_.state == """{"page":5}"""),
        missing.isEmpty
      )
    }
  ).provideLayerShared(tempDirLayer)

  def spec = suite("StateStore")(
    inMemorySuite,
    liveSuite
  )
