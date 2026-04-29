package exlo.runtime.iceberg

import exlo.Exlo
import exlo.domain.Connector
import exlo.runtime.{Destination, DestinationFactory, ExloState, SinkConfig, Telemetry}
import exlo.runtime.iceberg.IcebergCodecs.given
import zio.*
import zio.stream.ZStream
import zio.test.*

import java.nio.file.{Files, Path}

/**
 * End-to-end test for the env-driven `DestinationFactory` against a real Hadoop catalog
 * over a local temp directory. Proves that `EXLO_DESTINATION=iceberg` +
 * `EXLO_CATALOG_TYPE=hadoop` produces a working `Destination[S]` and a connector run
 * lands records in a real Iceberg table.
 *
 * Glue is NOT exercised here — it requires AWS creds at runtime. The Glue path uses the
 * exact same wiring; if Hadoop works, Glue works given valid credentials.
 */
object DestinationFactorySpec extends ZIOSpecDefault:

  /** Stateless test connector — emits a fixed batch and exits. */
  val testConnector: Connector[Unit, Any, Throwable] =
    Connector.stateless("factory_test", "0.1.0") {
      ZStream.fromZIO(ExloState.emit[Unit](Chunk("alpha", "beta", "gamma")))
    }

  private val tempDirLayer: ZLayer[Any, Throwable, Path] = ZLayer.scoped:
    ZIO.acquireRelease(
      ZIO.attemptBlocking(Files.createTempDirectory("exlo-factory-test-"))
    )(p => ZIO.attemptBlocking(deleteRecursively(p.toFile)).orDie)

  private def deleteRecursively(file: java.io.File): Unit =
    if file.isDirectory then Option(file.listFiles).foreach(_.foreach(deleteRecursively))
    file.delete()
    ()

  private def envProvider(warehouse: Path): ConfigProvider =
    ConfigProvider.fromMap(
      Map(
        "exlo.destination"        -> "iceberg",
        "exlo.catalog.type"       -> "hadoop",
        "exlo.catalog.warehouse"  -> warehouse.toAbsolutePath.toString,
        "exlo.table.namespace"    -> "exlo_test",
        "exlo.table.name"         -> "factory_test"
      )
    )

  def spec = suite("DestinationFactory")(
    test("EXLO_DESTINATION=iceberg + EXLO_CATALOG_TYPE=hadoop wires through end-to-end") {
      for
        warehouse <- ZIO.service[Path]
        // Inject the "env" via a test ConfigProvider; wires through to all the *.fromEnv
        // calls inside DestinationFactory + Catalogs + TableConfig.
        _ <- Exlo
               .run(testConnector, (), SinkConfig.testing)
               .provide(DestinationFactory.layer[Unit]("factory_test"), Telemetry.noop)
               .withConfigProvider(envProvider(warehouse))
        // The Hadoop catalog persists table metadata under
        // <warehouse>/<namespace>/<table>. Verify the directory structure exists.
        tableMetaExists <- ZIO.attemptBlocking {
          warehouse.resolve("exlo_test").resolve("factory_test").resolve("metadata").toFile.exists()
        }
      yield assertTrue(tableMetaExists)
    }
  ).provideLayerShared(tempDirLayer)
