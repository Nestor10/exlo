package exlo.domain

import exlo.Exlo
import exlo.runtime.{Destination, ExloState, SinkConfig}
import zio.*
import zio.stream.ZStream
import zio.test.*

object ConnectorAspectSpec extends ZIOSpecDefault:

  val baseConnector: Connector[Unit, Any, Throwable] =
    Connector.stateless("base", "0.1.0") {
      ZStream.fromZIO(ExloState.emit[Unit](Chunk("a", "b", "c")))
    }

  def spec = suite("ConnectorAspect")(
    test("identity aspect: connector behavior unchanged") {
      val wrapped = baseConnector @@ ConnectorAspect.identity
      for
        dest <- Destination.InMemory.make[Unit]
        fiber <- Exlo
                   .run(wrapped, (), SinkConfig.testing)
                   .provideSomeLayer[Any](ZLayer.succeed[Destination[Unit]](dest))
                   .fork
        _    <- TestClock.adjust(1.second)
        _    <- fiber.join
        all  <- dest.allRecords
      yield assertTrue(all == Chunk("a", "b", "c"))
    },
    test("logging aspect: start/done log lines emitted around the connector run") {
      val wrapped = baseConnector @@ ConnectorAspect.logging
      for
        dest <- Destination.InMemory.make[Unit]
        fiber <- Exlo
                   .run(wrapped, (), SinkConfig.testing)
                   .provideSomeLayer[Any](ZLayer.succeed[Destination[Unit]](dest))
                   .fork
        _    <- TestClock.adjust(1.second)
        _    <- fiber.join
        all  <- dest.allRecords
        logs <- ZTestLogger.logOutput
      yield assertTrue(
        all == Chunk("a", "b", "c"),
        logs.exists(_.message().contains("connector start")),
        logs.exists(_.message().contains("connector done"))
      )
    }
  )
