package exlo.domain

import exlo.Exlo
import exlo.runtime.{Destination, ExloState, SinkConfig}
import zio.*
import zio.stream.ZStream
import zio.test.*

object ConnectorFactorySpec extends ZIOSpecDefault:

  def spec = suite("Connector factories")(
    test("stateless: connector emits records via ExloState, framework tracks Unit state") {
      val c = Connector.stateless[Any, Throwable]("simple", "0.1.0") {
        ZStream.fromZIO(ExloState.emit[Unit](Chunk("a", "b", "c")))
      }
      for
        dest <- Destination.InMemory.make[Unit]
        fiber <- Exlo
                   .run(c, (), SinkConfig.testing)
                   .provideSomeLayer[Any](ZLayer.succeed[Destination[Unit]](dest))
                   .fork
        _    <- TestClock.adjust(1.second)
        _    <- fiber.join
        all  <- dest.allRecords
      yield assertTrue(all == Chunk("a", "b", "c"))
    },
    test("fromStream: stateful connector via inline factory") {
      final case class S(seq: Long)
      val c = Connector.fromStream[S, Any, Throwable]("stateful-inline", "0.1.0") {
        ZStream
          .fromIterable(1 to 3)
          .mapZIO { i =>
            ExloState.emit[S](Chunk(s"r-$i")) *>
              ExloState.update[S](_.copy(seq = i.toLong))
          }
      }
      for
        dest <- Destination.InMemory.make[S]
        fiber <- Exlo
                   .run(c, S(0), SinkConfig.testing)
                   .provideSomeLayer[Any](ZLayer.succeed[Destination[S]](dest))
                   .fork
        _    <- TestClock.adjust(1.second)
        _    <- fiber.join
        all  <- dest.allRecords
        snap <- dest.readState
      yield assertTrue(all == Chunk("r-1", "r-2", "r-3"), snap == Some(S(3)))
    }
  )
