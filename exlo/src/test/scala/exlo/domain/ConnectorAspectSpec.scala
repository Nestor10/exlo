package exlo.domain

import exlo.Exlo
import exlo.runtime.{Destination, ExloState, SinkConfig, Telemetry}
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
                   .provideSomeLayer[Any](ZLayer.succeed[Destination[Unit]](dest) ++ Telemetry.noop)
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
                   .provideSomeLayer[Any](ZLayer.succeed[Destination[Unit]](dest) ++ Telemetry.noop)
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
    },
    test("stateCompaction: compact runs before and after the connector emits") {
      // Connector adds [3-4] to the existing list. Compaction merges adjacent ranges.
      // Initial: [(1,2),(2,3)]. After resume-compact: [(1,3)]. Connector adds (3,4):
      // [(1,3),(3,4)]. After post-compact: [(1,4)].
      type Range = (Int, Int)
      val mergeAdjacent: List[Range] => List[Range] = ranges =>
        ranges.sortBy(_._1).foldLeft(List.empty[Range]) { (acc, r) =>
          acc match
            case prev :: rest if prev._2 == r._1 => (prev._1, r._2) :: rest
            case _                               => r :: acc
        }.reverse

      val connector: Connector[List[Range], Any, Throwable] =
        Connector.fromStream[List[Range], Any, Throwable]("ranges", "0.1.0") {
          ZStream.fromZIO(ExloState.update[List[Range]](_ :+ ((3, 4))))
        }
      val wrapped = connector @@ StateAspect.stateCompaction[List[Range]](mergeAdjacent)
      for
        dest <- Destination.InMemory.make[List[Range]]
        fiber <- Exlo
                   .run(wrapped, List((1, 2), (2, 3)), SinkConfig.testing)
                   .provideSomeLayer[Any](ZLayer.succeed[Destination[List[Range]]](dest) ++ Telemetry.noop)
                   .fork
        _    <- TestClock.adjust(1.second)
        _    <- fiber.join
        last <- dest.readState
      yield assertTrue(last == Some(List((1, 4))))
    }
  )
