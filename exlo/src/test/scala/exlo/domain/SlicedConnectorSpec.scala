package exlo.domain

import exlo.Exlo
import exlo.runtime.{Destination, ExloState, SinkConfig, Telemetry}
import zio.*
import zio.stream.ZStream
import zio.test.*
import zio.test.TestAspect.*

object SlicedConnectorSpec extends ZIOSpecDefault:

  final case class State(done: Set[Int])
  final case class Slice(id: Int)

  /** Test connector: emits `recordsPerSlice` records per slice, then marks slice done. */
  final class CountingSlicedConnector(numSlices: Int, recordsPerSlice: Int, par: Int)
      extends SlicedConnector[Slice, State, Any, Throwable]:
    def id: String                = "counting-sliced"
    def version: String           = "0.1.0"
    override def parallelism: Int = par

    def slices(state: State): ZStream[Any, Throwable, Slice] =
      ZStream
        .fromIterable(1 to numSlices)
        .filterNot(state.done.contains)
        .map(Slice(_))

    def extract(slice: Slice): ZStream[ExloState[State], Throwable, Unit] =
      ZStream.fromZIO {
        val recs = Chunk.fromIterable((1 to recordsPerSlice).map(i => s"slice-${slice.id}-r-$i"))
        ExloState.emit[State](recs) *>
          ExloState.update[State](s => s.copy(done = s.done + slice.id))
      }

  def spec = suite("SlicedConnector")(
    test("parallel slices: every record lands, state collects every slice's completion marker") {
      val cfg = SinkConfig(maxRecords = 10, maxInterval = 50.millis, bufferCapacity = 10_000)
      for
        dest <- Destination.InMemory.make[State]
        connector = new CountingSlicedConnector(numSlices = 8, recordsPerSlice = 5, par = 4)
        fiber <- Exlo
                   .run(connector.toConnector, State(Set.empty), cfg)
                   .provideSomeLayer[Any](ZLayer.succeed[Destination[State]](dest) ++ Telemetry.noop)
                   .fork
        _    <- TestClock.adjust(2.seconds)
        _    <- fiber.join
        all  <- dest.allRecords
        snap <- dest.readState
      yield assertTrue(
        all.length == 8 * 5,
        all.toSet.size == 8 * 5,
        snap.exists(_.done == (1 to 8).toSet)
      )
    } @@ nonFlaky(10),
    test("slices already marked done are skipped on resume") {
      val cfg = SinkConfig(maxRecords = 10, maxInterval = 50.millis, bufferCapacity = 10_000)
      for
        dest <- Destination.InMemory.seeded[State](
                  Chunk(Destination.InMemory.Snapshot(Chunk.empty, State(done = Set(1, 2, 3))))
                )
        connector = new CountingSlicedConnector(numSlices = 5, recordsPerSlice = 5, par = 4)
        fiber <- Exlo
                   .run(connector.toConnector, State(Set.empty), cfg)
                   .provideSomeLayer[Any](ZLayer.succeed[Destination[State]](dest) ++ Telemetry.noop)
                   .fork
        _    <- TestClock.adjust(2.seconds)
        _    <- fiber.join
        all  <- dest.allRecords
        snap <- dest.readState
      yield assertTrue(
        all.length == 2 * 5,
        all.forall(r => r.startsWith("slice-4-") || r.startsWith("slice-5-")),
        snap.exists(_.done == Set(1, 2, 3, 4, 5))
      )
    },
    test("concurrent state updates from many slices are STM-correct (no lost markers)") {
      val cfg = SinkConfig(maxRecords = 50, maxInterval = 50.millis, bufferCapacity = 10_000)
      for
        dest <- Destination.InMemory.make[State]
        connector = new CountingSlicedConnector(numSlices = 32, recordsPerSlice = 3, par = 16)
        fiber <- Exlo
                   .run(connector.toConnector, State(Set.empty), cfg)
                   .provideSomeLayer[Any](ZLayer.succeed[Destination[State]](dest) ++ Telemetry.noop)
                   .fork
        _    <- TestClock.adjust(2.seconds)
        _    <- fiber.join
        snap <- dest.readState
      yield assertTrue(snap.exists(_.done == (1 to 32).toSet))
    } @@ nonFlaky(10)
  )
