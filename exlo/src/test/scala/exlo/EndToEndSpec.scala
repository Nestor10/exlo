package exlo

import exlo.domain.Connector
import exlo.runtime.{Destination, ExloState, SinkConfig}
import zio.*
import zio.stream.ZStream
import zio.test.*

object EndToEndSpec extends ZIOSpecDefault:

  final case class CounterState(seq: Long)

  /** Connector that emits N batches of M records, advancing state.seq per batch. */
  final class CountingConnector(batches: Int, perBatch: Int)
      extends Connector[CounterState, Any, Throwable]:
    def id: String      = "counting"
    def version: String = "0.1.0"

    def emit: ZStream[ExloState[CounterState], Throwable, Unit] =
      ZStream
        .fromIterable(1 to batches)
        .mapZIO { batchNum =>
          val records = Chunk.fromIterable((1 to perBatch).map(i => s"batch-$batchNum-rec-$i"))
          ExloState.emit[CounterState](records) *>
            ExloState.update[CounterState](_.copy(seq = batchNum.toLong))
        }

  def spec = suite("end-to-end")(
    test("connector emits records, sink commits to destination, state advances") {
      val cfg = SinkConfig(maxRecords = 5, maxInterval = 50.millis, bufferCapacity = 1000)
      for
        dest <- Destination.InMemory.make[CounterState]
        fiber <- Exlo
                   .run(new CountingConnector(batches = 4, perBatch = 5), CounterState(0), cfg)
                   .provideSomeLayer[Any](ZLayer.succeed[Destination[CounterState]](dest))
                   .fork
        _    <- TestClock.adjust(500.millis)
        _    <- fiber.join
        all  <- dest.allRecords
        snap <- dest.readState
      yield assertTrue(
        all.length == 20,
        snap == Some(CounterState(seq = 4))
      )
    }
  )
