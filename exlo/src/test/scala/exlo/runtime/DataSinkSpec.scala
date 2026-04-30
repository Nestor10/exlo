package exlo.runtime

import zio.*
import zio.test.*

object DataSinkSpec extends ZIOSpecDefault:

  def spec = suite("DataSink.InMemory")(
    test("write returns highest seq# in batch") {
      for
        sink <- DataSink.InMemory.make
        seq  <- sink.write(Chunk(Sequenced(1, "a"), Sequenced(3, "c"), Sequenced(2, "b")))
      yield assertTrue(seq == 3L)
    },
    test("write accumulates records in write order") {
      for
        sink <- DataSink.InMemory.make
        _    <- sink.write(Chunk(Sequenced(1, "a"), Sequenced(2, "b")))
        _    <- sink.write(Chunk(Sequenced(3, "c")))
        all  <- sink.collected
      yield assertTrue(
        all.toList == List(Sequenced(1, "a"), Sequenced(2, "b"), Sequenced(3, "c"))
      )
    },
    test("durable watermark is the running max across writes") {
      for
        sink   <- DataSink.InMemory.make
        d0     <- sink.durable
        _      <- sink.write(Chunk(Sequenced(5, "e")))
        d1     <- sink.durable
        _      <- sink.write(Chunk(Sequenced(3, "c"))) // out-of-order seq doesn't lower durable
        d2     <- sink.durable
      yield assertTrue(d0 == 0L, d1 == 5L, d2 == 5L)
    },
    test("empty batch returns current durable, doesn't change records") {
      for
        sink   <- DataSink.InMemory.make
        _      <- sink.write(Chunk(Sequenced(7, "g")))
        result <- sink.write(Chunk.empty)
        all    <- sink.collected
      yield assertTrue(
        result == 7L,
        all.toList == List(Sequenced(7, "g"))
      )
    }
  )
