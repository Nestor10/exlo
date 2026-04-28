package exlo.runtime

import zio.*
import zio.test.*

object ExloStateSpec extends ZIOSpecDefault:

  final case class S(n: Int, msg: String)

  def spec = suite("ExloState")(
    test("Test impl: update mutates current; emit captures records") {
      for
        st <- ExloState.Test.make(S(0, "init"))
        _  <- st.emit(Chunk("a", "b"))
        _  <- st.update(s => s.copy(n = s.n + 1))
        _  <- st.update(_.copy(msg = "x"))
        cur     <- st.current
        emitted <- st.emitted
        updates <- st.updates
      yield assertTrue(
        cur == S(1, "x"),
        emitted == Chunk("a", "b"),
        updates == Chunk(S(1, "init"), S(1, "x"))
      )
    },
    test("Live impl: many concurrent updates are STM-serialized; no lost increments") {
      for
        dest <- Destination.InMemory.make[Int]
        pair <- Sink.make[Int](0, dest, SinkConfig.testing)
        (_, live) = pair
        _ <- ZIO.foreachParDiscard(1 to 1000)(_ => live.update(_ + 1))
        n <- live.current
      yield assertTrue(n == 1000)
    }
  )
