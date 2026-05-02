package exlo.runtime

import exlo.domain.{Emission, ExloError, Stage}
import zio.*
import zio.stream.ZStream
import zio.test.*

import java.time.Instant

object RunnerSpec extends ZIOSpecDefault:

  // ----- test fixtures ----------------------------------------------------

  /** Synthetic Long-state leaf stage. `reduce` is `max`. */
  private def stage(
      streamId:  String,
      emissions: ZStream[Any, ExloError, Emission[String, Long]],
      initial:   Long = 0L,
      onResume:  Long => UIO[Unit] = _ => ZIO.unit
  ): Stage[Unit, String, Long, Any] =
    new Stage[Unit, String, Long, Any]:
      val id           = streamId
      val version      = "1.0.0"
      val initialState = initial
      def reduce(a: Long, b: Long): Long = a max b
      val codec        = Codec.long
      def run[R0](
          input:  ZStream[R0, ExloError, Unit],
          resume: Long
      ): ZStream[Any & R0, ExloError, Emission[String, Long]] =
        ZStream.fromZIO(onResume(resume)).drain ++ emissions

  private val fastFlush = FlushPolicy(maxRows = 100, maxInterval = 100.millis)

  // ----- specs ------------------------------------------------------------

  def spec = suite("Runner")(
    test("records flow to the sink in order; no marks ⇒ no state commit") {
      val emissions = ZStream.fromIterable(
        (1 to 5).map(i => Emission.Record(s"r$i"))
      )
      for
        sink   <- DataSink.InMemory.make
        store  <- StateStore.InMemory.make
        _      <- Runner.run("c1", stage("s1", emissions), "sync-1", sink, store, fastFlush)
        rows   <- sink.collected
        sz     <- store.size
      yield assertTrue(
        rows.map(_.value).toList == List("r1", "r2", "r3", "r4", "r5"),
        sz == 0
      )
    },
    test("a single Mark commits state via reduce(initial, mark)") {
      val emissions = ZStream.fromIterable(List(
        Emission.Record("r1"),
        Emission.Record("r2"),
        Emission.Mark(7L)
      ))
      for
        sink   <- DataSink.InMemory.make
        store  <- StateStore.InMemory.make
        _      <- Runner.run("c1", stage("s1", emissions), "sync-1", sink, store, fastFlush)
        got    <- store.readByKey("c1", "s1", StateStore.WatermarkKey)
      yield assertTrue(got.exists(_.value == "7"))
    },
    test("multiple Marks reduce to the max under the stage's reduce") {
      val emissions = ZStream.fromIterable(List(
        Emission.Record("r1"),
        Emission.Mark(3L),
        Emission.Record("r2"),
        Emission.Mark(10L),
        Emission.Record("r3"),
        Emission.Mark(5L)  // less than 10; max(10, 5) = 10
      ))
      for
        sink   <- DataSink.InMemory.make
        store  <- StateStore.InMemory.make
        _      <- Runner.run("c1", stage("s1", emissions), "sync-1", sink, store, fastFlush)
        got    <- store.readByKey("c1", "s1", StateStore.WatermarkKey)
      yield assertTrue(got.exists(_.value == "10"))
    },
    test("resume: prior committed state is passed to run") {
      val emissions = ZStream.empty
      for
        seen   <- Promise.make[Nothing, Long]
        sink   <- DataSink.InMemory.make
        store  <- StateStore.InMemory.make
        seed    = StateRow("c1", "s1", StateStore.WatermarkKey, "42",
                           Instant.parse("2026-01-01T00:00:00Z"), "prior-sync")
        _      <- store.merge(seed)
        _      <- Runner.run(
                    "c1",
                    stage("s1", emissions, onResume = v => seen.succeed(v).unit),
                    "sync-2", sink, store, fastFlush
                  )
        got    <- seen.await
      yield assertTrue(got == 42L)
    },
    test("Mark before any Record: state still commits via final advance") {
      val emissions = ZStream.fromIterable(List(
        Emission.Mark(99L)
      ))
      for
        sink   <- DataSink.InMemory.make
        store  <- StateStore.InMemory.make
        _      <- Runner.run("c1", stage("s1", emissions), "sync-1", sink, store, fastFlush)
        got    <- store.readByKey("c1", "s1", StateStore.WatermarkKey)
      yield assertTrue(got.exists(_.value == "99"))
    },
    test("empty stream: no records, no commits, no error") {
      for
        sink   <- DataSink.InMemory.make
        store  <- StateStore.InMemory.make
        _      <- Runner.run("c1", stage("s1", ZStream.empty), "sync-1", sink, store, fastFlush)
        rows   <- sink.collected
        sz     <- store.size
      yield assertTrue(rows.isEmpty, sz == 0)
    },
    test("subsequent run reduces the prior committed state with new marks") {
      val firstRun  = ZStream.fromIterable(List(
        Emission.Record("r1"),
        Emission.Mark(3L)
      ))
      val secondRun = ZStream.fromIterable(List(
        Emission.Record("r2"),
        Emission.Mark(7L)
      ))
      for
        sink   <- DataSink.InMemory.make
        store  <- StateStore.InMemory.make
        _      <- Runner.run("c1", stage("s1", firstRun),  "sync-1", sink, store, fastFlush)
        _      <- Runner.run("c1", stage("s1", secondRun), "sync-2", sink, store, fastFlush)
        got    <- store.readByKey("c1", "s1", StateStore.WatermarkKey)
      yield assertTrue(got.exists(_.value == "7"))
    }
  )
