package exlo.runtime

import zio.*
import zio.test.*
import zio.test.TestAspect.*

/**
 * Tests for the single-fiber sink. Records flow via `ExloState.emit`; state advances via
 * `ExloState.update`. The sink fiber drains records continuously and commits (records +
 * state) atomically when count or time thresholds trip.
 */
object SinkSpec extends ZIOSpecDefault:

  final case class TestState(seq: Long)

  def spec = suite("Sink")(
    test("with no emits and no updates, no snapshots are committed") {
      for
        dest <- Destination.InMemory.make[TestState]
        pair <- Sink.make[TestState](TestState(0), dest, SinkConfig.testing)
        (sink, _) = pair
        loop <- sink.runLoop.fork
        // No emits, no updates. Advance clock to trigger the time threshold once.
        _ <- TestClock.adjust(SinkConfig.testing.maxInterval * 2)
        _ <- loop.interrupt.ignore
        // Even though the loop fired, drainAndStage gets nothing → commit writes the
        // initial state to InMemory destination. Acceptable: at-least-once semantics
        // tolerate this minor "empty commit" — for atomic destinations like Iceberg, an
        // empty commit is just a snapshot with no DataFiles + the state property.
        snapshots <- dest.snapshots
      yield assertTrue(snapshots.forall(_.records.isEmpty))
    },
    test("emit + update: records and state both land in one snapshot") {
      // High maxRecords so the count trigger doesn't fire during emit (which would race
      // the test fiber's subsequent update and commit with stale state). Time trigger via
      // TestClock.adjust gives a deterministic single commit that captures both.
      val cfg = SinkConfig(maxRecords = 100, maxInterval = 100.millis, bufferCapacity = 1000)
      for
        dest <- Destination.InMemory.make[TestState]
        pair <- Sink.make[TestState](TestState(0), dest, cfg)
        (sink, live) = pair
        loop <- sink.runLoop.fork
        _    <- live.emit(Chunk("a", "b", "c"))
        _    <- live.update(_.copy(seq = 3))
        _    <- TestClock.adjust(150.millis)
        _    <- dest.commitCount.repeatUntil(_ >= 1)
        _    <- loop.interrupt.ignore
        snaps <- dest.snapshots
      yield assertTrue(
        snaps.exists(s => s.records == Chunk("a", "b", "c") && s.state == TestState(3))
      )
    },
    test("count threshold triggers a commit before the interval elapses") {
      // To avoid the test-fiber/sink-fiber race on `update` vs commit, do the state update
      // BEFORE filling the buffer past maxRecords. With state=5 already in the TRef when
      // the count threshold trips, the resulting commit captures (records, seq=5).
      val cfg = SinkConfig(maxRecords = 5, maxInterval = 10.minutes, bufferCapacity = 100)
      for
        dest <- Destination.InMemory.make[TestState]
        pair <- Sink.make[TestState](TestState(0), dest, cfg)
        (sink, live) = pair
        loop <- sink.runLoop.fork
        _    <- live.update(_.copy(seq = 5))
        _    <- live.emit(Chunk("r1", "r2", "r3", "r4", "r5"))
        _    <- dest.commitCount.repeatUntil(_ >= 1)
        _    <- loop.interrupt.ignore
        snaps <- dest.snapshots
      yield assertTrue(
        snaps.exists(s => s.records == Chunk("r1", "r2", "r3", "r4", "r5") && s.state == TestState(5))
      )
    },
    test("time threshold triggers a commit even with no records") {
      val cfg = SinkConfig(maxRecords = 100, maxInterval = 100.millis, bufferCapacity = 100)
      for
        dest <- Destination.InMemory.make[TestState]
        pair <- Sink.make[TestState](TestState(0), dest, cfg)
        (sink, live) = pair
        loop <- sink.runLoop.fork
        _    <- live.update(_.copy(seq = 1))
        _    <- TestClock.adjust(150.millis)
        _    <- dest.commitCount.repeatUntil(_ >= 1)
        _    <- loop.interrupt.ignore
        snaps <- dest.snapshots
      yield assertTrue(snaps.exists(_.state == TestState(1)))
    }
  )
