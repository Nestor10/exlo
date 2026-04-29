package exlo.runtime

import exlo.domain.ExloError
import zio.*
import zio.stm.*
import zio.telemetry.opentelemetry.core.trace.Tracer

/**
 * Single drain-and-commit fiber.
 *
 * Workers emit records via `ExloState.emit` (queued) and update state via `ExloState.update`
 * (TRef). The sink fiber:
 *   1. Drains the record queue, calls `destination.writeRecords(batch)` — staging records to
 *      the destination so memory doesn't accumulate.
 *   2. When the count or time threshold trips, reads `currentState` and calls
 *      `destination.commit(state)` — one atomic transaction (for Iceberg-class destinations)
 *      or ordered writes (for at-least-once destinations).
 *
 * No watermark, no two-fiber coordination — atomic commits at the destination layer make
 * data and state durable together by construction.
 */
final class Sink[S](
    recordQueue: TQueue[String],
    currentState: TRef[S],
    destination: Destination[S],
    config: SinkConfig
):

  /** Block until either the queue has at least `maxRecords` items, OR there are any items
   *  to drain when called explicitly (used by the loop's outer timeout race). */
  private val waitForCount: STM[Nothing, Unit] =
    for
      size <- recordQueue.size
      _    <- STM.check(size >= config.maxRecords)
    yield ()

  /** Drain everything currently in the queue and stage it at the destination. */
  private val drainAndStage: IO[ExloError, Int] =
    for
      items <- recordQueue.takeAll.commit
      chunk = Chunk.fromIterable(items)
      _ <- ZIO.when(chunk.nonEmpty)(destination.writeRecords(chunk))
    yield chunk.length

  /** Commit pending records + current state atomically. Spanned so commit latency is visible. */
  private val commitNow: ZIO[Tracer, ExloError, Unit] =
    ZIO.serviceWithZIO[Tracer] { tracer =>
      tracer.span("sink.commit") { _ =>
        currentState.get.commit.flatMap(destination.commit)
      }
    }

  /**
   * One tick of the sink loop:
   *   1. Wait for count threshold OR interval timeout — whichever fires first.
   *   2. Drain any queued records into the destination's staging area.
   *   3. Commit (records + state) atomically.
   *
   * Wrapped in `uninterruptible` from the drain onward so a torn cycle can't lose records
   * that have been taken from the queue but not yet handed to the destination.
   */
  private val tick: ZIO[Tracer, ExloError, Unit] =
    waitForCount.commit.timeout(config.maxInterval) *>
      ZIO.uninterruptible {
        drainAndStage.flatMap { drained =>
          // Only commit if there were records to commit, OR enough time has elapsed that we
          // want to capture state advances. Simplification for v0.2: always commit on tick.
          commitNow
        }
      }

  /** Long-running fiber: ticks forever until interrupted. */
  def runLoop: ZIO[Tracer, ExloError, Unit] = tick.forever

  /** Final flush at run end: drain any remaining records, then commit. */
  def shutdown: ZIO[Tracer, ExloError, Unit] =
    ZIO.uninterruptible {
      drainAndStage *> commitNow
    }

object Sink:

  /**
   * Build the [[Sink]] together with the matching [[ExloState.Live]] env service. Reads
   * the resume state from the destination; falls back to `initialState` on cold start.
   */
  def make[S](
      initialState: S,
      destination: Destination[S],
      config: SinkConfig = SinkConfig.default
  ): IO[ExloError, (Sink[S], ExloState[S])] =
    for
      loaded <- destination.readState.map(_.getOrElse(initialState))
      queue  <- TQueue.bounded[String](config.bufferCapacity).commit
      ref    <- TRef.make(loaded).commit
    yield
      val sink         = new Sink[S](queue, ref, destination, config)
      val stateService = new ExloState.Live[S](ref, queue)
      (sink, stateService)
