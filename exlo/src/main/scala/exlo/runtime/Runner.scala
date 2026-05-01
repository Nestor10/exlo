package exlo.runtime

import exlo.domain.{Emission, ExloError, Stage}
import zio.*
import zio.stream.ZStream

/**
 * Drives a [[Stage]] to completion against a [[DataSink]] and
 * [[StateStore]].
 *
 * Lifecycle:
 *   1. Read resume state from `StateStore.readByKey(stage.id, streamName,
 *      WatermarkKey)`. Fall back to `stage.initialState` on cold start or
 *      decode failure (logged warning).
 *   2. Run `stage.run(ZStream.succeed(()), resume)` and dispatch each
 *      Emission:
 *        - Record(s)   → assign monotonic seq#, forward as `Sequenced(seq, s)`.
 *        - Mark(state) → admit `Pending(currentSeq, state)` to the
 *          [[WatermarkTracker]].
 *   3. Group records via [[FlushPolicy]] (`groupedWithin(maxRows, maxInterval)`)
 *      and on each batch:
 *        - Write to [[DataSink]]; receive `durableSeq`.
 *        - `WatermarkTracker.advance(durableSeq)` → released `Pending`s.
 *        - Read prior committed state, fold released marks via
 *          `stage.reduce`, write the merged row to StateStore.
 *   4. On stream end, run a final advance against the last durable seq. This
 *      catches the edge case of a stage emitting a Mark before any Record
 *      (Pending(0, _)) so the state still commits.
 *
 * This entry point handles the single-stage / leaf case: input is `Unit`
 * (kicked off by `ZStream.succeed(())`), output is `String` (records flow to
 * `DataSink`). Multi-stage chaining helpers will land alongside the first
 * connector that needs them.
 *
 * Error semantics: stage errors propagate as `ExloError`. State commits
 * before the error are kept (at-least-once); records buffered in
 * `groupedWithin` past the last successful sink write are lost — that's
 * correct, they were never durable.
 */
object Runner:

  def run[S, R](
      stage:       Stage[Unit, String, S, R],
      streamName:  String,
      syncId:      String,
      dataSink:    DataSink,
      stateStore:  StateStore,
      flushPolicy: FlushPolicy = FlushPolicy.default
  ): ZIO[R, ExloError, Unit] =
    for
      resume     <- readResume(stage, streamName, stateStore)
      seqRef     <- Ref.make(0L)
      durableRef <- Ref.make(0L)
      watermark  <- WatermarkTracker.make[S]

      pipeline = stage.run(ZStream.succeed(()), resume)
        .mapZIO {
          case Emission.Record(s) =>
            seqRef.updateAndGet(_ + 1L).map(seq => Some(Sequenced(seq, s)))
          case Emission.Mark(state) =>
            seqRef.get
              .flatMap(curSeq => watermark.admit(Pending(curSeq, state)))
              .as(None)
        }
        .collect { case Some(s) => s }
        .groupedWithin(flushPolicy.maxRows, flushPolicy.maxInterval)
        .mapZIO { batch =>
          for
            durableSeq <- dataSink.write(batch)
            _          <- durableRef.set(durableSeq)
            released   <- watermark.advance(durableSeq)
            _          <- commitState(stage, streamName, syncId, stateStore, released)
          yield ()
        }

      // Final advance: catches Marks emitted before any Record (Pending(0, _)).
      // For runs with records, this is a no-op because the last mapZIO already
      // advanced past every emitted Mark.
      finalize = for
        d        <- durableRef.get
        released <- watermark.advance(d)
        _        <- commitState(stage, streamName, syncId, stateStore, released)
      yield ()

      _ <- pipeline.runDrain.ensuring(
             finalize.tapErrorCause(c => ZIO.logErrorCause("final state commit failed", c)).ignore
           )
    yield ()

  private def readResume[S](
      stage:      Stage[?, ?, S, ?],
      streamName: String,
      stateStore: StateStore
  ): IO[ExloError, S] =
    stateStore.readByKey(stage.id, streamName, StateStore.WatermarkKey).flatMap {
      case None      => ZIO.succeed(stage.initialState)
      case Some(row) =>
        stage.codec.decode(row.value) match
          case Right(s) => ZIO.succeed(s)
          case Left(e)  =>
            ZIO.logWarning(
              s"could not decode resume state for ${stage.id}/$streamName, " +
                s"using initial: ${e.getMessage}"
            ).as(stage.initialState)
    }

  private def commitState[S](
      stage:      Stage[?, ?, S, ?],
      streamName: String,
      syncId:     String,
      stateStore: StateStore,
      released:   Chunk[Pending[S]]
  ): IO[ExloError, Unit] =
    if released.isEmpty then ZIO.unit
    else
      val foldedNew = released.map(_.state).reduce(stage.reduce)
      for
        prior   <- stateStore.readByKey(stage.id, streamName, StateStore.WatermarkKey)
        priorS   = prior.fold(stage.initialState)(r =>
                     stage.codec.decode(r.value).getOrElse(stage.initialState)
                   )
        merged   = stage.reduce(priorS, foldedNew)
        encoded  = stage.codec.encode(merged)
        now     <- Clock.instant
        row      = StateRow(stage.id, streamName, StateStore.WatermarkKey, encoded, now, syncId)
        _       <- stateStore.merge(row)
      yield ()
