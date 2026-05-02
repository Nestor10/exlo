package exlo.runtime

import exlo.domain.{Emission, ExloError, Stage}
import zio.*
import zio.stream.ZStream

/**
 * Drives a [[Stage]] to completion against a [[DataSink]] and
 * [[StateStore]].
 *
 * StateStore key shape: `(connectorId, stage.id, WatermarkKey)`.
 * `connectorId` is the source identity (e.g., `"brandwatch"`); `stage.id`
 * is the stream's intrinsic name (e.g., `"mentions"`). For chains, parent
 * and child share `connectorId` but have distinct `stage.id` so their
 * state rows don't collide.
 *
 * Lifecycle:
 *   1. Read resume state from `StateStore.readByKey(connectorId, stage.id,
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
 * Error semantics: stage errors propagate as `ExloError`. State commits
 * before the error are kept (at-least-once); records buffered in
 * `groupedWithin` past the last successful sink write are lost — that's
 * correct, they were never durable.
 */
object Runner:

  def run[S, R](
      connectorId: String,
      stage:       Stage[Unit, String, S, R],
      syncId:      String,
      dataSink:    DataSink,
      stateStore:  StateStore,
      flushPolicy: FlushPolicy = FlushPolicy.default
  ): ZIO[R, ExloError, Unit] =
    for
      resume     <- readResume(connectorId, stage, stateStore)
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
            _          <- commitState(connectorId, stage, syncId, stateStore, released)
          yield ()
        }

      // Final advance: catches Marks emitted before any Record (Pending(0, _)).
      // For runs with records, this is a no-op because the last mapZIO already
      // advanced past every emitted Mark.
      finalize = for
        d        <- durableRef.get
        released <- watermark.advance(d)
        _        <- commitState(connectorId, stage, syncId, stateStore, released)
      yield ()

      _ <- pipeline.runDrain.ensuring(
             finalize.tapErrorCause(c => ZIO.logErrorCause("final state commit failed", c)).ignore
           )
    yield ()

  /**
   * Drive a 2-stage chain: parent feeds child, child writes to DataSink.
   *
   * Per-stage commit semantics (Option C, the v0.2 default):
   *
   *   - **Child (leaf)** uses watermark-gated commits against `DataSink`
   *     durability — same as single-stage `run`. Records sequence into the
   *     sink; Marks admit to the child's `WatermarkTracker`; commits release
   *     after `dataSink.write` reports the matching `durableSeq`.
   *   - **Parent** commits eagerly. Each emitted `Mark` writes to StateStore
   *     immediately, regardless of whether downstream records are durable.
   *     With stateless parents (`S0 = Unit`), this is a no-op and never
   *     fires anyway. If a future workload demands gated parent commits
   *     (Option B), the upgrade is to track parent→child seq# mappings in
   *     this loop.
   *
   * Both stages share `connectorId`; their StateStore rows differ via
   * `stage.id` (each stage's intrinsic stream name).
   */
  def runChain[M, S0, S1, R](
      connectorId: String,
      parent:      Stage[Unit, M, S0, R],
      child:       Stage[M, String, S1, R],
      syncId:      String,
      dataSink:    DataSink,
      stateStore:  StateStore,
      flushPolicy: FlushPolicy = FlushPolicy.default
  ): ZIO[R, ExloError, Unit] =
    for
      parentResume    <- readResume(connectorId, parent, stateStore)
      childResume     <- readResume(connectorId, child, stateStore)
      childSeqRef     <- Ref.make(0L)
      childDurableRef <- Ref.make(0L)
      childWatermark  <- WatermarkTracker.make[S1]

      // Parent's emission stream:
      //   Records (typed M) become the child's input.
      //   Marks commit eagerly (Option C). Stateless parents emit no Marks,
      //   so the commit branch never fires in practice.
      parentRecords = parent.run(ZStream.succeed(()), parentResume).mapZIO {
                        case Emission.Record(m) => ZIO.some(m)
                        case Emission.Mark(s)   =>
                          commitState(connectorId, parent, syncId, stateStore,
                                      Chunk.single(Pending(0L, s))).as(None)
                      }.collect { case Some(m) => m }

      // Child's pipeline: Records → DataSink (sequenced/batched);
      //                   Marks → watermark-gated commit.
      childPipeline = child.run(parentRecords, childResume)
        .mapZIO {
          case Emission.Record(s) =>
            childSeqRef.updateAndGet(_ + 1L).map(seq => Some(Sequenced(seq, s)))
          case Emission.Mark(state) =>
            childSeqRef.get
              .flatMap(curSeq => childWatermark.admit(Pending(curSeq, state)))
              .as(None)
        }
        .collect { case Some(s) => s }
        .groupedWithin(flushPolicy.maxRows, flushPolicy.maxInterval)
        .mapZIO { batch =>
          for
            durableSeq <- dataSink.write(batch)
            _          <- childDurableRef.set(durableSeq)
            released   <- childWatermark.advance(durableSeq)
            _          <- commitState(connectorId, child, syncId, stateStore, released)
          yield ()
        }

      childFinalize = for
        d        <- childDurableRef.get
        released <- childWatermark.advance(d)
        _        <- commitState(connectorId, child, syncId, stateStore, released)
      yield ()

      _ <- childPipeline.runDrain.ensuring(
             childFinalize.tapErrorCause(c => ZIO.logErrorCause("final state commit failed", c)).ignore
           )
    yield ()

  private def readResume[S](
      connectorId: String,
      stage:       Stage[?, ?, S, ?],
      stateStore:  StateStore
  ): IO[ExloError, S] =
    stateStore.readByKey(connectorId, stage.id, StateStore.WatermarkKey).flatMap {
      case None      => ZIO.succeed(stage.initialState)
      case Some(row) =>
        stage.codec.decode(row.value) match
          case Right(s) => ZIO.succeed(s)
          case Left(e)  =>
            ZIO.logWarning(
              s"could not decode resume state for $connectorId/${stage.id}, " +
                s"using initial: ${e.getMessage}"
            ).as(stage.initialState)
    }

  private def commitState[S](
      connectorId: String,
      stage:       Stage[?, ?, S, ?],
      syncId:      String,
      stateStore:  StateStore,
      released:    Chunk[Pending[S]]
  ): IO[ExloError, Unit] =
    if released.isEmpty then ZIO.unit
    else
      val foldedNew = released.map(_.state).reduce(stage.reduce)
      for
        prior   <- stateStore.readByKey(connectorId, stage.id, StateStore.WatermarkKey)
        priorS   = prior.fold(stage.initialState)(r =>
                     stage.codec.decode(r.value).getOrElse(stage.initialState)
                   )
        merged   = stage.reduce(priorS, foldedNew)
        encoded  = stage.codec.encode(merged)
        now     <- Clock.instant
        row      = StateRow(connectorId, stage.id, StateStore.WatermarkKey, encoded, now, syncId)
        _       <- stateStore.merge(row)
        _       <- ZIO.logAnnotate("event", "state.commit") {
                     ZIO.logAnnotate("released", released.size.toString) {
                       ZIO.logAnnotate("max_seq", released.map(_.seq).max.toString) {
                         ZIO.logAnnotate("state_value", encoded) {
                           ZIO.logInfo("state committed")
                         }
                       }
                     }
                   }
      yield ()
