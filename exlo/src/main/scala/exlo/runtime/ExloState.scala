package exlo.runtime

import zio.*
import zio.stm.*

/**
 * Env service the connector uses for output.
 *
 *   - [[emit]] enqueues records onto the shared queue. The sink fiber drains continuously,
 *     keeping memory bounded.
 *   - [[update]] mutates state atomically against the shared `TRef[S]`. Concurrent slice
 *     fibers serialize via STM; no lost updates.
 *   - [[current]] reads the in-memory state.
 *
 * No `commit` method: with atomic destination commits, every state value lands in the same
 * transaction as the records that motivated it. Maintenance compactions (collapsing ranges,
 * dropping done slices) are just regular `update` calls; the next destination commit writes
 * the compacted form.
 */
trait ExloState[S]:
  def current: UIO[S]
  def emit(records: Chunk[String]): UIO[Unit]
  def update(f: S => S): UIO[Unit]

object ExloState:

  // ---- accessor helpers --------------------------------------------------------------------

  def current[S: Tag]: URIO[ExloState[S], S] =
    ZIO.serviceWithZIO[ExloState[S]](_.current)

  def emit[S: Tag](records: Chunk[String]): URIO[ExloState[S], Unit] =
    ZIO.serviceWithZIO[ExloState[S]](_.emit(records))

  def update[S: Tag](f: S => S): URIO[ExloState[S], Unit] =
    ZIO.serviceWithZIO[ExloState[S]](_.update(f))

  /**
   * Live impl. The `TRef[S]` and `TQueue[String]` are both held by the [[Sink]]; ExloState
   * is just the connector-facing surface over them. `update` is fully atomic STM —
   * concurrent slice fibers' updates serialize, no lost mutations.
   */
  final class Live[S](
      private[runtime] val currentState: TRef[S],
      private[runtime] val recordQueue: TQueue[String]
  ) extends ExloState[S]:

    def current: UIO[S] = currentState.get.commit

    def emit(records: Chunk[String]): UIO[Unit] =
      // Each record is its own tiny STM transaction. One big transaction with N offers
      // suffers retry cascades under contention (many slice fibers each running a 5-offer
      // tx); per-record commits keep contention small and transactions short.
      ZIO.foreachDiscard(records)(r => recordQueue.offer(r).commit)

    def update(f: S => S): UIO[Unit] =
      currentState.update(f).commit

  // ---- Test impl: infra-free, no Sink, no queue ------------------------------------------

  /**
   * Infra-free test impl. Backed by plain `Ref`s; ignores the runtime machinery. Useful for
   * testing connector logic in isolation: assert what was emitted, what state values were
   * passed through update.
   */
  final class Test[S](
      ref: Ref[S],
      private val emittedRef: Ref[Chunk[String]],
      private val updatesRef: Ref[Chunk[S]]
  ) extends ExloState[S]:
    def current: UIO[S] = ref.get
    def emit(records: Chunk[String]): UIO[Unit] = emittedRef.update(_ ++ records)
    def update(f: S => S): UIO[Unit] =
      ref.updateAndGet(f).flatMap(s => updatesRef.update(_ :+ s))

    def emitted: UIO[Chunk[String]] = emittedRef.get
    def updates: UIO[Chunk[S]]      = updatesRef.get

  object Test:
    def make[S](initial: S): UIO[Test[S]] =
      for
        ref <- Ref.make(initial)
        e   <- Ref.make(Chunk.empty[String])
        u   <- Ref.make(Chunk.empty[S])
      yield new Test[S](ref, e, u)

    def layer[S: Tag](initial: => S): ULayer[ExloState[S] & Test[S]] =
      ZLayer.fromZIOEnvironment {
        make[S](initial).map(impl => ZEnvironment[ExloState[S]](impl).add[Test[S]](impl))
      }
