package exlo.domain

import exlo.runtime.ExloState
import zio.*
import zio.stream.ZStream

/**
 * A `Connector => Connector` transformation. Mirrors zio-http's `Middleware` story but for
 * connectors instead of HTTP routes. Aspects compose via `@@`:
 *
 * {{{
 *   val instrumented = baseConnector @@ Logging @@ Metrics
 * }}}
 *
 * `UpperR` is the upper bound on the connector's required environment. Use `Any` for an
 * aspect that imposes no extra requirements.
 *
 * For aspects whose behavior depends on the connector's state type, see [[StateAspect]].
 */
trait ConnectorAspect[-UpperR]:
  def apply[S, R <: UpperR, E <: Throwable](c: Connector[S, R, E]): Connector[S, R, E]

object ConnectorAspect:

  /** Identity aspect — does nothing. Useful as a default or for testing composition. */
  val identity: ConnectorAspect[Any] = new ConnectorAspect[Any]:
    def apply[S, R, E <: Throwable](c: Connector[S, R, E]): Connector[S, R, E] = c

  /**
   * Log the connector's start and completion. The connector contract emits Unit (effects
   * only), so per-batch counting isn't visible at this layer — that belongs in the sink
   * with its data-fiber instrumentation. Useful as the canonical example of an aspect.
   */
  val logging: ConnectorAspect[Any] = new ConnectorAspect[Any]:
    def apply[S, R, E <: Throwable](c: Connector[S, R, E]): Connector[S, R, E] =
      val cid      = c.id
      val cversion = c.version
      new Connector[S, R, E]:
        def id: String      = cid
        def version: String = cversion
        def emit: ZStream[R & ExloState[S], E, Unit] =
          ZStream.fromZIO(ZIO.logInfo(s"$cid v$cversion: connector start")).drain ++
            c.emit ++
            ZStream.fromZIO(ZIO.logInfo(s"$cid v$cversion: connector done")).drain

/**
 * State-typed counterpart to [[ConnectorAspect]]. Use when the aspect's behavior depends
 * on the connector's state type `S` — e.g. a state-compaction aspect that needs an
 * `S => S` function it can apply to the live state.
 *
 * Cannot be a [[ConnectorAspect]] because that trait quantifies `S` universally inside
 * `apply`, leaving no way to mention a specific `S` at the wrapping site.
 */
trait StateAspect[S, -UpperR]:
  def apply[R <: UpperR, E <: Throwable](c: Connector[S, R, E]): Connector[S, R, E]

object StateAspect:

  /**
   * Apply a pure state-compaction function before and after the connector runs. The
   * canonical use case: a connector accumulates a list of completed/failed slice ranges
   * in its state, and the list grows unbounded unless adjacent ranges are merged.
   *
   * Runs once before the connector emits anything (catches up old representations on
   * resume), and once after (keeps storage small after the run's mutations). Both are
   * plain `ExloState.update` calls — STM-atomic, no extra machinery.
   */
  def stateCompaction[S: Tag](compact: S => S): StateAspect[S, Any] =
    new StateAspect[S, Any]:
      def apply[R, E <: Throwable](c: Connector[S, R, E]): Connector[S, R, E] =
        val cid      = c.id
        val cversion = c.version
        new Connector[S, R, E]:
          def id: String      = cid
          def version: String = cversion
          def emit: ZStream[R & ExloState[S], E, Unit] =
            ZStream.fromZIO(ExloState.update[S](compact)).drain ++
              c.emit ++
              ZStream.fromZIO(ExloState.update[S](compact)).drain

/** `@@` operator for applying an aspect to a connector. */
extension [S, R, E <: Throwable](c: Connector[S, R, E])
  def @@(aspect: ConnectorAspect[R]): Connector[S, R, E] =
    aspect.apply[S, R, E](c)

  def @@(aspect: StateAspect[S, R]): Connector[S, R, E] =
    aspect.apply[R, E](c)
