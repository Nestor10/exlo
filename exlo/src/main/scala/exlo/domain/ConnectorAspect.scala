package exlo.domain

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
        def id: String                                     = cid
        def version: String                                = cversion
        def emit: ZStream[R & exlo.runtime.ExloState[S], E, Unit] =
          ZStream.fromZIO(ZIO.logInfo(s"$cid v$cversion: connector start")).drain ++
            c.emit ++
            ZStream.fromZIO(ZIO.logInfo(s"$cid v$cversion: connector done")).drain

/** `@@` operator for applying an aspect to a connector. */
extension [S, R, E <: Throwable](c: Connector[S, R, E])
  def @@(aspect: ConnectorAspect[R]): Connector[S, R, E] =
    aspect.apply[S, R, E](c)
