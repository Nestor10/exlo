package exlo.domain

import exlo.runtime.ExloState
import zio.stream.ZStream

/**
 * A connector emits a stream of *effects* — records flow out via `ExloState.emit`, state
 * advances via `ExloState.update`, both accessed through the env service. The stream's
 * element type is `Unit` because there is nothing for a downstream consumer to do; the
 * framework's runner just calls `runDrain` to execute.
 *
 * `R` flows through unchanged; users provide their own services via `.provide` at the
 * bootstrap boundary. The framework adds itself to the environment via intersection — it
 * never owns or captures the user's environment.
 *
 * Errors `E` propagate up to [[exlo.Exlo.run]]; they must extend `Throwable` so framework
 * I/O errors and connector errors flow through the same channel.
 */
trait Connector[S, -R, +E <: Throwable]:

  /** Stable identifier for this connector. Used in logs and metrics. */
  def id: String

  /** Semantic version of the connector. Used for debugging and reproducibility. */
  def version: String

  /**
   * Emit records and advance state. Output flows entirely through the [[ExloState]] env
   * service:
   *   - `ExloState.emit(records)` enqueues records (the framework assigns IDs).
   *   - `ExloState.update(f)` advances state, capturing the current high-water mark
   *     atomically so the sink writes state only when the corresponding records are durable.
   *   - `ExloState.commit(s)` writes state directly (maintenance escape hatch).
   */
  def emit: ZStream[R & ExloState[S], E, Unit]

object Connector:

  /**
   * Construct a connector from an existing effect-stream. Useful when the stream is built
   * elsewhere (e.g. by a builder) or when the connector is small enough that defining a
   * named class adds noise.
   */
  def fromStream[S, R, E <: Throwable](
      id: String,
      version: String
  )(stream: => ZStream[R & ExloState[S], E, Unit]): Connector[S, R, E] =
    val (cid, cversion) = (id, version)
    new Connector[S, R, E]:
      def id: String                                       = cid
      def version: String                                  = cversion
      def emit: ZStream[R & ExloState[S], E, Unit]        = stream

  /**
   * Stateless connector. The framework still tracks `Unit` state for resume bookkeeping,
   * but the connector never reads or writes it.
   */
  def stateless[R, E <: Throwable](
      id: String,
      version: String
  )(stream: => ZStream[R & ExloState[Unit], E, Unit]): Connector[Unit, R, E] =
    fromStream[Unit, R, E](id, version)(stream)
