package exlo.domain

import exlo.runtime.Codec
import zio.stream.ZStream

/**
 * A node in a connector pipeline.
 *
 * A stage is a *value*, not a service. Given a stream of input records and a
 * resume value, it produces a stream of output records and resume-point
 * proposals. The runner orchestrates: it pipes Records to the next stage (or
 * to a `DataSink` if leaf), and routes Marks through that stage's watermark
 * tracker into the StateStore.
 *
 * Type parameters:
 *
 *   - `I`: input record type. Roots have `I = Unit` (the runner feeds a
 *     single-element kick-off stream). Inner/leaf stages have `I` equal to
 *     the upstream stage's `O`.
 *   - `O`: output record type. Inter-stage outputs can be any user type;
 *     the leaf stage that writes to a `DataSink` must have `O = String`.
 *   - `S`: state shape. Encoded for `StateStore` via [[codec]]; combined
 *     across released marks via [[reduce]].
 *   - `R`: env requirement.
 *
 * Variance follows ZStream: contravariant input, covariant output. The
 * effect channel is fixed to [[ExloError]] end-to-end.
 *
 * Five things a stage author provides:
 *
 *   - [[id]] / [[version]]: stable identity for logs, metrics, and StateStore
 *     key prefixes.
 *   - [[initialState]]: cold-start state when nothing is committed.
 *   - [[reduce]]: a left-fold semigroup on `S`. Released marks are folded in
 *     seq order; the result is then folded against prior committed state.
 *     Must be associative under left-fold; commutativity is not required.
 *     Examples: cursor / watermark — `(prev, next) => if (next > prev) next
 *     else prev`; last-write-wins — `(_, next) => next`; per-key map merge —
 *     `(a, b) => a ++ b`.
 *   - [[codec]]: serializes `S` for the StateStore.
 *   - [[run]]: produces `Emission[O, S]` given an input stream and resume.
 *     Pure value; testable by `runCollect`-ing the resulting stream against
 *     a synthetic input stream.
 */
trait Stage[-I, +O, S, -R]:

  /** This stage's intrinsic name — the "stream" within a connector
   *  (e.g., `"queries"`, `"mentions"`). Used as the `stream` slot in
   *  StateStore keys, in S3 path `stream=<id>` segments, and in
   *  log/trace annotations. NOT prefixed with the connector id; the
   *  connector id is a separate runtime concern carried through
   *  [[exlo.Exlo.run]]/`runChain` and `RunContext.connectorId`. */
  def id: String

  /** Semantic version; used for debugging and reproducibility. */
  def version: String

  /** Cold-start state when nothing is committed in the StateStore. */
  def initialState: S

  /**
   * Combine prior committed state with a newly released mark. Must be
   * associative under left-fold; commutativity is not required because the
   * runner always folds in seq order.
   */
  def reduce(prev: S, next: S): S

  /** Encoder/decoder used by the StateStore to persist `S`. */
  def codec: Codec[S]

  /**
   * Produce records and resume-point marks. The runner reads `resume` from
   * the StateStore on start (falling back to [[initialState]] on cold start)
   * and feeds the `input` stream — `ZStream.succeed(())` for roots, the
   * upstream stage's record stream otherwise.
   *
   * `R0` is the input stream's env; method-local so the class can keep
   * `-R` contravariance. The result needs `R & R0` (this stage's env plus
   * whatever the upstream stream brought along). For roots the input is
   * `ZStream[Any, ...]` so `R & Any = R`; for chains the upstream stage
   * typically shares this stage's `R` (e.g., both need `HttpExec`) and
   * `R & R = R`.
   */
  def run[R0](
      input:  ZStream[R0, ExloError, I],
      resume: S
  ): ZStream[R & R0, ExloError, Emission[O, S]]
