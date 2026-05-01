package exlo.domain

import exlo.runtime.Codec
import zio.stream.ZStream

/**
 * A source's description of what it produces and how to resume from it.
 *
 * A connector is a value, not a service. Its only behavior is to yield a
 * `ZStream` of [[Emission]]s given a resume point. It does not stage records,
 * mutate state stores, or commit anything — those are the runner's concerns.
 * No env-service plumbing, no `Throwable` in the error channel, no side
 * channels for output.
 *
 * Type parameters:
 *   - [[O]]: phantom tag identifying this connector's output. Lets a child
 *     connector consume from several parents at once via distinct
 *     `Source[T <: Tag]` services. `O` is a type parameter (not a type member)
 *     so that ZIO's runtime `Tag` derivation can see it through the signature
 *     — `Source[parent.Out]` would be path-dependent and unreachable to the
 *     env-lookup machinery. Use `Tag` directly (the upper bound) for leaf
 *     connectors that aren't intended to feed any child.
 *   - `S`: state shape. Encoded for StateStore via [[codec]]; combined across
 *     marks via [[reduce]].
 *   - `R`: env requirement.
 *
 * Five things a connector author provides:
 *
 *   - [[initialState]]: cold-start state, used by the runner when nothing is
 *     committed in the StateStore.
 *   - [[reduce]]: a semigroup on `S`, used at every flush boundary to fold
 *     released marks against the prior committed state. Must be associative
 *     under left-fold (marks arrive in seq order). Examples:
 *       - cursor / watermark: `(prev, next) => if (next > prev) next else prev`
 *       - last-write-wins:    `(_, next) => next`
 *       - per-key map merge:  `(a, b) => a ++ b`
 *   - [[codec]]: serializes `S` for the StateStore.
 *   - [[dataStream]]: produces records and resume-point marks given a resume
 *     point. Pure value; testable by `runCollect`-ing the stream.
 */
trait Connector[+O <: Tag, S, -R]:

  /** Stable identifier; used for logs, metrics, and StateStore key prefixes. */
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
   * and consumes the resulting stream end-to-end.
   */
  def dataStream(resume: S): ZStream[R, ExloError, Emission[S]]
