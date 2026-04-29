package exlo.domain

/**
 * Outcome of an HTTP-level error handler. Lets a connector author convert a specific
 * class of failure into a state mutation and continue the walk, instead of failing the
 * whole stream.
 *
 * Use case (the canonical one): a GraphQL or cursor-paginated source with hard-deleted
 * records. Hitting a poisoned range returns a deterministic server error; the connector
 * narrows the request window in state, returns `Continue(narrowedState)`, and the
 * framework re-issues `request(state)` against the new (smaller) window. After enough
 * narrowing, an inner threshold trips and the handler returns
 * `Continue(state.markFailed(slice))` to skip past it permanently.
 *
 * `Continue(s')` re-enters the loop at the request stage with `s'` — i.e. the next
 * iteration calls `request(s')`. There is no parsed page on the failure path, so
 * `records`, `nextRequest`, and `advance` are skipped for this iteration.
 *
 * `Fail` propagates the original error up to the runtime, ending the stream. This is
 * the default for any error not matched by the connector's `.recover` partial.
 */
sealed trait RecoveryAction[+S]

object RecoveryAction:
  final case class Continue[S](newState: S) extends RecoveryAction[S]
  case object Fail                          extends RecoveryAction[Nothing]
