# Adaptive slicing

When the slice factory needs to react to mid-run feedback — narrowing windows after a
poison-pill error, splitting a slice into halves, marking ranges as failed — write the
factory so it reads live state on every pull instead of treating its input parameter as
the source of truth.

## The thing to know

`HttpSlice[Slice, S].slices(f)` takes `f: S => ZStream[Any, E, Slice]`. The runtime
evaluates `f` once at the start of the run with the state at that moment. The returned
stream is then drained.

If your factory closes over the parameter `state`, you only see the initial snapshot.
If it reads `ExloState.current[S]` inside the stream, it sees live state — including
mutations made by `.advance` and `.recover` on other slices.

For static enumerations (skip-completed, run-once), the parameter is enough. For
adaptive enumerations (bisection, dynamic width, retry-with-narrower-window), ignore
the parameter and read live state.

## The pattern

State carries enough information for the factory to make decisions:

```scala
final case class State(
  cursor:        Instant,           // high-water mark
  currentWidth:  Duration,          // adaptive span size
  pendingSplit:  Set[Range],        // ranges marked by .recover for splitting
  failed:        Set[Range]         // ranges that hit min-width and got skipped
)
```

The recover handler tags state — it doesn't construct slices:

```scala
.recover {
  case (slice, state, _: PoisonError) if slice.width > MinWidth =>
    RecoveryAction.Continue(state.copy(pendingSplit = state.pendingSplit + slice.range))
  case (slice, state, _: PoisonError) =>
    RecoveryAction.Continue(state.copy(failed = state.failed + slice.range))
}
```

The factory reads live state via an unfold and applies the tagging rules:

```scala
.slices(_ => ZStream.unfoldZIO(()) { _ =>
  ExloState.current[State].map { s =>
    pickNext(s).map(slice => (slice, ()))
  }
})

def pickNext(s: State): Option[Slice] =
  s.pendingSplit.headOption match
    case Some(r) => Some(Slice.firstHalfOf(r))     // emit a narrower slice; the
                                                   // recover-or-success that follows
                                                   // will retag pendingSplit accordingly
    case None    => normalNextSlice(s)             // default: cursor + currentWidth
```

`pickNext` is the single source of truth for slice construction. The recover handler
just signals intent.

## Why this works under parallelism

`ExloState.update` is STM-backed (`TRef[S].update.commit`), so concurrent slice fibers'
state mutations serialize. Two slices can poison at the same time and both their
`pendingSplit` tags land safely. The factory sees a consistent view on each pull.

You don't need `parallelism(1)` to make this safe. Use `parallelism(N)` as you would
for any other slice walk; the state machinery handles the rest.

## What the factory should *not* do

- **Don't construct slices inside `.recover`.** Recover mutates state. The factory makes
  slices. Keep the boundary clean — when slice-construction logic lives in two places,
  the second place is wrong.
- **Don't use the `state` parameter passed to `slices`.** It's a snapshot, not a view.
  Either ignore it, or use it only for one-time setup decisions that genuinely don't
  need to update mid-run.
- **Don't track per-slice retry counters in `S`.** That's a logical race even with STM
  (slice A's retry counter and slice B's retry counter conflict on a single field). If
  per-slice state is needed, put it inside the `Slice` type itself or in a
  `Map[SliceKey, SliceLocalState]`.

## When to reach for this pattern

- Adaptive width: bisection, exponential backoff on window size, etc.
- Skip-and-mark on persistent errors: GDPR poison ranges, tombstoned IDs.
- Producer-consumer slicing: one slice's success unlocks the next slice's bounds.

For everything else — static range, enumerate-once-then-fan-out — use the simple
form: `slices(s => ZStream.fromIterable(allSlices.filterNot(s.done.contains)))`.

## See also

- [Getting Started](./getting-started.md) — the simple `HttpSlice` walkthrough
- `RecoveryAction` source — semantics of `.recover` re-entering at `Start`
- `StateAspect.stateCompaction` — collapsing `pendingSplit` / `failed` ranges as they
  grow
