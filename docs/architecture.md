# exlo architecture

The framework is plumbing. Stages describe what to emit and how to resume;
the runner moves bytes to storage and persists progress; substrate adapters
(S3 today; future: Kafka, Postgres, …) implement the storage contracts.
Record content at the leaf boundary is **never** a framework concern —
records that hit a `DataSink` are opaque `String`.

## Package layout

```
exlo.domain                  pure types — no infrastructure
  Stage[-I, +O, S, -R]       a node in the pipeline
  Emission[+O, +S]           Record(O) | Mark(S)
  ExloError                  sealed effect-channel error type

exlo.runtime                 framework primitives
  DataSink                   write(Chunk[Sequenced]) → durableSeq
  StateStore                 readByKey / merge / scan(Filter)
  Codec[A]                   encode/decode for state persistence
  WatermarkTracker[S]        admit / advance, releases pending marks
  FlushPolicy                maxRows + maxInterval
  SyncMode                   Incremental | FullSync — runtime config flag
  Runner                     the orchestration loop
  RunContext                 FiberRefs for syncId / connectorId / streamName
  Telemetry                  OTel tracer layers (live / noop / auto)

exlo.http                    high-ergonomics HTTP DSL — uses runtime
  HttpStream[-Parent, +Out]  per-stream primitives + Stage adapter
  HttpExec                   service trait + live impl + helpers
  HttpResponse               pre-parsed JSON; field/asString/asArray ext

exlo.s3                      substrate adapter — uses runtime
  S3DataSink                 JSONL+gzip per write, exlo envelope per record
  S3StateStore               one JSON object per (connector, stream, key)
                             with If-Match / If-None-Match conditional writes
  S3                         S3AsyncClient ZLayer

exlo                         top-level wiring
  Exlo.run                   single-stage entry point — any Stage[Unit, String, …]
  HttpExloApp                ZIOAppDefault for HttpStream-based connectors
```

Dependency direction is one-way: `domain` ← `runtime` ← `http`/`s3` ← `exlo`.
Substrate adapters depend on `runtime`; `runtime` never depends on a
substrate. Adding `exlo.kafka.*` or `exlo.postgres.*` doesn't touch
`exlo.runtime` or `exlo.domain`.

## Two tiers for connector authors

**High tier — `HttpExloApp` + `HttpStream`.** For HTTP-based sources
(typical case). Author defines a `List[HttpStream[Unit, String]]`, each
stream has four primitives. State and ctx are `Map[String, String]` — no
type parameters, no auxiliary case classes. See [The HTTP
layer](#the-http-layer) below.

**Low tier — `Stage[I, O, S, R]`.** Direct trait implementation. Use
when the source isn't HTTP, or when you need fine control: typed `I`/`O`,
custom `Codec`, custom env. See [The core primitives](#the-core-primitives).

## The core primitives

A stage is a value, not a service:

```scala
trait Stage[-I, +O, S, -R]:
  def id: String
  def version: String
  def initialState: S
  def reduce(prev: S, next: S): S
  def codec: Codec[S]
  def run(input: ZStream[Any, ExloError, I], resume: S): ZStream[R, ExloError, Emission[O, S]]
```

- `I` — input record type. Roots have `I = Unit` (the runner feeds a
  single-element kick-off stream). Inner/leaf stages have `I` equal to
  the upstream stage's `O`.
- `O` — output record type. Inter-stage flows can carry any user type;
  the leaf stage that writes to a `DataSink` must have `O = String`.
- `S` — state shape. `reduce` is a left-fold semigroup over `S` (must be
  associative under left-fold — marks arrive in seq order).
- `R` — env requirement. The runner calls `run(input, resume)` once and
  drives the resulting `ZStream` to completion.

Composition is plain `ZStream` piping: pull `Record`s out of the upstream
stage's emission stream, feed them to the next stage's input. The runner
side-effects each stage's `Mark`s into that stage's watermark tracker and
StateStore.

Test by `runCollect`-ing the stream against a synthetic input and resume
and asserting the sequence.

## Runner pipeline

```
stage.run(ZStream.succeed(()), resume)
  .mapZIO {
    case Record(s) => Sequenced(seq++, s)            // forward to sink
    case Mark(st)  => watermark.admit(Pending(curSeq, st)).as(None)  // hold
  }
  .collect { case Some(seqd) => seqd }
  .groupedWithin(flushPolicy.maxRows, flushPolicy.maxInterval)
  .mapZIO { batch =>
    durableSeq <- dataSink.write(batch)
    released   <- watermark.advance(durableSeq)
    commitState(reduce(prior, fold(released)))
  }
  .runDrain
  .ensuring(finalAdvance)
```

`groupedWithin` flushes on row count OR time elapsed. Records are durable
once `dataSink.write` returns; only then do the corresponding marks
commit state. Crash mid-flush drops the in-flight buffer (those records
were never durable, so their state can't release — at-least-once).

This is the single-stage / leaf entry point. Multi-stage chaining
helpers will land alongside the first connector that needs them; the
shape will be: walk a list of stages front-to-back, tap each stage's
emission stream to (a) pipe `Record`s into the next stage and (b)
side-effect `Mark`s into that stage's commit machinery.

## Watermark-gated state commits

A Mark says: *"if every Record I emitted before me is durable, then this
state value is a safe resume point."* The runner stamps each Mark with
the seq# of the most-recently-emitted Record; the mark is *admitted* to
the `WatermarkTracker` and stays pending until `dataSink.write` reports a
`durableSeq` that crosses it.

When marks release, they're folded together (left-fold under
`stage.reduce`), then merged with prior committed state via
read-reduce-write:

```
released = [m1, m2, m3]                   // seq-ordered
folded   = reduce(reduce(m1, m2), m3)
prior    = stateStore.readByKey(...)
merged   = reduce(prior, folded)
stateStore.merge(merged)
```

Idempotent under associative `reduce`: a torn flush (records written but
state commit interrupted) self-heals on next run because the prior state
is read fresh and re-folded.

## DataSink contract

```scala
trait DataSink:
  def write(batch: Chunk[Sequenced]): IO[ExloError, Long]
```

`write` returns the highest seq# now durable. Empty batches return the
current watermark unchanged. Implementations promise: when `write`
returns, every record in the batch is durable. Free to buffer-then-upload
(S3), append-then-fsync (file/Postgres), stage to a broker (Kafka), etc.

## StateStore contract

```scala
trait StateStore:
  def readByKey(connector, stream, key): IO[ExloError, Option[StateRow]]
  def merge(row: StateRow): IO[ExloError, Unit]
  def scan(connector, stream, filter: Filter[String]): ZStream[Any, ExloError, StateRow]
```

`merge` semantics: "newest `(committedAt, syncId)` wins on read."
Implementations can physically write a new object/row each time; reads
resolve the winner. No equality-deletes or compaction required for
correctness.

`S3StateStore.merge` uses S3's conditional writes (`If-Match` against the
read ETag, `If-None-Match: *` for cold start). On a 412 PreconditionFailed
the merge re-reads and retries (bounded to 5 attempts). This is genuinely
atomic single-writer-wins under contention, validated by a 20-fiber
concurrent-merge test against MinIO.

`scan(Filter.Tail(n))` must stream-fold for impls with large state stores.
The InMemory and S3 impls buffer-then-take because typical state per
stream is small (<100 keys); revisit if real workloads exceed that.

## The HTTP layer

A *connector* is the source (Zendesk, GitHub, Stripe). A *stream* is one
extractable endpoint within that source (Zendesk's `tickets`,
`ticket_metrics`, `kalos`). Each runtime invocation runs ONE stream,
selected via `EXLO_STREAM` env var.

### `HttpStream[-Parent, +Out]` — per-stream primitives

```scala
trait HttpStream[-Parent, +Out]:
  def name:         String
  def syncMode:     SyncMode             = SyncMode.Incremental
  def initialState: Map[String, String]  = Map.empty
  def initialCtx:   Map[String, String]  = Map.empty

  def request(parent: Parent, state: Map, ctx: Map): Request
  def records(parent: Parent, state: Map, ctx: Map, r: HttpResponse): Chunk[Out]
  def nextCtx(parent: Parent, state: Map, ctx: Map, r: HttpResponse): Option[Map]
  def nextState(parent: Parent, state: Map, ctx: Map, r: HttpResponse): Option[Map] = None
```

Type parameters:

- `Parent` — the upstream stage's record type. `Unit` for root streams
  (no parent).
- `Out` — the record type this stream emits. Leaf streams that write to
  a `DataSink` use `Out = String`. Inner streams can emit any user type
  the next stage consumes.

State and ctx are both `Map[String, String]`. The persistence boundary
is in the data layer:

- **`state`** — persisted across runs. `nextState` returning `Some` emits
  a Mark that the runner commits to StateStore.
- **`ctx`** — ephemeral within a run. Lives in a `Ref` for the duration
  of the run; discarded at end.

For each parent record, the stream runs an inner walker: while
`nextCtx` returns `Some`, build the next request and iterate. State is
shared across parent records via an internal `Ref`, so a Mark from
parent A is visible to the next iteration over parent B. `nextCtx`
returning `None` ends the per-parent walk; the stream then advances to
the next parent record (if any).

### `HttpResponse` — pre-parsed

```scala
final case class HttpResponse(
    status:  Int,
    headers: Map[String, String],
    body:    String,
    json:    Json    // already parsed; non-JSON bodies fail in HttpExec
)
```

`HttpExec.run` reads the body and parses JSON before invoking user
methods, so `records` / `nextCtx` / `nextState` are pure (no `IO` around
JSON access). Field navigation via the `field` extension on `Json`:
`r.json.field("results").flatMap(_.asArray).getOrElse(Chunk.empty)`.

### `HttpExec` — service trait

```scala
trait HttpExec:
  def run(req: Request): IO[ExloError, HttpResponse]

object HttpExec:
  def run(req: Request): ZIO[HttpExec, ExloError, HttpResponse] =
    ZIO.serviceWithZIO[HttpExec](_.run(req))
  def get(url: String): Request   // pure helper, no service required
  val live: ZLayer[Client, Nothing, HttpExec]   // production wiring
```

Service trait so tests can inject a fake (returns canned responses by
URL) without spinning up a real `zio.http.Server`. The `HttpStream` →
`Stage` adapter declares `R = HttpExec`; production wires `HttpExec.live`
over `Client.default`, tests provide a `ZLayer.succeed` of a fake.

First-pass `live` includes only transient retry (IOException,
TimeoutException — 3 retries, 1s/2s/4s with jitter). Auth (`OAuth`),
status-class retry (`retryOnStatus`), and rate-limit are deferred —
each will land as a decorator on `HttpExec.live`, or as a `ZPipeline`
composing into the stage chain.

### `HttpExloApp` — ZIOAppDefault

```scala
trait HttpExloApp extends ZIOAppDefault:
  def id:        String
  def version:   String      = "1.0.0"
  def s3Config:  S3Config
  def streams:   List[HttpStream[Unit, String]]   // one or many; runtime picks via EXLO_STREAM
```

The trait owns nothing about HTTP itself — primitives live on
`HttpStream`. It only adds connector identity, the canonical layer set
(`S3DataSink`, `S3StateStore`, `S3`, `Client`, `HttpExec.live`,
`Telemetry.auto`), and `EXLO_STREAM`-based dispatch.

Single-stream-shape only today: every entry in `streams` is a root
stream (`Parent = Unit`) emitting `String` records to the DataSink.
Multi-stage connectors with parent/child wiring will use a different
entry point that composes a chain of `Stage`s.

### `SyncMode` — runtime config flag

```scala
enum SyncMode:
  case Incremental    // load state from StateStore on cold start (default)
  case FullSync       // ignore existing state; always start from initialState
```

Both modes commit state forward when `nextState` returns `Some`. Operator
overrides per stream via env. A typical workflow:

1. Initial backfill: `EXLO_SYNC_MODE=fullSync` — pulls everything,
   leaves the latest cursor in StateStore.
2. Steady state: drop the override (`Incremental`) — resumes from where
   the backfill left off.
3. Repair: flip to `fullSync` for one run — pulls fresh, advances state
   over what was there.

### Example: PokeApi

```scala
object PokeApi extends HttpExloApp:
  val id       = "pokeapi"
  val s3Config = S3Config(bucket = "my-data")

  val streams = List(
    new HttpStream[Unit, String]:
      val name = "pokemon"

      def request(p: Unit, s: Map[String, String], c: Map[String, String]) =
        HttpExec.get(c.getOrElse("next", "https://pokeapi.co/api/v2/pokemon?offset=0"))

      def records(p: Unit, s: Map[String, String], c: Map[String, String], r: HttpResponse) =
        r.json.field("results").flatMap(_.asArray).getOrElse(Chunk.empty).map(_.toJson)

      def nextCtx(p: Unit, s: Map[String, String], c: Map[String, String], r: HttpResponse) =
        r.json.field("next").flatMap(_.asString).map(url => Map("next" -> url))
  )
```

The `next` URL lives in `ctx` — purely in-run pagination, never persisted.
`nextState` is not overridden, so the connector never emits a Mark. Each
run paginates from `?offset=0` to exhaustion.

For an incremental connector, override `nextState`:

```scala
override def nextState(p, s, c, r) =
  r.json.field("issues").flatMap(_.asArray).flatMap(_.lastOption)
    .flatMap(_.field("updated_at")).flatMap(_.asString)
    .map(ts => Map("since" -> ts))
```

Each page's last record's timestamp becomes the new resume cursor; the
runner persists it after the page is durable.

## Multi-stage composition (open work)

Every multi-stream HTTP connector is a small DAG of stages:
list-and-detail (queries → mentions, issues → comments), tree-walk,
fan-in. The model is plain `ZStream` piping plus per-stage state commits;
no `Source[T]` env services, no FedBy ZLayer indirection.

Sketched shape, pending the first connector:

```scala
// In a connector that owns both stages:
val queries:  Stage[Unit,  Query, QState, HttpExec] = ...
val mentions: Stage[Query, String, MState, HttpExec] = ...

// The Runner.runChain (TBD) walks this list, piping records and
// side-effecting marks per-stage:
Runner.runChain(queries, mentions, ...)
```

Open questions — to be settled by brandwatch:

- **Intermediate-stage state semantics.** Leaf state commits are
  watermark-gated against `DataSink.write`. Intermediate stages have no
  durable boundary downstream. Two reasonable answers: commit eagerly
  (at-most-once relative to downstream durability), or thread a
  cross-stage watermark. Eager is simpler; brandwatch's `queries`
  doesn't need state at all so it punts the question.
- **Parallelism within a stage.** brandwatch's `mentions` is naturally
  parallel by `(queryId, window)`. ZPipeline gives us `mapZIOPar(n)` for
  free; the question is where it belongs in the API.
- **The `HttpExloApp` shape for chains.** `streams: List[HttpStream[Unit,
  String]]` only handles roots. A chain-aware variant is needed.

## The exlo envelope

Each line of a JSONL+gzip data file written by `S3DataSink`:

```json
{
  "_exlo_ab_id":      "01HQXJ...",
  "_exlo_emitted_at": 1730403600000,
  "_exlo_data":       { ... }
}
```

- `_exlo_ab_id` — UUIDv4 per record.
- `_exlo_emitted_at` — epoch milliseconds (sink-side wall clock).
- `_exlo_data` — the stage's record string parsed as JSON; falls back to
  a JSON string literal if the record isn't valid JSON.

Naming follows Airbyte's `_airbyte_*` convention so downstream tools
that already grok that pattern can plug in with one regex change.

## Entry points

### High level — `HttpExloApp`

```scala
object MyConnector extends HttpExloApp:
  val id       = "mything"
  val s3Config = S3Config(bucket = "my-bucket")
  val streams  = List(...)

// run with: EXLO_STREAM=<name> java -jar ...
```

### Low level — `Exlo.run`

```scala
object MyApp extends ZIOAppDefault:
  def run = Exlo.run(myStage, "stream-name").provide(
    S3DataSink.layer(s3Config),
    S3StateStore.layer(s3Config),
    S3.layer(s3Config),
    Telemetry.auto,
    MyService.layer  // the stage's R
  )
```

`Exlo.run` generates a `syncId`, sets the four `RunContext` FiberRefs
(`syncId`, `connectorId`, `connectorVersion`, `streamName`), opens a root
tracer span, annotates ZIO logs, and delegates to `Runner.run`.

## Validation status

- 47 unit tests + integration tests, 0 failures.
- Core primitives, `Runner`, `WatermarkTracker`, `FlushPolicy`, `Codec`,
  `StateStore.InMemory`, `DataSink.InMemory` — unit tests in `exlo`.
- `S3DataSink` end-to-end against MinIO; `S3StateStore` round-trips +
  20-fiber concurrent-merge contention test — integration tests in
  `exlo-it`.
- `HttpStream` shape validated by `PokeApiSpec` (litmus) running the full
  runner pipeline against a fake `HttpExec`.

## Not yet validated

- **Live HTTP smoke** — `HttpExec.live` against a real API end-to-end.
- **Multi-stage chaining** — `Runner.runChain` and friends. The planned
  brandwatch port is the first real exercise.
- **Auth / status-retry / rate-limit** — port from the deleted code as
  decorators on `HttpExec.live` or as `ZPipeline` stages.
