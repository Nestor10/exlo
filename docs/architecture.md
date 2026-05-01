# exlo architecture

The framework is plumbing. Connectors describe what to emit and how to
resume; the runner moves bytes to storage and persists progress; substrate
adapters (S3 today; future: Kafka, Postgres, …) implement the storage
contracts. Record content is **never** a framework concern — records on
the wire are opaque `String`.

## Package layout

```
exlo.domain                  pure types — no infrastructure
  Tag                        phantom marker for connector outputs
  Emission[+S]               Record(String) | Mark[S]
  Connector[+O <: Tag, S, -R]
  ExloError                  sealed effect-channel error type

exlo.runtime                 framework primitives
  DataSink                   write(Chunk[Sequenced]) → durableSeq
  StateStore                 readByKey / merge / scan(Filter)
  Codec[A]                   encode/decode for state persistence
  WatermarkTracker[S]        admit / advance, releases pending marks
  FlushPolicy                maxRows + maxInterval
  SyncMode                   Incremental | FullSync — runtime config flag
  Source[T <: Tag]           env service for parent records
  FedBy                      ZLayer wiring a parent connector as a Source
  Runner                     the orchestration loop
  RunContext                 FiberRefs for syncId / connectorId / streamName
  Telemetry                  OTel tracer layers (live / noop / auto)

exlo.http                    high-ergonomics HTTP DSL — uses runtime
  HttpStream                 per-stream primitives + Connector adapter
  HttpExec                   service trait + live impl + helpers
  HttpResponse               pre-parsed JSON; field/asString/asArray ext

exlo.s3                      substrate adapter — uses runtime
  S3DataSink                 JSONL+gzip per write, exlo envelope per record
  S3StateStore               one JSON object per (connector, stream, key)
                             with If-Match / If-None-Match conditional writes
  S3                         S3AsyncClient ZLayer

exlo                         top-level wiring
  Exlo.run                   low-level entry point — any Connector
  HttpExloApp                ZIOAppDefault for an HttpStream-based connector
```

Dependency direction is one-way: `domain` ← `runtime` ← `http`/`s3` ← `exlo`.
Substrate adapters depend on `runtime`; `runtime` never depends on a
substrate. Adding `exlo.kafka.*` or `exlo.postgres.*` doesn't touch
`exlo.runtime` or `exlo.domain`.

## Two tiers for connector authors

**High tier — `HttpExloApp` + `HttpStream`.** For HTTP-based sources
(typical case). Author defines a `List[HttpStream]`, each stream has four
primitives. State and ctx are `Map[String, String]` — no type parameters,
no auxiliary case classes. See [The HTTP layer](#the-http-layer) below.

**Low tier — `Connector[O, S, R]`.** Direct trait implementation. Use
when the source isn't HTTP, or when you need fine control: typed `S`,
custom `Codec`, custom env. See [The core primitives](#the-core-primitives).

## The core primitives

A connector is a value, not a service:

```scala
trait Connector[+O <: Tag, S, -R]:
  def id: String
  def version: String
  def initialState: S
  def reduce(prev: S, next: S): S
  def codec: Codec[S]
  def dataStream(resume: S): ZStream[R, ExloError, Emission[S]]
```

- `O` — phantom output tag for `Source[T]` disambiguation in parent-child
  flows. Use the upper bound `Tag` for leaf connectors not consumed via
  `FedBy`.
- `S` — state shape. `reduce` is a left-fold semigroup over `S` (must be
  associative under left-fold — marks arrive in seq order).
- `R` — env requirement. The runner calls `dataStream(resume)` once and
  drives the resulting `ZStream` to completion.

Test by `runCollect`-ing the stream against synthetic state and asserting
the sequence.

## Runner pipeline

```
connector.dataStream(resume)
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

## Watermark-gated state commits

A Mark says: *"if every Record I emitted before me is durable, then this
state value is a safe resume point."* The runner stamps each Mark with
the seq# of the most-recently-emitted Record; the mark is *admitted* to
the `WatermarkTracker` and stays pending until `dataSink.write` reports a
`durableSeq` that crosses it.

When marks release, they're folded together (left-fold under
`connector.reduce`), then merged with prior committed state via
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

### `HttpStream` — per-stream primitives

```scala
trait HttpStream:
  def name:         String
  def syncMode:     SyncMode             = SyncMode.Incremental
  def initialState: Map[String, String]  = Map.empty
  def initialCtx:   Map[String, String]  = Map.empty

  def request(state: Map, ctx: Map): Request
  def records(state: Map, ctx: Map, r: HttpResponse): Chunk[String]
  def nextCtx(state: Map, ctx: Map, r: HttpResponse): Option[Map]
  def nextState(state: Map, ctx: Map, r: HttpResponse): Option[Map] = None
```

State and ctx are both `Map[String, String]` — no type parameters, no
codecs to derive. The persistence boundary is in the data layer:

- **`state`** — persisted across runs. `nextState` returning `Some` emits
  a Mark that the runner commits to StateStore.
- **`ctx`** — ephemeral within a run. Lives in a `Ref` for the duration
  of the run; discarded at end.

`nextCtx` returning `None` ends the run (no more pages). `nextState`
returning `None` (default) means no advance for this page — most
full-refresh connectors never override it.

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
`Connector` adapter declares `R = HttpExec`; production wires
`HttpExec.live` over `Client.default`, tests provide a `ZLayer.succeed`
of a fake.

First-pass `live` includes only transient retry (IOException,
TimeoutException — 3 retries, 1s/2s/4s with jitter). Auth (`OAuth`),
status-class retry (`retryOnStatus`), and rate-limit are deferred —
each will land as a decorator on `HttpExec.live`.

### `HttpExloApp` — ZIOAppDefault

```scala
trait HttpExloApp extends ZIOAppDefault:
  def id:        String
  def version:   String      = "1.0.0"
  def s3Config:  S3Config
  def streams:   List[HttpStream]   // one or many; runtime picks via EXLO_STREAM
```

The trait owns nothing about HTTP itself — primitives live on
`HttpStream`. It only adds connector identity, the canonical layer set
(`S3DataSink`, `S3StateStore`, `S3`, `Client`, `HttpExec.live`,
`Telemetry.auto`), and `EXLO_STREAM`-based dispatch.

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
    new HttpStream:
      val name = "pokemon"

      def request(s, c) =
        HttpExec.get(c.getOrElse("next", "https://pokeapi.co/api/v2/pokemon?offset=0"))

      def records(s, c, r) =
        r.json.field("results").flatMap(_.asArray).getOrElse(Chunk.empty).map(_.toJson)

      def nextCtx(s, c, r) =
        r.json.field("next").flatMap(_.asString).map(url => Map("next" -> url))
  )
```

The `next` URL lives in `ctx` — purely in-run pagination, never persisted.
`nextState` is not overridden, so the connector never emits a Mark. Each
run paginates from `?offset=0` to exhaustion.

For an incremental connector, override `nextState`:

```scala
override def nextState(s, c, r) =
  r.json.field("issues").flatMap(_.asArray).flatMap(_.lastOption)
    .flatMap(_.field("updated_at")).flatMap(_.asString)
    .map(ts => Map("since" -> ts))
```

Each page's last record's timestamp becomes the new resume cursor; the
runner persists it after the page is durable.

## Source / FedBy (parent-child)

A child connector that consumes a parent's records declares
`Source[ParentTag]` in its env:

```scala
trait UsersTag extends Tag

class OrdersConnector extends Connector[OrdersOutTag, OrdersState, Source[UsersTag] & HttpClient]:
  def dataStream(resume) =
    ZStream.serviceWithStream[Source[UsersTag]](_.stream).mapZIO(fetchOrders)
```

`FedBy(usersConnector)` produces a `ZLayer[Rp, ExloError, Source[UsersTag]]`
that runs the parent inside a forked scope and pipes its records through
a bounded queue feeding the child. Multi-parent: stack `FedBy` layers;
each distinct phantom tag becomes a distinct `Source[T]` service in the
env. ZIO's layer memoization gives free fan-out — one parent runner
shared across multiple children.

**v1 caveats — not yet validated by a real connector.** Parents in
`FedBy` mode use an internal `NoopStateStore` and always cold-start (the
queue is volatile, so committing parent state would over-claim
durability). Suitable for parents cheap to re-enumerate; persistent-tail
`Source` impls for incremental parents come later.

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
- `_exlo_data` — the connector's record string parsed as JSON; falls
  back to a JSON string literal if the record isn't valid JSON.

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
  def run = Exlo.run(myConnector, "stream-name").provide(
    S3DataSink.layer(s3Config),
    S3StateStore.layer(s3Config),
    S3.layer(s3Config),
    Telemetry.auto,
    MyService.layer  // the connector's R
  )
```

`Exlo.run` generates a `syncId`, sets the four `RunContext` FiberRefs
(`syncId`, `connectorId`, `connectorVersion`, `streamName`), opens a root
tracer span, annotates ZIO logs, and delegates to `Runner.run`.

## Validation status

- 50 unit tests + 9 integration tests, 0 failures.
- Core primitives, `Runner`, `WatermarkTracker`, `FlushPolicy`, `Codec`,
  `StateStore.InMemory`, `DataSink.InMemory`, `Source`/`FedBy` smoke —
  unit tests in `exlo`.
- `S3DataSink` end-to-end against MinIO; `S3StateStore` round-trips +
  20-fiber concurrent-merge contention test — integration tests in
  `exlo-it`.
- `HttpStream` shape validated by `PokeApiSpec` (litmus) running the full
  runner pipeline against a fake `HttpExec` — 232ms.

## Not yet validated

- **Live HTTP smoke** — `HttpExec.live` against a real API end-to-end.
- **Multi-stream connector** — `streams: List[HttpStream]` with shared
  auth/baseUrl. Zendesk-style rebuild is the planned litmus.
- **`FedBy` with a real connector** — only the layer-composition smoke
  test exists.
- **Auth / status-retry / rate-limit** — deferred from the deleted code;
  port as decorators on `HttpExec.live`.
