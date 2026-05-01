# exlo architecture

The framework is plumbing. Connectors describe what to emit and how to
resume; the runner moves bytes to storage and persists progress; substrate
adapters (S3, future: Kafka, Postgres) implement the storage contracts.
Record content is **never** a framework concern — records on the wire are
opaque `String`.

## The five primitives

```
exlo.domain
  Tag                     phantom marker for connector outputs
  Emission[+S]            Record(String) | Mark[S]
  Connector[+O <: Tag, S, -R]
  ExloError               sealed effect-channel error type

exlo.runtime
  DataSink                write(Chunk[Sequenced]) → durableSeq
  StateStore              readByKey / merge / scan(Filter)
  Codec[A]                encode/decode for state persistence
  WatermarkTracker[S]     admit / advance, releases pending marks
  FlushPolicy             maxRows + maxInterval
  Source[T <: Tag]        env service for parent records
  FedBy                   ZLayer wiring a parent connector as a Source
  Runner                  the orchestration loop

exlo.s3                   substrate adapter (sibling of runtime, not under it)
  S3DataSink              JSONL+gzip per write, exlo envelope per record
  S3StateStore            one JSON object per (connector, stream, key)
  S3                      S3AsyncClient ZLayer
```

## What a connector author writes

Five small things. No env-service plumbing, no `Throwable` in the error
channel, no side channels for output.

```scala
trait Connector[+O <: Tag, S, -R]:
  def id: String
  def version: String
  def initialState: S
  def reduce(prev: S, next: S): S
  def codec: Codec[S]
  def dataStream(resume: S): ZStream[R, ExloError, Emission[S]]
```

`O` is a phantom output tag (so child connectors can disambiguate parents
via `Source[Users]` vs `Source[Projects]`). `S` is the state shape.
`reduce` is a left-fold semigroup over `S`; the runner uses it at every
flush boundary to combine prior committed state with newly released marks.

A typical paginated connector emits page-of-records, then a Mark with the
cursor, then page-of-records, then a Mark, …

```scala
def dataStream(cursor: Cursor): ZStream[HttpClient, ExloError, Emission[Cursor]] =
  ZStream.unfoldZIO(cursor) { c =>
    fetchPage(c).map { page =>
      val records = ZStream.fromChunk(page.issues).map(Emission.Record(_))
      val mark    = ZStream(Emission.Mark(page.nextCursor))
      Some(((records ++ mark), page.nextCursor)).filter(_ => !page.exhausted)
    }
  }.flatten
```

Pure value. Test by `runCollect` on the stream and asserting the sequence.

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
once `dataSink.write` returns; only then can the corresponding marks
commit state. Crash mid-flush drops the in-flight buffer (those records
were never durable, so their state can't release — at-least-once).

## Watermark-gated state commits

A Mark says: *"if every Record I emitted before me is durable, then this
state value is a safe resume point."* The runner stamps each Mark with
the seq# of the most-recently-emitted Record at the moment the Mark was
yielded. The mark is *admitted* to the WatermarkTracker; it stays pending
until `dataSink.write` reports a `durableSeq` that crosses it.

When marks release, they're folded together (left-fold under
`connector.reduce`), then merged with the prior committed state via
read-reduce-write:

```
released                = [m1, m2, m3]            // seq-ordered
folded                  = reduce(reduce(m1, m2), m3)
prior                   = stateStore.readByKey(...)
merged                  = reduce(prior, folded)
stateStore.merge(merged)
```

This is idempotent under associative reduce: a torn flush (records
written but state commit interrupted) self-heals on next run because the
prior state is read fresh and re-folded.

## DataSink contract

```scala
trait DataSink:
  def write(batch: Chunk[Sequenced]): IO[ExloError, Long]
```

`write` returns the highest seq# of records now durable. Empty batches
return the current durable watermark unchanged (runner short-circuits
empty groups via `groupedWithin` semantics).

Implementations are free to:
- Buffer to a tempfile and upload (S3 sink)
- Append to an open file and fsync (Postgres-COPY sink, future)
- Stage to a local broker partition (Kafka sink, future)

…as long as the contract holds: when `write` returns, every record in
the batch is durable.

## StateStore contract

```scala
trait StateStore:
  def readByKey(connector, stream, key): IO[ExloError, Option[StateRow]]
  def merge(row: StateRow): IO[ExloError, Unit]
  def scan(connector, stream, filter: Filter[String]): ZStream[Any, ExloError, StateRow]
```

`merge` is "newest committedAt + syncId wins on read" — implementations
can physically write a new file/object/row each time and let read resolve
the winner. No equality-deletes or compaction required for correctness.

`scan(Filter.Tail(n))` must stream-fold (not buffer the whole table) for
implementations with potentially-large state stores. The InMemory and S3
impls buffer-then-take because typical state per stream is small (<100
keys); revisit if real workloads exceed that.

## Source / FedBy

A child connector that consumes a parent's records declares
`Source[ParentTag]` in its env:

```scala
trait UsersTag extends Tag

class OrdersConnector extends Connector[OrdersOutTag, OrdersState, Source[UsersTag] & HttpClient]:
  def dataStream(resume) =
    ZStream.serviceWithStream[Source[UsersTag]](_.stream).mapZIO(fetchOrders)
```

`FedBy(usersConnector)` produces a `ZLayer[Rp, ExloError, Source[UsersTag]]`
that runs the parent inside a forked scope and pipes its records through a
bounded queue feeding the child. Multi-parent: stack `FedBy` layers; each
distinct phantom tag becomes a distinct `Source[T]` service in the env.
ZIO's layer memoization gives free fan-out (one parent runner shared
across multiple children).

v1 caveat: parents in FedBy mode use an internal `NoopStateStore` and
always cold-start, because the queue is volatile and committing parent
state would over-claim durability the child hasn't observed yet.
Suitable for parents cheap to re-enumerate (HTTP list endpoints).
Persistent-tail Source impls for incremental parents come later.

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

The naming follows Airbyte's `_airbyte_*` convention so downstream tools
that already grok that pattern can plug in with one regex change.

## User-facing entry point

```scala
object MyApp extends ZIOAppDefault:
  def run =
    val s3Config = S3Config(bucket = "my-bucket")
    Exlo.run(myConnector, "stream-name").provide(
      S3DataSink.layer(s3Config),
      S3StateStore.layer(s3Config),
      S3.layer(s3Config),
      Telemetry.auto,
      MyService.layer  // the connector's R requirement
    )
```

`Exlo.run` generates a `syncId`, sets the four `RunContext` FiberRefs
(`syncId`, `connectorId`, `connectorVersion`, `streamName`) for the
duration of the run, opens a root tracer span, annotates ZIO logs, and
delegates to `Runner.run`.

## Layering

```
domain  ◄──────── runtime  ◄──────── s3
                                     (future: kafka, postgres, ...)
                                     ▲
                                     │
                                     user app
```

Dependency direction is one-way: substrate adapters depend on `runtime`,
never the other way. Adding a new substrate (`exlo.kafka.*`,
`exlo.postgres.*`) doesn't touch any code under `exlo.runtime` or
`exlo.domain`.
