# Getting Started with exlo

exlo is a Scala 3 / ZIO framework for building HTTP ingestion connectors. You describe
*what* to fetch and *how state moves*; the framework drives the loop, retries transient
network failures, lands records in Iceberg, and persists state durably.

The framework gives you three connector shapes, each picked by what your source looks like:

- **`HttpExtract.fullPull`** — stateless one-shot. One request, one response, done.
- **`HttpExtract[S]`** — cursor-paginated. Sequential walk: page → state → next page.
- **`HttpSlice[Slice, S]`** — embarrassingly parallel. Enumerate slices, fetch each one
  (optionally with intra-slice pagination), runs concurrently up to a bound.

Each shape is a builder with a small set of questions. Answer them, get a `Connector`.

> **Note on HTTP methods.** Every shape takes a `zio.http.Request` you build yourself —
> `Request.get`, `Request.post(url, body)`, anything zio-http supports. The framework
> doesn't care what verb you use.

---

## Shape 1: `HttpExtract.fullPull` — stateless full pull

Three questions:

1. **What request do I send?** — `.request(...)`
2. **How do I parse the response?** — `.parse(resp => ZIO[..., P])`
3. **What records do I emit from the parsed page?** — `.records(page => Chunk[String])`

```scala
val kalosConnector = HttpExtract.fullPull
  .request(Request.get(url"https://www.pokemon.com/us/api/pokedex/kalos/"))
  .parse(parseArray)
  .records(items => Chunk.fromIterable(items.map(_.toJson)))
  .toConnector("pokeapi", "0.1.0")
```

`toConnector(id, version)` finalizes into a `Connector[Unit, Client, Throwable]`. The
`Unit` state parameter signals stateless — full-pull connectors have nothing to checkpoint.

Full example: `examples/src/main/scala/examples/pokeapi/PokeApiApp.scala`.

---

## Shape 2: `HttpExtract[S]` — cursor-paginated

`S` is your state type — whatever you need to remember to resume. Common shapes: a cursor
string, a page number, a high-water timestamp, or a record:

```scala
final case class State(cursor: String, lastSeen: Instant)
given JsonCodec[State] = DeriveJsonCodec.gen[State]
```

Five questions, in order:

### 1. `.request(state => Request)` — what request do I send right now?

Given the *current* state, build the HTTP request for the next page. On the first run,
state is your initial value (passed at runtime). On later iterations, it's whatever
`.advance` returned last time.

```scala
.request(s => Request.get(url"https://api.example.com/items?cursor=${s.cursor}"))
```

### 2. `.parse(resp => ZIO[Any, Throwable, P])` — what's the shape of one page?

Turn the raw `Response` into a typed page `P`. `P` is yours — case class, JSON AST, list
of records, whatever. The framework just hands `P` to the next steps.

```scala
final case class Page(items: List[Item], nextCursor: Option[String])

.parse(resp => resp.body.asString.flatMap(s =>
  ZIO.fromEither(s.fromJson[Page]).mapError(...)))
```

### 3. `.records(page => Chunk[String])` — what do I emit from this page?

Given the parsed page, return the records to land downstream. Each record is a `String`
(typically a JSON line). The destination stamps framework metadata (`exlo_sync_id`,
`exlo_stream`, `exlo_recorded_at`, etc.) and writes to Iceberg.

```scala
.records(page => Chunk.fromIterable(page.items.map(_.toJson)))
```

### 4. `.nextRequest((state, page) => Option[Request])` — another page? *(optional)*

Given the new state and the page you just parsed, decide whether to keep going. Return
`Some(req)` to fetch another page, `None` to stop. Omit this method entirely for
single-request connectors.

```scala
.nextRequest((s, page) =>
  page.nextCursor.map(c => Request.get(url"https://api.example.com/items?cursor=$c")))
```

### 5. `.advance((state, page) => S)` — how does state move forward?

Given the parsed page, return the new state. This is what gets checkpointed *after the
records from this page are durable*. The framework guarantees data-then-state ordering,
so a crashed run resumes from the last fully-committed page.

For incremental sync, advance the cursor / high-water mark here:

```scala
.advance((s, page) => s.copy(cursor = page.nextCursor.getOrElse(s.cursor)))
```

### Putting it together

```scala
val itemsConnector = HttpExtract[State]
  .request(s => Request.get(url"https://api.example.com/items?cursor=${s.cursor}"))
  .parse(resp => resp.body.asString.flatMap(s =>
    ZIO.fromEither(s.fromJson[Page]).mapError(...)))
  .records(page => Chunk.fromIterable(page.items.map(_.toJson)))
  .nextRequest((_, page) => page.nextCursor.map(c => Request.get(url"...?cursor=$c")))
  .advance((s, page) => s.copy(cursor = page.nextCursor.getOrElse(s.cursor)))
  .bearer(sys.env("API_TOKEN"))
  .toConnector("example", "0.1.0")
```

The compiler refuses to call `.toConnector` until both `.records` and `.advance` are set.
`.nextRequest` is optional (default: stop after the first page).

---

## Shape 3: `HttpSlice[Slice, S]` — parallel slices

For sources you can fan out by some natural key — date ranges, tenant ids, geographies.
You enumerate the slices; the framework fetches them in parallel up to `parallelism`
(default 4). Each slice can also page internally if needed.

`Slice` is your slice type (`LocalDate`, a tenant id case class, whatever). `S` is your
state type, same as `HttpExtract`.

Six questions:

### 1. `.slices(state => ZStream[Any, Throwable, Slice])` — what's the universe of slices?

Given current state, emit the slices to process this run. State carries what's already
been done — a slice connector typically tracks completed slices in `S` and skips them.

```scala
.slices(state => ZStream.fromIterable(allDates.filterNot(state.completed.contains)))
```

### 2. `.request((slice, state) => Request)` — what request do I send for this slice?

Given a slice (and current state, in case you need it), build the first request.

```scala
.request((day, _) => Request.get(url"https://api.example.com/events?date=$day"))
```

### 3. `.parse(resp => ZIO[..., P])` — same as Extract

### 4. `.records(page => Chunk[String])` — same as Extract

### 5. `.nextRequest((slice, state, page) => Option[Request])` — another page *within this slice*? *(optional)*

For slices that span multiple pages. Return `Some(req)` to keep walking the same slice,
`None` to consider this slice complete.

### 6. `.advance((state, slice, page) => S)` — how does state move forward?

Called after every page lands. Use it to mark slices complete, advance high-water marks,
or aggregate counts. State updates from concurrent slice fibers are serialized by the
runtime, so you don't need to think about race conditions.

```scala
.advance((s, day, _) => s.copy(completed = s.completed + day))
```

### Plus

- `.parallelism(n)` — concurrent slices (default 4).
- All the same auth / retry / OAuth methods as `HttpExtract`.

```scala
val eventsConnector = HttpSlice[LocalDate, State]
  .slices(state => ZStream.fromIterable(daysToProcess(state)))
  .request((day, _) => Request.get(url"...?date=$day"))
  .parse(parsePage)
  .records(emitItems)
  .advance((s, day, _) => s.copy(completed = s.completed + day))
  .parallelism(8)
  .toSlicedConnector("events", "0.1.0")
```

---

## What you get for free

- **Retries on transient network failures.** `IOException`,
  `PrematureChannelClosureException`, `TimeoutException` retry with jittered exponential
  backoff (3 attempts). Override per-connector with `.retry(...)`.
- **Retries on selected HTTP statuses.** `.retryOnStatus(s => s.code == 429 || s.code >= 500)`.
- **OAuth 2 token caching + refresh.** `.oauth(OAuthFlow.ClientCredentials(...))`,
  `OAuthFlow.RefreshToken(...)`, or `OAuthFlow.Password(...)`.
- **Metadata stamping.** Every record gets `exlo_sync_id`, `exlo_connector`,
  `exlo_connector_version`, `exlo_stream`, `exlo_recorded_at`.
- **Durable state with data-then-state ordering.** State is only persisted after the
  records it describes have committed. Crashes resume from the last fully-landed page.

---

## Wiring into a deployable app

Every connector ships through a `StreamRegistry`. The deploy reads `EXLO_STREAM` from
the environment and dispatches to the matching entry. Same image, different stream:

```scala
val registry: StreamRegistry = new StreamRegistry:
  val streams = Map[String, ZIO[Any, Throwable, Unit]](
    "kalos" -> {
      for
        sinkCfg <- SinkConfig.fromEnv
        _ <- Exlo
               .run(kalosConnector, (), sinkCfg)
               .provide(Client.default, DestinationFactory.layer[Unit]("pokeapi"))
      yield ()
    }
  )

def run = StreamRegistry.runSelected(registry)
```

Even single-stream apps go through this — there's no "single-stream" carve-out.
Multi-stream apps just add more entries to the map.

---

## Next steps

- [Configuration](./configuration.md) — environment variables, destinations
- [Local Development](./local-development.md) — run a connector locally
- [Adaptive Slicing](./adaptive-slicing.md) — slice factories that react to mid-run
  state (bisection, poison-pill skip)
- [Integration Testing](./integration-testing.md) — tests that hit real APIs
