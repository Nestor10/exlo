package examples.zendesk

import exlo.Exlo
import exlo.domain.SlicedConnector
import exlo.http.HttpSlice
import exlo.runtime.{DestinationFactory, SinkConfig, StreamRegistry, Telemetry}
import zio.*
import zio.http.*
import zio.json.*
import zio.json.ast.Json
import zio.logging.{ConsoleLoggerConfig, consoleJsonLogger}
import zio.stream.ZStream
import zio.telemetry.opentelemetry.core.trace.Tracer

import java.time.{Instant, OffsetDateTime, ZoneOffset}

/**
 * Zendesk source — slice-based connectors for both `/api/v2/incremental/<resource>`
 * (server-side time-window filtering) and standard `/api/v2/<resource>` (client-side
 * stop on slice.start crossing). Both share auth + the same slice/state model.
 *
 * Why slices: the slice's `start`/`end` give a hard time-window boundary that's visible
 * to `nextRequest`, so we get clean incremental-stop semantics without the cursor-juggling
 * the v0.1 Python connector needed.
 *
 * State across runs:
 *   - `done: Set[Window]` — completed time windows. STM-atomic per slice; out-of-order
 *     completion is handled because we track the SET, not a single watermark. Resume
 *     filters this set.
 *   - For long-running deploys with daily windows over many years, the done set grows
 *     linearly. Compaction (rolling contiguous prefix into an hwm) is a TODO; documented
 *     in CONFIG.md.
 *
 * Streams in this registry:
 *   - `tickets`         — `/incremental/tickets` (server-side filter)
 *   - `ticket_metrics`  — `/ticket_metrics` (no /incremental endpoint; client-side stop)
 */
object Zendesk:

  // ---- creds + state -----------------------------------------------------------------------

  /** Per-tenant config from env: `ZENDESK_SUBDOMAIN`, `ZENDESK_USERNAME`, `ZENDESK_PASSWORD`. */
  final case class Creds(subdomain: String, username: String, password: String):
    def baseUrl: String = s"https://$subdomain.zendesk.com/api/v2"

  object Creds:
    val config: Config[Creds] =
      (Config.string("subdomain") ++ Config.string("username") ++ Config.string("password"))
        .nested("zendesk")
        .map { case (s, u, p) => Creds(s, u, p) }

    def fromEnv: ZIO[Any, Config.Error, Creds] = ZIO.config(config)

  /** A time window, unix-epoch seconds. */
  final case class Window(start: Long, end: Long)
  object Window:
    given JsonCodec[Window] = DeriveJsonCodec.gen[Window]

  /** Per-resource state: which windows have been fully processed. */
  final case class State(done: Set[Window] = Set.empty)
  object State:
    given JsonCodec[State] = DeriveJsonCodec.gen[State]
    val zero: State        = State(Set.empty)

  // ---- response shapes + parsers -----------------------------------------------------------

  /**
   * Unified parsed page. For `/incremental` endpoints, `nextUrl` comes from the response's
   * `next_page` field; for standard endpoints it comes from `links.next`. `minUpdated` is
   * used by the standard-endpoint slice-stop check.
   */
  final case class Page(
      records: List[Json],
      minUpdated: Option[String],
      nextUrl: Option[String]
  )

  /** Parser for `/api/v2/incremental/<resource>` envelope. */
  def parseIncrementalPage(resource: String): Response => ZIO[Any, Throwable, Page] = resp =>
    resp.body.asString.flatMap { body =>
      ZIO
        .fromEither(body.fromJson[Json])
        .mapError(e => new RuntimeException(s"json parse: $e"))
        .flatMap {
          case Json.Obj(fields) =>
            val records = fields.find(_._1 == resource).map(_._2) match
              case Some(Json.Arr(items)) => items.toList
              case _                     => Nil
            val updatedAts = recordUpdatedAts(records)
            val nextUrl = fields.find(_._1 == "next_page").map(_._2) match
              case Some(Json.Str(s)) => Some(s)
              case _                 => None
            // Zendesk also emits `end_of_stream: true` on the terminal page; both signals
            // line up. We use `next_page` as the canonical "more pages" indicator.
            ZIO.succeed(
              Page(
                records    = records,
                minUpdated = if updatedAts.isEmpty then None else Some(updatedAts.min),
                nextUrl    = nextUrl
              )
            )
          case _ => ZIO.fail(new RuntimeException("expected JSON object"))
        }
    }

  /** Parser for standard `/api/v2/<resource>` envelope (with `links.next` cursor). */
  def parseStandardPage(resource: String): Response => ZIO[Any, Throwable, Page] = resp =>
    resp.body.asString.flatMap { body =>
      ZIO
        .fromEither(body.fromJson[Json])
        .mapError(e => new RuntimeException(s"json parse: $e"))
        .flatMap {
          case Json.Obj(fields) =>
            val records = fields.find(_._1 == resource).map(_._2) match
              case Some(Json.Arr(items)) => items.toList
              case _                     => Nil
            val updatedAts = recordUpdatedAts(records)
            val nextUrl = fields.find(_._1 == "links").map(_._2) match
              case Some(Json.Obj(links)) =>
                links.find(_._1 == "next").map(_._2) match
                  case Some(Json.Str(s)) => Some(s)
                  case _                 => None
              case _ => None
            ZIO.succeed(
              Page(
                records    = records,
                minUpdated = if updatedAts.isEmpty then None else Some(updatedAts.min),
                nextUrl    = nextUrl
              )
            )
          case _ => ZIO.fail(new RuntimeException("expected JSON object"))
        }
    }

  private def recordUpdatedAts(records: List[Json]): List[String] = records.flatMap {
    case Json.Obj(rec) =>
      rec.find(_._1 == "updated_at").map(_._2) match
        case Some(Json.Str(s)) => Some(s)
        case _                 => None
    case _ => None
  }

  // ---- window helpers ----------------------------------------------------------------------

  /** Generate non-overlapping daily windows covering `[from, to)`. Last window may be partial. */
  def dailyWindows(from: Long, to: Long): List[Window] =
    if from >= to then Nil
    else
      val SecondsPerDay = 86400L
      LazyList
        .iterate(from)(_ + SecondsPerDay)
        .takeWhile(_ < to)
        .map(start => Window(start, math.min(start + SecondsPerDay, to)))
        .toList

  /** ISO-8601 string from unix seconds, UTC. */
  private def toIso(unixSecond: Long): String =
    OffsetDateTime.ofInstant(Instant.ofEpochSecond(unixSecond), ZoneOffset.UTC).toString

  // ---- connector builders ------------------------------------------------------------------

  /**
   * Slice-based incremental connector for resources that support
   * `/api/v2/incremental/<resource>?start_time=X&end_time=Y`. Server filters to the window;
   * client paginates via response `next_page` until null. Slices are daily; `parallelism`
   * controls concurrent slice fetches (good for backfill).
   */
  def incrementalConnector(
      creds: Creds,
      streamId: String,
      streamVersion: String,
      resource: String,
      from: Long       = Instant.parse("2020-01-01T00:00:00Z").getEpochSecond,
      parallelism: Int = 4
  ): SlicedConnector[Window, State, Client & Tracer, Throwable] =
    HttpSlice[Window, State]
      .slices { state =>
        val to = Instant.now().getEpochSecond
        ZStream
          .fromIterable(dailyWindows(from, to))
          .filterNot(w => state.done.contains(w))
      }
      .request { (slice, _) =>
        Request.get(
          URL
            .decode(
              s"${creds.baseUrl}/incremental/$resource?start_time=${slice.start}&end_time=${slice.end}"
            )
            .toOption
            .get
        )
      }
      .parse(parseIncrementalPage(resource))
      .records(page => Chunk.fromIterable(page.records.map(_.toJson)))
      .nextRequest { (_, _, page) =>
        page.nextUrl.flatMap(url => URL.decode(url).toOption.map(Request.get(_)))
      }
      .advance { (state, slice, page) =>
        // /incremental: terminal when next_page is null. Server already filtered the
        // records to the window; nothing to do client-side.
        if page.nextUrl.isEmpty then state.copy(done = state.done + slice) else state
      }
      .basicAuth(creds.username, creds.password)
      .header(Header.ContentType(MediaType.application.json))
      .parallelism(parallelism)
      .toSlicedConnector(streamId, streamVersion)

  /**
   * Slice-based connector for resources WITHOUT `/incremental` support. Standard endpoint
   * with `sort=-updated_at` (descending); client paginates via `links.next` and stops when
   * the oldest record on the page predates `slice.start`. One slice per run by default
   * (`parallelism=1`) to avoid over-fetching duplicate top records on parallel slices.
   *
   * Records emitted include some "leakage" on the terminal page: records older than
   * `slice.start` on the last page are emitted (we don't have slice context inside the
   * `records` callback). Downstream dedup handles it. For tighter emit control, the
   * framework would need a `(Slice, P) => Chunk[String]` records signature — TODO.
   */
  def slicedCursorConnector(
      creds: Creds,
      streamId: String,
      streamVersion: String,
      resource: String,
      from: Long = Instant.parse("2020-01-01T00:00:00Z").getEpochSecond
  ): SlicedConnector[Window, State, Client & Tracer, Throwable] =
    HttpSlice[Window, State]
      .slices { state =>
        val to = Instant.now().getEpochSecond
        // One big slice covering the not-yet-done range. Done set captures completed
        // single-slice runs; resume picks up where the last run left off.
        val doneEnd = state.done.toList.map(_.end).maxOption.getOrElse(from)
        if doneEnd < to then ZStream(Window(doneEnd, to))
        else ZStream.empty
      }
      .request { (_, _) =>
        Request.get(
          URL.decode(s"${creds.baseUrl}/$resource?sort=-updated_at&page%5Bsize%5D=100").toOption.get
        )
      }
      .parse(parseStandardPage(resource))
      .records(page => Chunk.fromIterable(page.records.map(_.toJson)))
      .nextRequest { (slice, _, page) =>
        // Stop when we've descended past slice.start — already-seen territory.
        val crossed = page.minUpdated.exists(_ < toIso(slice.start))
        if crossed then None
        else page.nextUrl.flatMap(url => URL.decode(url).toOption.map(Request.get(_)))
      }
      .advance { (state, slice, page) =>
        // Terminal when nextUrl is null OR oldest on page predates slice.start.
        val crossed  = page.minUpdated.exists(_ < toIso(slice.start))
        val terminal = page.nextUrl.isEmpty || crossed
        if terminal then state.copy(done = state.done + slice) else state
      }
      .basicAuth(creds.username, creds.password)
      .header(Header.ContentType(MediaType.application.json))
      .parallelism(1)
      .toSlicedConnector(streamId, streamVersion)

  // ---- specific streams --------------------------------------------------------------------

  /** Tickets via `/api/v2/incremental/tickets`. Daily-window slices, parallel backfill. */
  def tickets(creds: Creds): SlicedConnector[Window, State, Client & Tracer, Throwable] =
    incrementalConnector(creds, "zendesk_tickets", "0.1.0", "tickets")

  /** Ticket metrics via `/api/v2/ticket_metrics` (no /incremental). Single-slice + cursor stop. */
  def ticketMetrics(creds: Creds): SlicedConnector[Window, State, Client & Tracer, Throwable] =
    slicedCursorConnector(creds, "zendesk_ticket_metrics", "0.1.0", "ticket_metrics")

/** Runtime registry. Each entry runs one stream end-to-end via Exlo.run + provided env. */
object ZendeskStreams:

  def make(creds: Zendesk.Creds): StreamRegistry = new StreamRegistry:
    override val streams: Map[String, ZIO[Any, Throwable, Unit]] = Map(
      "tickets"        -> runWith(Zendesk.tickets(creds)),
      "ticket_metrics" -> runWith(Zendesk.ticketMetrics(creds))
    )

  private def runWith(
      sliced: SlicedConnector[Zendesk.Window, Zendesk.State, Client & Tracer, Throwable]
  ): ZIO[Any, Throwable, Unit] =
    for
      sinkCfg <- SinkConfig.fromEnv
      _ <- Exlo
             .run(sliced.toConnector, Zendesk.State.zero, sinkCfg)
             .provide(
               Client.default,
               DestinationFactory.layer[Zendesk.State](sliced.id),
               Telemetry.auto
             )
    yield ()

/**
 * Single entry point — reads creds + `EXLO_STREAM` from env, dispatches via the registry.
 * One Docker image per source. Argo Workflow Job sets `EXLO_STREAM` (and matching
 * `EXLO_TABLE_NAME`) per stream to run.
 */
object ZendeskApp extends ZIOAppDefault:

  // Replace ZIO's default text logger with a structured JSON one. Combined with
  // `Telemetry.auto`'s `logAnnotated = true` (when `live` is selected), every log line
  // carries `trace_id` / `span_id` for the active span — so logs and traces correlate
  // in the collector.
  override val bootstrap: ZLayer[Any, Nothing, Unit] =
    Runtime.removeDefaultLoggers >>> consoleJsonLogger(ConsoleLoggerConfig.default)

  def run =
    for
      creds   <- Zendesk.Creds.fromEnv
      registry = ZendeskStreams.make(creds)
      _       <- StreamRegistry.runSelected(registry)
    yield ()
