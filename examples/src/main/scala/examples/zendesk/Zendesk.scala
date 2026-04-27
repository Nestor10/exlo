package examples.zendesk

import exlo.Exlo
import exlo.domain.Connector
import exlo.http.HttpExtract
import exlo.runtime.{DestinationFactory, SinkConfig, StreamRegistry}
import zio.*
import zio.http.*
import zio.json.*
import zio.json.ast.Json

/**
 * Zendesk source — a family of streams over the same `https://<subdomain>.zendesk.com/api/v2`
 * base URL, with shared basic auth and the same `links.next` cursor pagination pattern.
 *
 * Demonstrates the multi-stream pattern: one source, many endpoints, one sbt project, one
 * Docker image. Stream selection happens at runtime via `EXLO_STREAM`.
 *
 * Streams in this registry:
 *   - `tickets`         — `/tickets` endpoint
 *   - `ticket_metrics`  — `/ticket_metrics` endpoint
 *
 * Real connectors would also include `users`, `organizations`, `audits`, etc. — same
 * structure, different `endpointPath` + `recordsKey`.
 *
 * Run:
 * {{{
 *   ZENDESK_SUBDOMAIN=mycorp \
 *   ZENDESK_USERNAME=user@example.com \
 *   ZENDESK_PASSWORD=*** \
 *   EXLO_STREAM=tickets \
 *   EXLO_DESTINATION=iceberg \
 *   EXLO_CATALOG_TYPE=s3tables \
 *   EXLO_CATALOG_WAREHOUSE=arn:aws:s3tables:... \
 *   EXLO_CATALOG_REGION=us-east-1 \
 *   EXLO_TABLE_NAMESPACE=zendesk \
 *   EXLO_TABLE_NAME=tickets \
 *   sbt 'examples/runMain examples.zendesk.ZendeskApp'
 * }}}
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

  /** Cursor state shared across all Zendesk streams (each table has its own cursor). */
  final case class State(cursor: String)
  given JsonCodec[State] = DeriveJsonCodec.gen[State]

  // ---- shared response shape + parser ------------------------------------------------------

  /**
   * Generic Zendesk page envelope. Tracks both the newest (`maxUpdated`) and oldest
   * (`minUpdated`) record on the page — newest advances the cursor, oldest determines
   * when to stop pagination on incremental runs.
   */
  final case class Page(
      records: List[Json],
      maxUpdated: Option[String],
      minUpdated: Option[String],
      nextUrl: Option[String]
  )

  /** Build a parser keyed by the records' top-level array key (e.g. `tickets`, `ticket_metrics`). */
  def parsePage(recordsKey: String): Response => ZIO[Any, Throwable, Page] = resp =>
    resp.body.asString.flatMap { body =>
      ZIO
        .fromEither(body.fromJson[Json])
        .mapError(e => new RuntimeException(s"json parse: $e"))
        .flatMap {
          case Json.Obj(fields) =>
            val records = fields.find(_._1 == recordsKey).map(_._2) match
              case Some(Json.Arr(items)) => items.toList
              case _                     => Nil

            val updatedAts = records.flatMap {
              case Json.Obj(rec) =>
                rec.find(_._1 == "updated_at").map(_._2) match
                  case Some(Json.Str(s)) => Some(s)
                  case _                 => None
              case _ => None
            }

            val nextUrl = fields.find(_._1 == "links").map(_._2) match
              case Some(Json.Obj(links)) =>
                links.find(_._1 == "next").map(_._2) match
                  case Some(Json.Str(s)) => Some(s)
                  case _                 => None
              case _ => None

            ZIO.succeed(
              Page(
                records    = records,
                maxUpdated = if updatedAts.isEmpty then None else Some(updatedAts.max),
                minUpdated = if updatedAts.isEmpty then None else Some(updatedAts.min),
                nextUrl    = nextUrl
              )
            )
          case _ => ZIO.fail(new RuntimeException("expected JSON object"))
        }
    }

  // ---- generic cursor connector ------------------------------------------------------------

  /**
   * Build a Zendesk connector for any endpoint that fits the standard pattern:
   * `sort=-updated_at` initial request, `links.next` cursor pagination, records under a
   * top-level array key.
   */
  def cursorConnector(
      creds: Creds,
      streamId: String,
      streamVersion: String,
      endpointPath: String,
      recordsKey: String
  ): Connector[State, Client, Throwable] =
    HttpExtract[State]
      .request(_ =>
        Request.get(
          URL.decode(s"${creds.baseUrl}$endpointPath?page%5Bsize%5D=100&sort=-updated_at").toOption.get
        )
      )
      .parse(parsePage(recordsKey))
      .records(p => Chunk.fromIterable(p.records.map(_.toJson)))
      .nextRequest { (_, page) =>
        // TODO incremental-stop: ideally, when `sort=-updated_at` and the oldest record on
        // the page is at-or-before the PREVIOUS run's cursor, we should stop pagination —
        // we've crossed into already-seen territory. Today the framework's `advance` runs
        // before `nextRequest` and is pure (`(S, P) => S`, no env access), so there's no
        // clean way to read "the cursor at run start" from inside `nextRequest`. Without
        // the stop, every run pages through Zendesk's full history. Cursor still tracks
        // and state still persists; just not as efficient as the Python original.
        page.nextUrl.flatMap(url => URL.decode(url).toOption.map(Request.get(_)))
      }
      .advance { (state, page) =>
        page.maxUpdated.fold(state)(u => if u > state.cursor then state.copy(cursor = u) else state)
      }
      .basicAuth(creds.username, creds.password)
      .header(Header.ContentType(MediaType.application.json))
      .toConnector(streamId, streamVersion)

  // ---- specific streams --------------------------------------------------------------------

  def tickets(creds: Creds): Connector[State, Client, Throwable] =
    cursorConnector(creds, "zendesk-tickets", "0.1.0", "/tickets", "tickets")

  def ticketMetrics(creds: Creds): Connector[State, Client, Throwable] =
    cursorConnector(creds, "zendesk-ticket-metrics", "0.1.0", "/ticket_metrics", "ticket_metrics")

/**
 * Runtime registry. Each entry is a fully-provided ZIO that runs one stream end-to-end
 * (connector + Exlo.run + Client + DestinationFactory). `EXLO_STREAM` selects.
 */
object ZendeskStreams:

  def make(creds: Zendesk.Creds): StreamRegistry = new StreamRegistry:
    override val streams: Map[String, ZIO[Any, Throwable, Unit]] = Map(
      "tickets"        -> runWith(Zendesk.tickets(creds)),
      "ticket_metrics" -> runWith(Zendesk.ticketMetrics(creds))
    )

  /** Wire one stream's connector to a SinkConfig (env) + Client (default) + Destination (env). */
  private def runWith(
      connector: Connector[Zendesk.State, Client, Throwable]
  ): ZIO[Any, Throwable, Unit] =
    for
      sinkCfg <- SinkConfig.fromEnv
      _ <- Exlo
             .run(connector, Zendesk.State(""), sinkCfg)
             .provide(
               Client.default,
               DestinationFactory.layer[Zendesk.State](connector.id)
             )
    yield ()

/**
 * Single entry point — reads creds + `EXLO_STREAM` from env, dispatches via the registry.
 *
 * One Docker image, one main class, one container CMD. Each Argo Workflow Job sets a
 * different `EXLO_STREAM` (and matching `EXLO_TABLE_NAME`) to run a different stream.
 */
object ZendeskApp extends ZIOAppDefault:
  def run =
    for
      creds   <- Zendesk.Creds.fromEnv
      registry = ZendeskStreams.make(creds)
      _       <- StreamRegistry.runSelected(registry)
    yield ()
