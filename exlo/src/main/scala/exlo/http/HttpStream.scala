package exlo.http

import exlo.domain.{Connector, Emission, ExloError, Tag}
import exlo.runtime.{Codec, SyncMode}
import zio.*
import zio.http.Request
import zio.json.*
import zio.stream.ZStream

/**
 * One stream within an HTTP-based connector. A connector
 * ([[exlo.HttpExloApp]]) is a collection of streams that share auth, base
 * URL, etc.; each stream declares its own pagination, parsing, and state
 * shape.
 *
 * Owns the four user-defined primitives (`request`, `records`, `nextCtx`,
 * `nextState`), the persistence-vs-ephemeral split between `state` and
 * `ctx`, and the adapter from those into a generic [[Connector]] the
 * runner can drive.
 *
 *   - `nextCtx` returns the bag the *next request in this run* should see;
 *     discarded at end of run.
 *   - `nextState` returns the bag *future runs* should see (when
 *     `syncMode = Incremental`). Default: `None` — connector never persists.
 */
trait HttpStream:

  // ---------- identity ----------
  def name: String

  // ---------- behavior config ----------
  def syncMode: SyncMode = SyncMode.Incremental

  // ---------- data slots ----------
  def initialState: Map[String, String] = Map.empty
  def initialCtx:   Map[String, String] = Map.empty

  // ---------- primitives ----------
  def request(state: Map[String, String], ctx: Map[String, String]): Request

  def records(state: Map[String, String], ctx: Map[String, String], r: HttpResponse): Chunk[String]

  /** Some(c) = next request uses ctx `c`; None = stream is done. */
  def nextCtx(state: Map[String, String], ctx: Map[String, String], r: HttpResponse): Option[Map[String, String]]

  /** Some(s) = persist new state (Mark emitted); None = no state advance. */
  def nextState(state: Map[String, String], ctx: Map[String, String], r: HttpResponse): Option[Map[String, String]] = None

  // ---------- adapter to Connector ----------
  // HttpStream connectors are leaf connectors; no FedBy use case, so the
  // output tag is the upper bound `Tag`. The Connector's `id` is composed
  // from the connector id + stream name so the StateStore key disambiguates.

  /** Materialize this stream as a generic `Connector`, scoped by the given
   *  connector id and version (which become the `Connector.id`/`version`). */
  final def asConnector(connectorId: String, connectorVersion: String): Connector[Tag, Map[String, String], HttpExec] =
    val streamRef = this
    new Connector[Tag, Map[String, String], HttpExec]:
      val id           = s"${connectorId}_${streamRef.name}"
      val version      = connectorVersion
      val initialState = streamRef.initialState
      val codec        = HttpStream.mapCodec
      def reduce(a: Map[String, String], b: Map[String, String]): Map[String, String] = a ++ b

      def dataStream(resume: Map[String, String]): ZStream[HttpExec, ExloError, Emission[Map[String, String]]] =
        // FullSync wipes the runner-loaded resume. Incremental honors it.
        val effectiveResume = streamRef.syncMode match
          case SyncMode.Incremental => resume
          case SyncMode.FullSync    => streamRef.initialState

        ZStream.unwrap {
          Ref.make(streamRef.initialCtx).map { ctxRef =>
            // Loop-state Option[State]: None means "stop after this iteration."
            ZStream.unfoldChunkZIO[
              HttpExec, ExloError, Emission[Map[String, String]], Option[Map[String, String]]
            ](Some(effectiveResume)) {
              case None => ZIO.none
              case Some(state) =>
                for
                  ctx  <- ctxRef.get
                  req   = streamRef.request(state, ctx)
                  resp <- HttpExec.run(req)
                  recs    = streamRef.records(state, ctx, resp)
                  nState  = streamRef.nextState(state, ctx, resp)
                  nCtx    = streamRef.nextCtx(state, ctx, resp)
                  emissions = recs.map[Emission[Map[String, String]]](Emission.Record(_)) ++
                              Chunk.fromIterable(
                                nState.map(s => Emission.Mark(s): Emission[Map[String, String]])
                              )
                  nextStateValue = nState.getOrElse(state)
                  next = nCtx match
                    case Some(_) => Some(nextStateValue)  // continue
                    case None    => None                  // stop after this batch
                  _ <- nCtx.fold(ZIO.unit)(c => ctxRef.set(c))
                yield Some((emissions, next))
            }
          }
        }

object HttpStream:

  given JsonEncoder[Map[String, String]] = JsonEncoder.map[String, String]
  given JsonDecoder[Map[String, String]] = JsonDecoder.map[String, String]

  val mapCodec: Codec[Map[String, String]] = Codec.fromJson[Map[String, String]]
