package exlo

import exlo.domain.ExloError
import exlo.http.{HttpExec, HttpStream}
import exlo.runtime.{FlushPolicy, Telemetry}
import exlo.s3.{S3, S3Config, S3DataSink, S3StateStore}
import zio.*
import zio.http.Client

/**
 * Stand-alone ZIO app trait for an HTTP-based connector — one *connector*
 * (the source) exposing one or more *streams* (extractable endpoints).
 *
 * The connector author defines:
 *   - identity (`id`, `version`)
 *   - infrastructure (`s3Config`)
 *   - shared things at the connector level (auth, base URL, headers,
 *     retry policy) as plain `protected val`s
 *   - the streams the connector exposes (`streams: List[HttpStream]`)
 *
 * Each runtime invocation runs ONE stream, selected via `EXLO_STREAM` env
 * var (or the equivalent `exlo.stream` config key). The framework looks up
 * the named stream, builds its `Connector`, and runs it via `Exlo.run`
 * with the canonical layer set: S3 data sink + S3 state store + S3 client +
 * `HttpExec.live` + zio-http `Client.default` + telemetry.
 *
 * Example — single-stream:
 *
 * {{{
 *   object PokeApi extends HttpExloApp:
 *     val id       = "pokeapi"
 *     val s3Config = S3Config(bucket = "my-data")
 *
 *     val streams = List(
 *       new HttpStream:
 *         val name = "pokemon"
 *         def request(s, c)    = HttpExec.get(c.getOrElse("next", "https://pokeapi.co/api/v2/pokemon?offset=0"))
 *         def records(s, c, r) = r.json.field("results").flatMap(_.asArray).getOrElse(Chunk.empty).map(_.toJson)
 *         def nextCtx(s, c, r) = r.json.field("next").flatMap(_.asString).map(url => Map("next" -> url))
 *     )
 * }}}
 */
trait HttpExloApp extends ZIOAppDefault:

  def id:          String
  def version:     String      = "1.0.0"
  def s3Config:    S3Config
  def flushPolicy: FlushPolicy = FlushPolicy.default

  /** All streams this connector exposes. Operator selects one via
   *  `EXLO_STREAM` (or `exlo.stream` config key). */
  def streams: List[HttpStream]

  override def run: ZIO[ZIOAppArgs & Scope, Any, Any] =
    val program = for
      streamName <- ZIO.config(Config.string("stream").nested("exlo"))
                      .mapError(e => ExloError.ConnectorFailure(s"missing or invalid EXLO_STREAM: ${e.getMessage}"))
      selected   <- ZIO.fromOption(streams.find(_.name == streamName))
                      .orElseFail(ExloError.ConnectorFailure(
                        s"unknown stream '$streamName' for connector '$id'; " +
                          s"available: ${streams.map(_.name).mkString(", ")}"
                      ))
      _ <- Exlo.run(selected.asConnector(id, version), streamName, flushPolicy)
    yield ()

    program.provide(
      S3DataSink.layer(s3Config),
      S3StateStore.layer(s3Config),
      S3.layer(s3Config),
      Client.default,
      HttpExec.live,
      Telemetry.auto
    )
