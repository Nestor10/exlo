package exlo

import exlo.domain.ExloError
import exlo.http.{HttpExec, HttpStream}
import exlo.runtime.{DataSink, FlushPolicy, StateStore, Telemetry}
import exlo.s3.{S3, S3Config, S3DataSink, S3StateStore}
import zio.*
import zio.http.Client
import zio.logging.{ConsoleLoggerConfig, consoleJsonLogger}
import zio.telemetry.opentelemetry.core.trace.Tracer

/**
 * Where records and state go. Selected at runtime via the
 * `EXLO_DESTINATION` env var:
 *
 *   - `logging` — records go to ZIO logs, state lives in-memory and
 *     evaporates on exit. Dev mode; no S3 / AWS credentials required.
 *   - anything else (default) — S3DataSink + S3StateStore via
 *     `s3Config`. Production.
 */
private enum Destination:
  case Logging, S3

private object Destination:
  def fromEnv: Destination =
    sys.env.get("EXLO_DESTINATION").map(_.toLowerCase) match
      case Some("logging") => Logging
      case _               => S3

/**
 * Stand-alone ZIO app trait for an HTTP-based connector — one *connector*
 * (the source) exposing one or more *streams* (extractable endpoints).
 *
 * Each entry in [[streams]] is a *pipeline* — either a single root stream
 * or a parent `>>>` child composition. Single streams are implicitly
 * pipelines via a `Conversion`; composition is via the `>>>` extension on
 * `HttpStream[Unit, String]`. Example:
 *
 * {{{
 *   val streams: List[HttpPipeline] = List(
 *     queries,                   // root, dumps queries to sink
 *     queries >>> mentions,      // chain, mentions per query
 *     queries >>> topics
 *   )
 * }}}
 *
 * EXLO_STREAM dispatches by pipeline name (= stream name for roots,
 * child name for chains). The framework looks up the matching pipeline,
 * builds its `Stage`(s), and runs via `Exlo.run` / `Exlo.runChain` with
 * the canonical layer set.
 *
 * The HTTP transport layer is overridable via [[httpExec]] for connectors
 * that need authentication. Default is [[HttpExec.live]] (no auth);
 * brandwatch and similar override with [[HttpExec.bearer]] or future
 * `oauth` decorators.
 *
 * Multi-stage state semantics (Option C, the v0.2 default): parent stages
 * are stateless re-enumerators (`S = Unit`); only child leaves have
 * durable, watermark-gated state. Cross-stage seq propagation is not
 * implemented.
 */
trait HttpExloApp extends ZIOAppDefault:

  def id:          String
  def version:     String      = "1.0.0"
  def s3Config:    S3Config
  def flushPolicy: FlushPolicy = FlushPolicy.default

  /** All streams this connector exposes. EXLO_STREAM dispatches by pipeline name. */
  def streams: List[HttpPipeline]

  /** HTTP transport layer. Override for auth/rate-limit decorators. */
  def httpExec: ZLayer[Client, Nothing, HttpExec] = HttpExec.live

  /** Replace ZIO's default key=value console logger with structured JSON.
   *  Per-line fields include `timestamp`, `level`, `message`, plus every
   *  `logAnnotate` value in scope (`sync_id`, `connector`, `stream`, …). */
  override val bootstrap: ZLayer[Any, Nothing, Unit] =
    Runtime.removeDefaultLoggers >>> consoleJsonLogger(ConsoleLoggerConfig.default)

  override def run: ZIO[ZIOAppArgs & Scope, Any, Any] =
    val program: ZIO[HttpExec & DataSink & StateStore & Tracer, ExloError, Unit] = for
      streamName <- ZIO.config(Config.string("stream").nested("exlo"))
                      .mapError(e =>
                        ExloError.ConnectorFailure(s"missing or invalid EXLO_STREAM: ${e.getMessage}")
                      )
      pipeline <- ZIO.fromOption(streams.find(_.name == streamName))
                    .orElseFail(ExloError.ConnectorFailure(
                      s"unknown stream '$streamName' for connector '$id'; " +
                        s"available: ${streams.map(_.name).mkString(", ")}"
                    ))
      _ <- pipeline.runVia(id, version, flushPolicy)
    yield ()

    Destination.fromEnv match
      case Destination.Logging =>
        program.provide(
          DataSink.loggingLayer,
          StateStore.InMemory.layer,
          Client.default,
          httpExec,
          Telemetry.auto
        )
      case Destination.S3 =>
        program.provide(
          S3DataSink.layer(s3Config),
          S3StateStore.layer(s3Config),
          S3.layer(s3Config),
          Client.default,
          httpExec,
          Telemetry.auto
        )

/**
 * One runnable pipeline entry in an [[HttpExloApp]]. Either a single root
 * stream that emits records straight to the DataSink, or a 2-stage chain
 * where a stateless parent feeds a stateful child.
 *
 * Build single streams via the implicit `Conversion[HttpStream[Unit,
 * String], HttpPipeline]`; build chains via the `>>>` extension on root
 * streams. All records flow as `String` (typically JSON).
 */
sealed trait HttpPipeline:
  /** Pipeline name — used for EXLO_STREAM dispatch. */
  def name: String

  /** Drive the pipeline end-to-end. Called by [[HttpExloApp]] after dispatch. */
  def runVia(connectorId: String, version: String, flushPolicy: FlushPolicy)
      : ZIO[HttpExec & DataSink & StateStore & Tracer, ExloError, Unit]

object HttpPipeline:

  /** Root-only pipeline: `Stage[Unit, String, _, HttpExec]` to DataSink. */
  final case class Root(stream: HttpStream[Unit, String]) extends HttpPipeline:
    val name: String = stream.name
    def runVia(connectorId: String, version: String, flushPolicy: FlushPolicy)
        : ZIO[HttpExec & DataSink & StateStore & Tracer, ExloError, Unit] =
      Exlo.run(connectorId, stream.asStage(version), flushPolicy)

  /** 2-stage chain: parent root + child leaf. Pipeline name = child's name. */
  final case class Chain(
      parent: HttpStream[Unit, String],
      child:  HttpStream[String, String]
  ) extends HttpPipeline:
    val name: String = child.name
    def runVia(connectorId: String, version: String, flushPolicy: FlushPolicy)
        : ZIO[HttpExec & DataSink & StateStore & Tracer, ExloError, Unit] =
      Exlo.runChain(
        connectorId,
        parent.asStage(version),
        child.asStage(version),
        flushPolicy
      )

  /** A root stream is implicitly a Root pipeline. Lets users write
   *  `streams = List(rootStream, rootStream >>> childStream)` without
   *  wrapping. */
  given Conversion[HttpStream[Unit, String], HttpPipeline] = Root(_)

  /** Compose a root with a child: `parent >>> child` builds a Chain. */
  extension (parent: HttpStream[Unit, String])
    def >>>(child: HttpStream[String, String]): HttpPipeline = Chain(parent, child)
