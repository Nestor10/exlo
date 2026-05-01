package exlo

import exlo.domain.{ExloError, Stage}
import exlo.runtime.{DataSink, FlushPolicy, RunContext, Runner, StateStore}
import zio.*
import zio.telemetry.opentelemetry.core.trace.Tracer

/**
 * Framework entry point. Generates a `syncId`, sets the [[RunContext]]
 * FiberRefs (`syncId`, `connectorId`, `connectorVersion`, `streamName`),
 * opens a root tracer span, and delegates to [[Runner.run]].
 *
 * Single-stage entry point — the [[Stage]] is treated as a leaf: input is
 * `Unit` (kicked off by an empty-stream marker), output is `String`
 * (records flow to `DataSink`). Multi-stage chaining helpers will land
 * alongside the first connector that needs them.
 *
 * Users extend `ZIOAppDefault` and call from `def run`, providing
 * `DataSink`, `StateStore`, `Tracer`, and the stage's `R` via layers:
 *
 * {{{
 *   object MyApp extends ZIOAppDefault:
 *     def run = Exlo.run(myStage, "stream-name").provide(
 *       S3DataSink.layer(s3Config),
 *       S3StateStore.layer(s3Config),
 *       S3.layer(s3Config),
 *       Telemetry.auto,
 *       MyService.layer
 *     )
 * }}}
 */
object Exlo:

  def run[S, R](
      stage:       Stage[Unit, String, S, R],
      streamName:  String,
      flushPolicy: FlushPolicy = FlushPolicy.default
  ): ZIO[R & DataSink & StateStore & Tracer, ExloError, Unit] =
    for
      syncId <- Random.nextUUID.map(_.toString)
      sink   <- ZIO.service[DataSink]
      store  <- ZIO.service[StateStore]
      _      <- withContext(syncId, stage, streamName) {
                  Runner.run(stage, streamName, syncId, sink, store, flushPolicy)
                }
    yield ()

  private def withContext[S, R, A](
      syncId:     String,
      stage:      Stage[?, ?, S, R],
      streamName: String
  )(zio: ZIO[R & Tracer, ExloError, A]): ZIO[R & Tracer, ExloError, A] =
    RunContext.withRun(syncId, stage.id, stage.version) {
      RunContext.streamName.locally(streamName) {
        ZIO.logAnnotate("sync_id", syncId) {
          ZIO.logAnnotate("connector", stage.id) {
            ZIO.logAnnotate("stream", streamName) {
              ZIO.serviceWithZIO[Tracer] { tracer =>
                tracer.root(s"connector.run ${stage.id}") { span =>
                  for
                    _      <- span.setAttribute("exlo.sync_id", syncId)
                    _      <- span.setAttribute("exlo.connector_id", stage.id)
                    _      <- span.setAttribute("exlo.connector_version", stage.version)
                    _      <- span.setAttribute("exlo.stream", streamName)
                    _      <- ZIO.logInfo("connector run start")
                    result <- zio.tapErrorCause(c => ZIO.logErrorCause("connector run failed", c))
                    _      <- ZIO.logInfo("connector run done")
                  yield result
                }
              }
            }
          }
        }
      }
    }
