package exlo

import exlo.domain.{Connector, ExloError, Tag}
import exlo.runtime.{DataSink, FlushPolicy, RunContext, Runner, StateStore}
import zio.*
import zio.telemetry.opentelemetry.core.trace.Tracer

/**
 * Framework entry point. Generates a `syncId`, sets the [[RunContext]]
 * FiberRefs (`syncId`, `connectorId`, `connectorVersion`, `streamName`),
 * opens a root tracer span, and delegates to [[Runner.run]].
 *
 * Users extend `ZIOAppDefault` and call from `def run`, providing
 * `DataSink`, `StateStore`, `Tracer`, and the connector's `R` via layers:
 *
 * {{{
 *   object MyApp extends ZIOAppDefault:
 *     def run = Exlo.run(myConnector, "stream-name").provide(
 *       S3DataSink.layer(s3Config),
 *       S3StateStore.layer(s3Config),
 *       S3.layer(s3Config),
 *       Telemetry.auto,
 *       MyService.layer
 *     )
 * }}}
 */
object Exlo:

  def run[O <: Tag, S, R](
      connector:   Connector[O, S, R],
      streamName:  String,
      flushPolicy: FlushPolicy = FlushPolicy.default
  ): ZIO[R & DataSink & StateStore & Tracer, ExloError, Unit] =
    for
      syncId <- Random.nextUUID.map(_.toString)
      sink   <- ZIO.service[DataSink]
      store  <- ZIO.service[StateStore]
      _      <- withContext(syncId, connector, streamName) {
                  Runner.run(connector, streamName, syncId, sink, store, flushPolicy)
                }
    yield ()

  private def withContext[O <: Tag, S, R, A](
      syncId:    String,
      connector: Connector[O, S, R],
      streamName: String
  )(zio: ZIO[R & Tracer, ExloError, A]): ZIO[R & Tracer, ExloError, A] =
    RunContext.withRun(syncId, connector.id, connector.version) {
      RunContext.streamName.locally(streamName) {
        ZIO.logAnnotate("sync_id", syncId) {
          ZIO.logAnnotate("connector", connector.id) {
            ZIO.logAnnotate("stream", streamName) {
              ZIO.serviceWithZIO[Tracer] { tracer =>
                tracer.root(s"connector.run ${connector.id}") { span =>
                  for
                    _      <- span.setAttribute("exlo.sync_id", syncId)
                    _      <- span.setAttribute("exlo.connector_id", connector.id)
                    _      <- span.setAttribute("exlo.connector_version", connector.version)
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
