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
 * `connectorId` and `stage.id` are distinct: connectorId is the source
 * (e.g., `"brandwatch"`), stage.id is the stream within (e.g.,
 * `"mentions"`). Both ride through to S3 paths and StateStore keys as
 * `connector=…/stream=…/`.
 */
object Exlo:

  /** Run a single (root + leaf) stage. */
  def run[S, R](
      connectorId: String,
      stage:       Stage[Unit, String, S, R],
      flushPolicy: FlushPolicy = FlushPolicy.default
  ): ZIO[R & DataSink & StateStore & Tracer, ExloError, Unit] =
    for
      syncId <- Random.nextUUID.map(_.toString)
      sink   <- ZIO.service[DataSink]
      store  <- ZIO.service[StateStore]
      _      <- withContext(syncId, connectorId, stage.id, stage.version) {
                  Runner.run(connectorId, stage, syncId, sink, store, flushPolicy)
                }
    yield ()

  /**
   * Run a 2-stage chain (parent + child) end-to-end. The
   * `RunContext.streamName` and tracer span are keyed off the **child's**
   * id since that's the stream the user selected via `EXLO_STREAM`.
   */
  def runChain[M, S0, S1, R](
      connectorId: String,
      parent:      Stage[Unit, M, S0, R],
      child:       Stage[M, String, S1, R],
      flushPolicy: FlushPolicy = FlushPolicy.default
  ): ZIO[R & DataSink & StateStore & Tracer, ExloError, Unit] =
    for
      syncId <- Random.nextUUID.map(_.toString)
      sink   <- ZIO.service[DataSink]
      store  <- ZIO.service[StateStore]
      _      <- withContext(syncId, connectorId, child.id, child.version) {
                  Runner.runChain(connectorId, parent, child, syncId, sink, store, flushPolicy)
                }
    yield ()

  private def withContext[R, A](
      syncId:      String,
      connectorId: String,
      streamName:  String,
      version:     String
  )(zio: ZIO[R & Tracer, ExloError, A]): ZIO[R & Tracer, ExloError, A] =
    RunContext.withRun(syncId, connectorId, version) {
      RunContext.streamName.locally(streamName) {
        ZIO.logAnnotate("sync_id", syncId) {
          ZIO.logAnnotate("connector", connectorId) {
            ZIO.logAnnotate("stream", streamName) {
              ZIO.serviceWithZIO[Tracer] { tracer =>
                tracer.root(s"connector.run $connectorId/$streamName") { span =>
                  for
                    _      <- span.setAttribute("exlo.sync_id", syncId)
                    _      <- span.setAttribute("exlo.connector_id", connectorId)
                    _      <- span.setAttribute("exlo.connector_version", version)
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
