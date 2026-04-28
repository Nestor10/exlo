package exlo

import exlo.domain.{Connector, ExloError}
import exlo.runtime.{Destination, ExloState, RunContext, Sink, SinkConfig}
import zio.*

/**
 * Framework entry point. Users extend `ZIOAppDefault` and call `Exlo.run` from `def run`,
 * providing their own services and the destination via layers.
 *
 * Example:
 * {{{
 * object MyConnector extends ZIOAppDefault:
 *   def run = Exlo
 *     .run(myConnector, initialState = MyState.zero)
 *     .provide(MyService.live, IcebergDestination.layer[MyState])
 * }}}
 */
object Exlo:

  /**
   * Run a connector to completion.
   *
   *   1. Read state from `Destination` (or use `initialState` on cold start).
   *   2. Build [[Sink]] + [[ExloState]] over a shared `TRef[S]` / `TQueue[String]`.
   *   3. Fork the sink's drain-and-commit loop.
   *   4. Drain the connector's `emit` stream — records flow via `ExloState.emit`.
   *   5. On stream end, run a final flush to commit any pending records + final state.
   */
  def run[S: Tag, R, E <: Throwable](
      connector: Connector[S, R, E],
      initialState: => S,
      sinkConfig: SinkConfig = SinkConfig.default
  ): ZIO[R & Destination[S], Throwable, Unit] =
    val core = for
      dest      <- ZIO.service[Destination[S]]
      sinkAndSt <- Sink.make[S](initialState, dest, sinkConfig)
      (sink, stateService) = sinkAndSt
      _ <- runWithSink(connector, sink, stateService)
    yield ()

    for
      syncId <- Random.nextUUID.map(_.toString)
      _ <- RunContext.withRun(syncId, connector.id, connector.version) {
             ZIO.logAnnotate("sync_id", syncId) {
               ZIO.logAnnotate("connector", connector.id) {
                 ZIO.logAnnotate("version", connector.version) {
                   ZIO.logInfo("starting connector run") *>
                     core.tapErrorCause(c => ZIO.logErrorCause("connector run failed", c)) *>
                     ZIO.logInfo("connector run completed")
                 }
               }
             }
           }
    yield ()

  private def runWithSink[S: Tag, R, E <: Throwable](
      connector: Connector[S, R, E],
      sink: Sink[S],
      stateService: ExloState[S]
  ): ZIO[R, Throwable, Unit] =
    val drain: ZIO[R & ExloState[S], Throwable, Unit] = connector.emit.runDrain
    val raced: ZIO[R & ExloState[S], Throwable, Unit] =
      drain.raceFirst(sink.runLoop)

    raced
      .ensuring(sink.shutdown.orDie)
      .provideSomeEnvironment[R](_.add[ExloState[S]](stateService))
