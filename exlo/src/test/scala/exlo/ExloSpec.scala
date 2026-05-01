package exlo

import exlo.domain.{Emission, ExloError, Stage}
import exlo.runtime.{Codec, DataSink, FlushPolicy, RunContext, StateStore}
import zio.*
import zio.stream.ZStream
import zio.test.*

object ExloSpec extends ZIOSpecDefault:

  private def stage(emissions: ZStream[Any, ExloError, Emission[String, Long]]): Stage[Unit, String, Long, Any] =
    new Stage[Unit, String, Long, Any]:
      val id           = "smoke"
      val version      = "1.0.0"
      val initialState = 0L
      def reduce(a: Long, b: Long): Long = a max b
      val codec        = Codec.long
      def run(
          input:  ZStream[Any, ExloError, Unit],
          resume: Long
      ): ZStream[Any, ExloError, Emission[String, Long]] = emissions

  private val fastFlush = FlushPolicy(maxRows = 100, maxInterval = 100.millis)

  def spec = suite("Exlo.run")(
    test("end-to-end: records reach the sink, state commits, RunContext is set") {
      val emissions = ZStream.fromIterable(List(
        Emission.Record("""{"a":1}"""),
        Emission.Record("""{"a":2}"""),
        Emission.Mark(7L)
      ))
      for
        sinkRef  <- DataSink.InMemory.make
        storeRef <- StateStore.InMemory.make
        // Capture FiberRef values inside a tap on the stage's stream.
        observed <- Ref.make(("", "", "", ""))
        captured  = ZStream.fromZIO {
                      for
                        sId  <- RunContext.syncId.get
                        cId  <- RunContext.connectorId.get
                        cVer <- RunContext.connectorVersion.get
                        sn   <- RunContext.streamName.get
                        _    <- observed.set((sId, cId, cVer, sn))
                      yield ()
                    }.drain ++ emissions
        _        <- Exlo.run(stage(captured), "smoke-stream", fastFlush)
                      .provide(
                        ZLayer.succeed[DataSink](sinkRef),
                        ZLayer.succeed[StateStore](storeRef),
                        exlo.runtime.Telemetry.noop
                      )
        rows     <- sinkRef.collected
        state    <- storeRef.readByKey("smoke", "smoke-stream", StateStore.WatermarkKey)
        ctx      <- observed.get
      yield assertTrue(
        rows.map(_.value).toList == List("""{"a":1}""", """{"a":2}"""),
        state.exists(_.value == "7"),
        ctx._2 == "smoke",
        ctx._3 == "1.0.0",
        ctx._4 == "smoke-stream",
        ctx._1.nonEmpty // syncId is generated
      )
    }
  )
