package exlo.runtime

import zio.*
import zio.test.*

object SinkConfigSpec extends ZIOSpecDefault:

  def spec = suite("SinkConfig env loading")(
    test("reads all three fields under the exlo.sink namespace") {
      val provider = ConfigProvider.fromMap(
        Map(
          "exlo.sink.max_records"     -> "42",
          "exlo.sink.max_interval"    -> "PT5S",
          "exlo.sink.buffer_capacity" -> "500"
        )
      )
      for cfg <- SinkConfig.fromEnv.withConfigProvider(provider)
      yield assertTrue(cfg == SinkConfig(42, 5.seconds, 500))
    },
    test("missing fields fall back to defaults") {
      val provider = ConfigProvider.fromMap(Map("exlo.sink.max_records" -> "100"))
      for cfg <- SinkConfig.fromEnv.withConfigProvider(provider)
      yield assertTrue(
        cfg.maxRecords == 100,
        cfg.maxInterval == SinkConfig.default.maxInterval,
        cfg.bufferCapacity == SinkConfig.default.bufferCapacity
      )
    }
  )
