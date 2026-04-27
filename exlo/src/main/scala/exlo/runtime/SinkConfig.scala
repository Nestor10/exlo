package exlo.runtime

import zio.*

/**
 * Sink flush thresholds. The sink flushes when EITHER condition is met first:
 *   - buffer reaches `maxRecords`, or
 *   - `maxInterval` has elapsed since the last flush.
 *
 * `bufferCapacity` bounds the records TQueue. Workers block on offer when full — natural
 * backpressure when the sink can't keep up. Set well above `maxRecords` so the buffer doesn't
 * become the bottleneck under normal operation.
 */
final case class SinkConfig(
    maxRecords: Int,
    maxInterval: Duration,
    bufferCapacity: Int
)

object SinkConfig:
  val default: SinkConfig = SinkConfig(
    maxRecords     = 1000,
    maxInterval    = 30.seconds,
    bufferCapacity = 10_000
  )

  /** Tight thresholds for tests so behavior is observable in milliseconds. */
  val testing: SinkConfig = SinkConfig(
    maxRecords     = 5,
    maxInterval    = 200.millis,
    bufferCapacity = 1_000
  )

  /**
   * ZIO Config descriptor under the `exlo.sink` namespace. With the default env-var
   * provider, this resolves to:
   *   - `EXLO_SINK_MAX_RECORDS`
   *   - `EXLO_SINK_MAX_INTERVAL` (ISO-8601 duration, e.g. `PT30S`)
   *   - `EXLO_SINK_BUFFER_CAPACITY`
   * Each falls back to the corresponding field of [[default]].
   */
  val config: Config[SinkConfig] =
    val records  = Config.int("max_records").withDefault(default.maxRecords)
    val interval = Config.duration("max_interval").withDefault(default.maxInterval)
    val buffer   = Config.int("buffer_capacity").withDefault(default.bufferCapacity)
    (records ++ interval ++ buffer).nested("sink").nested("exlo").map {
      case (r, i, c) => SinkConfig(r, i, c)
    }

  /** Load [[SinkConfig]] from the ambient `ConfigProvider` (env vars by default). */
  def fromEnv: ZIO[Any, Config.Error, SinkConfig] = ZIO.config(config)
