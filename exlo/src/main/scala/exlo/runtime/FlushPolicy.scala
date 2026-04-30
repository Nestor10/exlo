package exlo.runtime

import zio.Duration

/**
 * When the runner flushes a batch to the [[DataSink]]. Two triggers:
 *
 *   - `maxRows`: emit the buffered group as soon as it reaches this many
 *     records.
 *   - `maxInterval`: emit the buffered group after this much time has elapsed
 *     since the first record arrived, even if `maxRows` hasn't been reached.
 *
 * The runner translates this directly into `ZStream.groupedWithin(maxRows,
 * maxInterval)` — one chunk per flush boundary.
 */
final case class FlushPolicy(maxRows: Int, maxInterval: Duration):
  require(maxRows > 0, s"maxRows must be > 0, got $maxRows")
  require(maxInterval.toNanos > 0L, s"maxInterval must be > 0, got $maxInterval")

object FlushPolicy:
  /** 1000 records or 30 seconds, whichever comes first. */
  val default: FlushPolicy = FlushPolicy(maxRows = 1000, maxInterval = Duration.fromSeconds(30))
