package exlo.runtime

import exlo.domain.ExloError
import zio.*

/**
 * The runner's data path: write a batch of sequenced records and return the
 * highest seq# now durable. The returned watermark is what the runner feeds
 * to [[WatermarkTracker.advance]] to release pending state marks.
 *
 * Why a tiny custom trait instead of `ZSink`: batching cadence is the
 * runner's concern (driven by [[FlushPolicy]] via `ZStream.groupedWithin`),
 * not the sink's. A sink just promises that when `write` returns, every
 * record in the batch is durable. Dropping ZSink keeps the contract one
 * line; implementations remain trivial.
 *
 * Implementations:
 *   - [[DataSink.InMemory]]: in-process test impl, accumulates in a Ref.
 *   - exlo.runtime.s3.S3DataSink: JSONL+gzip uploaded to S3 per write.
 *
 * Empty batches are tolerated: the contract returns the current durable
 * watermark unchanged. Runners typically short-circuit empty batches anyway
 * via `ZStream.groupedWithin`, which only emits non-empty groups.
 */
trait DataSink:
  def write(batch: Chunk[Sequenced]): IO[ExloError, Long]

object DataSink:

  /**
   * In-memory test sink. Accumulates every record across every write; assert
   * the collected stream from tests via [[InMemory.collected]].
   */
  final class InMemory private (
      records:        Ref[Chunk[Sequenced]],
      durableSeqRef:  Ref[Long]
  ) extends DataSink:

    def write(batch: Chunk[Sequenced]): IO[ExloError, Long] =
      if batch.isEmpty then durableSeqRef.get
      else
        val newMax = batch.map(_.seq).max
        records.update(_ ++ batch) *>
          durableSeqRef.updateAndGet(_ max newMax)

    /** All records ever written, in write order. */
    def collected: UIO[Chunk[Sequenced]] = records.get

    /** Highest seq# durable so far. */
    def durable: UIO[Long] = durableSeqRef.get

  object InMemory:
    def make: UIO[InMemory] =
      for
        rs <- Ref.make(Chunk.empty[Sequenced])
        ds <- Ref.make(0L)
      yield new InMemory(rs, ds)
