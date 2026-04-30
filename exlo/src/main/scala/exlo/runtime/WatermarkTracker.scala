package exlo.runtime

import zio.*

import scala.collection.immutable.TreeMap

/**
 * Holds [[Pending]] state proposals admitted from the connector and releases
 * them once their seq# is below the data sink's durable watermark.
 *
 *   - `admit` is called from the dispatch fiber every time the connector
 *     emits an `Emission.Mark`.
 *   - `advance(durableSeq)` is called from the runner's flush loop after each
 *     successful `DataSink.write`. It returns all pending whose seq# is now
 *     `<= durableSeq`, in seq order, and removes them from the tracker.
 *
 * Single `Ref[TreeMap[Long, S]]` is sufficient: there is one mutable piece of
 * state, all operations on it are atomic via CAS, and we never need to
 * compose updates across multiple Refs (which is the only case where STM
 * would be required, per zionomicon ch.21).
 */
trait WatermarkTracker[S]:
  def admit(p: Pending[S]): UIO[Unit]
  def advance(durableSeq: Long): UIO[Chunk[Pending[S]]]

object WatermarkTracker:

  def make[S]: UIO[WatermarkTracker[S]] =
    Ref.make(TreeMap.empty[Long, S]).map(new Live[S](_))

  private final class Live[S](ref: Ref[TreeMap[Long, S]]) extends WatermarkTracker[S]:
    def admit(p: Pending[S]): UIO[Unit] =
      ref.update(_ + (p.seq -> p.state))

    def advance(durableSeq: Long): UIO[Chunk[Pending[S]]] =
      ref.modify { tm =>
        // SortedMap.until(k) is exclusive: keys < k. We want keys <= durableSeq,
        // so split at durableSeq + 1.
        val splitAt    = durableSeq + 1L
        val toRelease  = tm.until(splitAt)
        val remaining  = tm.from(splitAt)
        val released   = Chunk.fromIterable(
          toRelease.iterator.map((seq, st) => Pending(seq, st)).toVector
        )
        (released, remaining)
      }
