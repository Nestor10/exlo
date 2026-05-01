package exlo.runtime

import exlo.domain.{Connector, ExloError, Tag}
import zio.{Tag as ZTag, *}
import zio.stream.ZStream

/**
 * Wire a parent connector as a `Source[O]` for a child connector, where `O`
 * is the parent's output tag.
 *
 * Layer composition handles the type math: applying multiple `FedBy` layers
 * to the same child satisfies multiple `Source[T]` requirements at once,
 * because the phantom tags `T` are distinct types and ZIO's env intersects
 * them. ZLayer memoization gives free fan-out: if two children reference the
 * same parent, the parent's runner runs once.
 *
 * v1 caveats:
 *   - The parent's records flow into a bounded queue feeding the child. They
 *     are not separately persisted — the child consumes them as the parent
 *     produces them.
 *   - Because the queue is volatile, the parent's StateStore commits would
 *     create a false "I'm done with these records" claim if the child hadn't
 *     consumed them yet. Therefore: the parent in FedBy uses a NoopStateStore
 *     internally and always cold-starts. Suitable for parents that are cheap
 *     to re-enumerate (HTTP list endpoints). For expensive incremental
 *     parents, run them as standalone connectors with persistent sinks and
 *     point the child at that persisted output via a different Source impl.
 */
object FedBy:

  def apply[O <: Tag, Sp, Rp](
      parent:         Connector[O, Sp, Rp],
      bufferCapacity: Int = 1024
  )(using ZTag[Source[O]]): ZLayer[Rp, ExloError, Source[O]] =
    ZLayer.scoped[Rp] {
      for
        queue <- ZIO.acquireRelease(
                   Queue.bounded[String](bufferCapacity)
                 )(_.shutdown)
        sink   = new QueueDataSink(queue)
        store  = NoopStateStore
        syncId <- Random.nextUUID.map(_.toString)
        _      <- (Runner.run(parent, "fedby", syncId, sink, store) *> queue.shutdown)
                    .forkScoped
      yield new Source[O]:
        def stream: ZStream[Any, ExloError, String] = ZStream.fromQueueWithShutdown(queue)
    }

  /** DataSink whose `write` offers each record's value to a bounded queue. */
  private final class QueueDataSink(queue: Queue[String]) extends DataSink:
    def write(batch: Chunk[Sequenced]): IO[ExloError, Long] =
      if batch.isEmpty then ZIO.succeed(0L)
      else
        queue
          .offerAll(batch.map(_.value))
          .as(batch.map(_.seq).max)

  /** Discards all writes; reads return None. Used for FedBy's parent. */
  private object NoopStateStore extends StateStore:
    def readByKey(c: String, s: String, k: String): IO[ExloError, Option[StateRow]] =
      ZIO.none
    def merge(row: StateRow): IO[ExloError, Unit] =
      ZIO.unit
    def scan(c: String, s: String, f: Filter[String]): ZStream[Any, ExloError, StateRow] =
      ZStream.empty
