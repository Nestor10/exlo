package exlo.runtime

import exlo.domain.ExloError
import zio.*

/**
 * Demo / dev `Destination` that logs each record + commit. No persistence — proves the
 * framework moves data end-to-end. Switch to [[exlo.runtime.iceberg.IcebergDestination]]
 * via `EXLO_DESTINATION=iceberg` when you want real durability.
 */
final class LoggingDestination[S](
    connectorId: String,
    pending: Ref[Chunk[String]],
    totalRef: Ref[Long]
) extends Destination[S]:

  def writeRecords(records: Chunk[String]): IO[ExloError, Unit] =
    pending.update(_ ++ records) *> ZIO.foreachDiscard(records) { r =>
      ZIO.logInfo(s"[$connectorId] record: ${truncate(r, 200)}")
    }

  def commit(state: S): IO[ExloError, Unit] =
    pending.getAndSet(Chunk.empty).flatMap { records =>
      for
        total <- totalRef.updateAndGet(_ + records.length)
        _ <- ZIO.logInfo(
               s"[$connectorId] commit: ${records.length} records, state=$state (total committed: $total)"
             )
      yield ()
    }

  /** No persistence — every run starts cold. */
  def readState: IO[ExloError, Option[S]] = ZIO.succeed(None)

  def total: UIO[Long] = totalRef.get

  private def truncate(s: String, n: Int): String =
    if s.length <= n then s else s.take(n) + s"… (+${s.length - n} chars)"

object LoggingDestination:
  def make[S](connectorId: String): UIO[LoggingDestination[S]] =
    for
      p     <- Ref.make(Chunk.empty[String])
      total <- Ref.make(0L)
    yield new LoggingDestination[S](connectorId, p, total)

  def layer[S: Tag](connectorId: String): ULayer[Destination[S] & LoggingDestination[S]] =
    ZLayer.fromZIOEnvironment {
      make[S](connectorId).map(impl =>
        ZEnvironment[Destination[S]](impl).add[LoggingDestination[S]](impl)
      )
    }
