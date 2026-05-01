package exlo.runtime

import exlo.domain.ExloError
import zio.*
import zio.stream.ZStream

import java.time.Instant

/**
 * A row in the StateStore, keyed by `(connectorId, streamName, key)`.
 *
 * `key` is opaque text — the watermark adapter uses a fixed sentinel
 * (`"$watermark$"`) so simple incremental connectors get one row per stream;
 * keyed connectors derive their own keys (e.g. per-tenant cursor).
 *
 * `value` is the connector's `Codec[S]`-encoded state. The StateStore never
 * inspects it.
 *
 * `committedAt` and `syncId` together resolve "newest wins" when multiple
 * rows share a key: latest committedAt, then syncId as a tiebreaker.
 */
final case class StateRow(
    connector:    String,
    stream:       String,
    key:          String,
    value:        String,
    committedAt:  Instant,
    syncId:       String
)

/**
 * How [[StateStore.scan]] selects rows for a given `(connector, stream)`.
 *
 *   - [[Filter.ByKey]]: strict equality. Used by the watermark adapter and
 *     by per-key consumers reading their own cursor.
 *   - [[Filter.Tail]]: bounded streaming fold over the latest committed rows
 *     by `(committedAt, syncId)`, never materializing the full table.
 *     Use for "last N keys committed" introspection.
 */
sealed trait Filter[+K]
object Filter:
  final case class ByKey[K](key: K)   extends Filter[K]
  final case class Tail(n: Int)       extends Filter[Nothing]:
    require(n > 0, s"Tail(n) must have n > 0, got $n")

/**
 * Tiny key/value/scan store for connector state.
 *
 *   - [[readByKey]]: latest row for `(connector, stream, key)`, or None.
 *   - [[merge]]: write a new row. "Newest committedAt + syncId wins" on read,
 *     so this is effectively last-write-wins per key. Implementations are
 *     free to physically write a new file/object/row each time and let read
 *     resolve the winner — no compaction required for correctness.
 *   - [[scan]]: streaming, bounded-memory. Concrete impls must not buffer
 *     the entire table; they must stream-fold per the contract.
 */
trait StateStore:

  def readByKey(connector: String, stream: String, key: String)
      : IO[ExloError, Option[StateRow]]

  def merge(row: StateRow): IO[ExloError, Unit]

  def scan(connector: String, stream: String, filter: Filter[String])
      : ZStream[Any, ExloError, StateRow]

object StateStore:

  /** Sentinel key used by the watermark adapter when state is global per stream. */
  val WatermarkKey: String = "$watermark$"

  /**
   * In-memory test impl. Backed by a single Ref keyed by
   * `(connector, stream, key)`, holding the latest row by
   * `(committedAt, syncId)`.
   */
  final class InMemory private (ref: Ref[Map[(String, String, String), StateRow]])
      extends StateStore:

    def readByKey(connector: String, stream: String, key: String)
        : IO[ExloError, Option[StateRow]] =
      ref.get.map(_.get((connector, stream, key)))

    def merge(row: StateRow): IO[ExloError, Unit] =
      ref.update { m =>
        val k = (row.connector, row.stream, row.key)
        m.get(k) match
          case None       => m + (k -> row)
          case Some(prev) => if newerThan(row, prev) then m + (k -> row) else m
      }

    def scan(connector: String, stream: String, filter: Filter[String])
        : ZStream[Any, ExloError, StateRow] =
      ZStream.fromZIO(ref.get).flatMap { m =>
        val matching = m.toVector.collect {
          case ((c, s, _), row) if c == connector && s == stream => row
        }
        filter match
          case Filter.ByKey(k) =>
            ZStream.fromIterable(matching.filter(_.key == k))
          case Filter.Tail(n) =>
            val sorted = matching.sortBy(r => (r.committedAt, r.syncId)).reverse.take(n)
            ZStream.fromIterable(sorted)
      }

    /** Test introspection: number of rows currently held. */
    def size: UIO[Int] = ref.get.map(_.size)

  object InMemory:
    def make: UIO[InMemory] =
      Ref.make(Map.empty[(String, String, String), StateRow]).map(new InMemory(_))

  private def newerThan(a: StateRow, b: StateRow): Boolean =
    val cmp = a.committedAt.compareTo(b.committedAt)
    if cmp != 0 then cmp > 0 else a.syncId.compareTo(b.syncId) > 0
