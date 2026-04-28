package exlo.runtime.iceberg

import exlo.domain.ExloError
import org.apache.iceberg.{FileScanTask, Table}
import org.apache.iceberg.data.{GenericRecord, Record}
import org.apache.iceberg.data.parquet.GenericParquetReaders
import org.apache.iceberg.io.CloseableIterable
import org.apache.iceberg.parquet.Parquet
import zio.*
import zio.stream.ZStream

import scala.jdk.CollectionConverters.*

/**
 * Read records from a parent connector's Iceberg destination, incrementally by snapshot.
 *
 * The parent→child handoff pattern in `exlo`:
 *   - Parent connector commits records + state to its destination, exits.
 *   - Child connector, in its own subsequent run, calls [[readIncremental]] passing the
 *     last parent snapshot ID it processed (stored in the child's own state).
 *   - Child gets a stream of records added in snapshots strictly after `from`. It uses
 *     [[latestSnapshot]] to capture the new high-water mark before processing, persisting
 *     it as the next cursor on success.
 *
 * Resource discipline: the underlying Iceberg `CloseableIterable` (planFiles + per-file
 * Parquet readers) is wrapped in `ZStream.unwrapScoped` + `ZIO.fromAutoCloseable`, so file
 * descriptors are released even if the downstream stream fails or is interrupted.
 *
 * Failure modes:
 *   - If `from` is no longer an ancestor of the current snapshot (e.g., expired by
 *     `expire_snapshots`), the stream fails with [[ExloError.SnapshotExpired]]. This
 *     surfaces a loud signal to the orchestration layer; the child cannot self-recover.
 *   - Empty parent table (`currentSnapshot == null`): both `latestSnapshot` returns `None`
 *     and `readIncremental(None)` returns an empty stream — the child's `slices` enumeration
 *     simply produces nothing this run.
 *   - Bootstrap (child has no `from`): `readIncremental(None)` performs a FULL table scan
 *     across all current data files. Use sparingly for backfills; once a cursor is captured,
 *     subsequent runs should always pass `Some(snapshotId)`.
 */
trait IcebergDataSource:

  type SnapshotRef = Long

  /** The latest snapshot ID currently in the source table, or None if the table is empty. */
  def latestSnapshot: IO[ExloError, Option[SnapshotRef]]

  /**
   * Stream the records added since `from`. If `from` is `None`, performs a full table scan.
   * Records are emitted as opaque payload strings (the schema's single `payload` column).
   */
  def readIncremental(from: Option[SnapshotRef]): ZStream[Any, ExloError, String]

object IcebergDataSource:

  /** Build a data source over an already-loaded Iceberg `Table`. */
  def fromTable(table: Table): IcebergDataSource = new Impl(table)

  // ---- Impl ------------------------------------------------------------------------------

  private final class Impl(table: Table) extends IcebergDataSource:

    def latestSnapshot: IO[ExloError, Option[Long]] =
      ZIO
        .attemptBlocking {
          table.refresh()
          Option(table.currentSnapshot()).map(_.snapshotId())
        }
        .mapError(t => ExloError.StorageError("iceberg latestSnapshot failed", t))

    def readIncremental(from: Option[Long]): ZStream[Any, ExloError, String] =
      planTasksScoped(from)
        .flatMap(readTaskScoped)
        .mapError(refineError)

    /**
     * Scoped plan of FileScanTasks for either incremental (some snapshot) or full (none).
     * The CloseableIterable returned by Iceberg is registered with the scope so it's closed
     * regardless of downstream completion.
     */
    private def planTasksScoped(from: Option[Long]): ZStream[Any, Throwable, FileScanTask] =
      ZStream.unwrapScoped {
        ZIO
          .fromAutoCloseable {
            ZIO.attemptBlocking {
              table.refresh()
              val current = Option(table.currentSnapshot()).map(_.snapshotId())
              (from, current) match
                // Empty source table, no cursor → nothing to read.
                case (None, None) =>
                  CloseableIterable.empty[FileScanTask]()
                // Empty source table, but caller has a cursor → cursor is bogus / expired.
                case (Some(id), None) =>
                  throw new IllegalArgumentException(
                    s"Starting snapshot $id is not a parent ancestor of end snapshot " +
                      "(source table currently has no snapshots)"
                  )
                // Bootstrap / full scan: caller has no cursor.
                case (None, Some(_)) =>
                  table.newScan().planFiles()
                // Caller is caught up — already at the latest snapshot. Nothing new.
                case (Some(fromId), Some(currentId)) if fromId == currentId =>
                  CloseableIterable.empty[FileScanTask]()
                // Normal incremental case.
                case (Some(fromId), Some(_)) =>
                  table
                    .newIncrementalAppendScan()
                    .fromSnapshotExclusive(fromId)
                    .planFiles()
            }
          }
          .map(it => ZStream.fromIterator(it.iterator().asScala))
      }

    /** Read records from a single FileScanTask as a Scoped ZStream. */
    private def readTaskScoped(task: FileScanTask): ZStream[Any, Throwable, String] =
      ZStream.unwrapScoped {
        ZIO
          .fromAutoCloseable {
            ZIO.attemptBlocking {
              val inputFile = table.io().newInputFile(task.file().location())
              Parquet
                .read(inputFile)
                .project(table.schema())
                .createReaderFunc((fileSchema: org.apache.parquet.schema.MessageType) =>
                  GenericParquetReaders.buildReader(table.schema(), fileSchema)
                )
                .filter(task.residual())          // push down manifest-level filters
                .split(task.start(), task.length()) // honor split offsets
                .build()
                .asInstanceOf[CloseableIterable[Record]]
            }
          }
          .map { iterable =>
            ZStream
              .fromIterator(iterable.iterator().asScala)
              .map {
                case g: GenericRecord =>
                  g.getField("payload") match
                    case s: String => s
                    case other     =>
                      throw new RuntimeException(
                        s"expected String payload column, got ${Option(other).map(_.getClass.getName).getOrElse("null")}"
                      )
                case other =>
                  throw new RuntimeException(
                    s"expected GenericRecord, got ${other.getClass.getName}"
                  )
              }
          }
      }

    /** Iceberg surfaces "expired starting snapshot" as a generic IllegalArgumentException
     *  with the message "Starting snapshot (exclusive) X is not a parent ancestor of
     *  end snapshot Y". Lift it into our typed [[ExloError.SnapshotExpired]]. */
    private def refineError(t: Throwable): ExloError = t match
      case e: IllegalArgumentException
          if Option(e.getMessage).exists(_.contains("is not a parent ancestor")) =>
        // Best-effort: extract the starting snapshot ID from the message; default to -1.
        val id = Option(e.getMessage)
          .flatMap("""Starting snapshot[^\d-]*(-?\d+)""".r.findFirstMatchIn)
          .flatMap(m => m.group(1).toLongOption)
          .getOrElse(-1L)
        ExloError.SnapshotExpired(id, e)
      case e: ExloError => e
      case other        => ExloError.StorageError(s"iceberg readIncremental failed: ${other.getMessage}", other)
