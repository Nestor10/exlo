package exlo.runtime.iceberg

import exlo.domain.ExloError
import exlo.runtime.{Destination, RunContext}
import org.apache.iceberg.*
import org.apache.iceberg.catalog.{Catalog, TableIdentifier}
import org.apache.iceberg.data.GenericRecord
import org.apache.iceberg.data.parquet.GenericParquetWriter
import org.apache.iceberg.io.DataWriter
import org.apache.iceberg.parquet.Parquet
import org.apache.iceberg.types.Types
import zio.*
import zio.json.JsonCodec

import java.time.{OffsetDateTime, ZoneOffset}
import java.util.UUID
import scala.jdk.CollectionConverters.*

/**
 * Iceberg-backed destination — the platform's happy path.
 *
 *   - `writeRecords` writes a Parquet `DataFile` against the table (uncommitted) and
 *     appends to an in-memory pending list. Memory stays bounded — records flow out as
 *     Parquet files in S3/local FS as soon as they're staged.
 *   - `commit` does one Iceberg `AppendFiles` transaction: appends all pending DataFiles,
 *     sets the connector's serialized state as a snapshot property, commits. Atomic by
 *     Iceberg's own guarantee.
 *   - `readState` reads the latest snapshot's `exlo.state` property and decodes via
 *     the connector's `JsonCodec[S]`.
 *
 * Schema is fixed: one column `payload: STRING`. Records are opaque to the framework —
 * connector authors emit JSON strings, downstream queries cast and parse as needed.
 *
 * For multi-connector DAGs (parent→child handoff), child connectors read this destination's
 * snapshots incrementally via `IcebergDataSource` (Phase 11.2).
 */
final class IcebergDestination[S: JsonCodec](
    table: Table,
    pending: Ref[List[DataFile]]
) extends Destination[S]:
  import IcebergDestination.*

  def writeRecords(records: Chunk[String]): IO[ExloError, Unit] =
    if records.isEmpty then ZIO.unit
    else
      RunContext.snapshot.flatMap { case (syncId, connectorId, connectorVersion) =>
        ZIO
          .attemptBlocking(
            writeOneParquetFile(table, records, syncId, connectorId, connectorVersion)
          )
          .flatMap(file => pending.update(_ :+ file))
          .mapError(t => ExloError.StorageError("iceberg writeRecords failed", t))
      }

  def commit(state: S): IO[ExloError, Unit] =
    pending.getAndSet(Nil).flatMap { files =>
      ZIO
        .attempt {
          val append = table.newAppend()
          files.foreach(append.appendFile)
          val stateJson = summon[JsonCodec[S]].encoder.encodeJson(state, None).toString
          append.set(stateProperty, stateJson)
          append.commit()
        }
        .mapError(t => ExloError.StorageError("iceberg commit failed", t))
    }

  def readState: IO[ExloError, Option[S]] =
    ZIO
      .attempt {
        // Refresh first to pick up any catalog-level updates (e.g., a previous run committed
        // from a different process).
        table.refresh()
        Option(table.currentSnapshot()).flatMap { snap =>
          Option(snap.summary().get(stateProperty)).flatMap { json =>
            summon[JsonCodec[S]].decoder.decodeJson(json).toOption
          }
        }
      }
      .mapError(t => ExloError.StorageError("iceberg readState failed", t))

object IcebergDestination:

  /**
   * Schema: connector payload + framework-managed operational metadata.
   *
   *   - `payload`: opaque connector record (typically JSON-stringified).
   *   - `exlo_recorded_at`: when the framework staged this batch into Iceberg.
   *   - `exlo_sync_id`: per-run UUID, ties rows to log lines tagged with `sync_id`.
   *   - `exlo_connector`: connector id (e.g. `pokeapi-kalos`).
   *   - `exlo_connector_version`: connector's semver — useful for debugging behavior
   *     differences across deploys.
   *
   * Field IDs are stable; adding new fields means appending with a higher ID.
   */
  val schema: Schema = new Schema(
    Types.NestedField.required(1, "payload",                Types.StringType.get()),
    Types.NestedField.required(2, "exlo_recorded_at",       Types.TimestampType.withZone()),
    Types.NestedField.required(3, "exlo_sync_id",           Types.StringType.get()),
    Types.NestedField.required(4, "exlo_connector",         Types.StringType.get()),
    Types.NestedField.required(5, "exlo_connector_version", Types.StringType.get())
  )

  val partitionSpec: PartitionSpec = PartitionSpec.unpartitioned()

  /** Snapshot summary property key holding the connector's JSON-encoded state. */
  val stateProperty: String = "exlo.state"

  /** Load or create the destination's Iceberg table, then build the destination. */
  def make[S: JsonCodec](
      catalog: Catalog,
      tableIdentifier: TableIdentifier
  ): Task[IcebergDestination[S]] =
    for
      table <- ZIO.attemptBlocking {
                 if catalog.tableExists(tableIdentifier) then catalog.loadTable(tableIdentifier)
                 else catalog.createTable(tableIdentifier, schema, partitionSpec)
               }
      pending <- Ref.make(List.empty[DataFile])
    yield new IcebergDestination[S](table, pending)

  /**
   * Build a destination over an already-loaded `Table`. Useful for tests that construct
   * the table via `HadoopTables` directly, or for code paths that want to manage the
   * `Catalog` lifecycle separately from the destination.
   */
  def fromTable[S: JsonCodec](table: Table): UIO[IcebergDestination[S]] =
    Ref.make(List.empty[DataFile]).map(new IcebergDestination[S](table, _))

  /**
   * Write a single Parquet file against the table. All records in the batch share one
   * `exlo_recorded_at` timestamp (the moment the batch was staged) and the run-context
   * fields (sync_id, connector, version).
   */
  private def writeOneParquetFile(
      table: Table,
      records: Chunk[String],
      syncId: String,
      connectorId: String,
      connectorVersion: String
  ): DataFile =
    val recordedAt = OffsetDateTime.now(ZoneOffset.UTC)

    val outFile = table
      .io()
      .newOutputFile(
        table.locationProvider().newDataLocation(s"exlo-${UUID.randomUUID()}.parquet")
      )

    val writer: DataWriter[GenericRecord] = Parquet
      .writeData(outFile)
      .schema(table.schema())
      // Iceberg 1.10 renamed `buildWriter(MessageType)` to `create(Schema, MessageType)`.
      .createWriterFunc((msg: org.apache.parquet.schema.MessageType) =>
        GenericParquetWriter.create(table.schema(), msg)
      )
      .overwrite()
      .withSpec(partitionSpec)
      .build()

    try
      records.foreach { r =>
        val gr = GenericRecord.create(table.schema())
        gr.setField("payload",                r)
        gr.setField("exlo_recorded_at",       recordedAt)
        gr.setField("exlo_sync_id",           syncId)
        gr.setField("exlo_connector",         connectorId)
        gr.setField("exlo_connector_version", connectorVersion)
        writer.write(gr)
      }
    finally writer.close()

    writer.toDataFile()
