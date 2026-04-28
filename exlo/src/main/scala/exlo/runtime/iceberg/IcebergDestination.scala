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

import java.time.{Instant, OffsetDateTime, ZoneOffset}
import java.util.UUID
import scala.jdk.CollectionConverters.*

/**
 * Iceberg-backed destination — the platform's happy path.
 *
 *   - `writeRecords` writes a Parquet `DataFile` against the table (uncommitted) and
 *     appends to an in-memory pending list. Memory stays bounded — records flow out as
 *     Parquet files in S3/local FS as soon as they're staged.
 *   - `commit` does one Iceberg `AppendFiles` transaction on the data table (data files
 *     only — no state in snapshot summary), then appends one row to the sidecar
 *     [[StateStore]] table. Crash between the two steps yields at-least-once duplicates
 *     in the data table; that is the contract.
 *   - `readState` queries [[StateStore]] for the latest checkpoint for this connector+stream.
 *     On cold start (no sidecar row) it checks the data table's current snapshot summary
 *     for the legacy `exlo.state` key and performs a one-shot bootstrap migration.
 *
 * Schema is fixed: one column `payload: STRING`. Records are opaque to the framework —
 * connector authors emit JSON strings, downstream queries cast and parse as needed.
 *
 * For multi-connector DAGs (parent→child handoff), child connectors read this destination's
 * snapshots incrementally via `IcebergDataSource` (Phase 11.2).
 */
final class IcebergDestination[S: JsonCodec](
    table: Table,
    pending: Ref[List[DataFile]],
    stateStore: StateStore
) extends Destination[S]:
  import IcebergDestination.*

  def writeRecords(records: Chunk[String]): IO[ExloError, Unit] =
    if records.isEmpty then ZIO.unit
    else
      RunContext.snapshot.flatMap { case (syncId, connectorId, connectorVersion, streamName) =>
        ZIO
          .attemptBlocking(
            writeOneParquetFile(table, records, syncId, connectorId, connectorVersion, streamName)
          )
          .flatMap(file => pending.update(_ :+ file))
          .mapError(t => ExloError.StorageError("iceberg writeRecords failed", t))
      }

  def commit(state: S): IO[ExloError, Unit] =
    for
      files <- pending.getAndSet(Nil)
      // 1. Commit data files to the data table (no state in snapshot summary).
      _ <- ZIO.when(files.nonEmpty) {
             ZIO
               .attempt {
                 val append = table.newAppend()
                 files.foreach(append.appendFile)
                 append.commit()
               }
               .mapError(t => ExloError.StorageError("iceberg commit failed", t))
           }
      // 2. Append state to sidecar.
      // Crash between step 1 and step 2 → next run resumes from older state →
      // duplicates in data table. At-least-once by design.
      (syncId, connector, _, stream) <- RunContext.snapshot
      stateJson = summon[JsonCodec[S]].encoder.encodeJson(state, None).toString
      row = StateRow(
              syncId              = syncId,
              connector           = connector,
              stream              = stream,
              stateVersion        = 0L,
              connectorConfigHash = "",
              streamConfigHash    = "",
              state               = stateJson,
              committedAt         = Instant.now()
            )
      _ <- stateStore.append(row)
    yield ()

  def readState: IO[ExloError, Option[S]] =
    for
      connector <- RunContext.connectorId.get
      stream    <- RunContext.streamName.get
      rowOpt    <- stateStore.readLatest(connector, stream).flatMap {
                     case some @ Some(_) => ZIO.succeed(some)
                     case None           => bootstrapMigrate(connector, stream)
                   }
    yield rowOpt.flatMap(row => summon[JsonCodec[S]].decoder.decodeJson(row.state).toOption)

  /**
   * One-shot bootstrap migration: if the sidecar has no row for this stream but the data
   * table's current snapshot carries the legacy `exlo.state` summary key, hydrate one
   * sidecar row from it and return it. After hydration the sidecar is authoritative and
   * this path is never taken again.
   */
  private def bootstrapMigrate(connector: String, stream: String): IO[ExloError, Option[StateRow]] =
    ZIO
      .attemptBlocking {
        table.refresh()
        Option(table.currentSnapshot())
          .flatMap(snap => Option(snap.summary().get(stateProperty)))
      }
      .mapError(t => ExloError.StorageError("iceberg bootstrap migration failed", t))
      .flatMap {
        case None => ZIO.succeed(None)
        case Some(stateJson) =>
          val row = StateRow(
            syncId              = Ulid.generate(),
            connector           = connector,
            stream              = stream,
            stateVersion        = 0L,
            connectorConfigHash = "",
            streamConfigHash    = "",
            state               = stateJson,
            committedAt         = Instant.now()
          )
          stateStore.append(row).as(Some(row))
      }

object IcebergDestination:

  /**
   * Schema: connector payload + framework-managed operational metadata.
   *
   *   - `payload`: opaque connector record (typically JSON-stringified).
   *   - `exlo_recorded_at`: when the framework staged this batch into Iceberg.
   *   - `exlo_sync_id`: per-run UUID, ties rows to log lines tagged with `sync_id`.
   *   - `exlo_connector`: connector id / source (e.g. `zendesk`, `pokeapi-kalos`).
   *   - `exlo_connector_version`: connector's semver.
   *   - `exlo_stream`: stream name within the connector source (e.g. `tickets`,
   *     `ticket_metrics`, `kalos`). Always populated — every connector ships through a
   *     `StreamRegistry` and selects its stream via `EXLO_STREAM`.
   *
   * Field IDs are stable; adding new fields means appending with a higher ID.
   */
  val schema: Schema = new Schema(
    Types.NestedField.required(1, "payload",                Types.StringType.get()),
    Types.NestedField.required(2, "exlo_recorded_at",       Types.TimestampType.withZone()),
    Types.NestedField.required(3, "exlo_sync_id",           Types.StringType.get()),
    Types.NestedField.required(4, "exlo_connector",         Types.StringType.get()),
    Types.NestedField.required(5, "exlo_connector_version", Types.StringType.get()),
    Types.NestedField.required(6, "exlo_stream",            Types.StringType.get())
  )

  val partitionSpec: PartitionSpec = PartitionSpec.unpartitioned()

  /** Snapshot summary property key — **read-only** in the bootstrap migration path. */
  val stateProperty: String = "exlo.state"

  /** Load or create the destination's Iceberg table, then build the destination. */
  def make[S: JsonCodec](
      catalog: Catalog,
      tableIdentifier: TableIdentifier,
      stateStore: StateStore
  ): Task[IcebergDestination[S]] =
    for
      table <- ZIO.attemptBlocking {
                 if catalog.tableExists(tableIdentifier) then catalog.loadTable(tableIdentifier)
                 else catalog.createTable(tableIdentifier, schema, partitionSpec)
               }
      pending <- Ref.make(List.empty[DataFile])
    yield new IcebergDestination[S](table, pending, stateStore)

  /**
   * Build a destination over an already-loaded `Table`. Useful for tests that construct
   * the table via `HadoopTables` directly, or for code paths that want to manage the
   * `Catalog` lifecycle separately from the destination.
   */
  def fromTable[S: JsonCodec](table: Table, stateStore: StateStore): UIO[IcebergDestination[S]] =
    Ref.make(List.empty[DataFile]).map(new IcebergDestination[S](table, _, stateStore))

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
      connectorVersion: String,
      streamName: String
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
        gr.setField("exlo_stream",            streamName)
        writer.write(gr)
      }
    finally writer.close()

    writer.toDataFile()
