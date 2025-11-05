package exlo.infra

import exlo.domain.DataFile
import exlo.domain.ExloError
import exlo.domain.ExloRecord
import org.apache.iceberg.Files
import org.apache.iceberg.PartitionSpec
import org.apache.iceberg.Table
import org.apache.iceberg.data.GenericRecord
import org.apache.iceberg.data.parquet.GenericParquetWriter
import org.apache.iceberg.io.DataWriter
import org.apache.iceberg.io.OutputFile
import org.apache.iceberg.parquet.Parquet
import org.apache.parquet.schema.MessageType
import zio.*

import java.io.File
import java.util.UUID as JUUID
import scala.jdk.CollectionConverters.*

/**
 * Generic Iceberg writer operations.
 *
 * This service encapsulates the Iceberg SDK operations that are identical across all catalog types:
 * - Parquet file writing with GenericRecord serialization - Transaction staging with AppendFiles
 * builders - Snapshot commits with state metadata
 *
 * These operations depend on org.apache.iceberg.Table but don't care which catalog loaded it. This
 * allows all catalog implementations (Nessie, Glue, Hive, JDBC, Databricks) to share the same
 * writer logic, eliminating code duplication.
 *
 * This follows the Zionomicon Ch 17 service composition principle: extract invariant operations
 * into a shared service.
 *
 * Design Decision: Takes Table as parameter rather than namespace/tableName. This makes the
 * interface catalog-agnostic and pushes table lookup to the caller (which has CatalogOps).
 */
trait IcebergWriter:

  /**
   * Write a batch of records to a Parquet file and stage it for commit.
   *
   * This operation: 1. Creates a temporary Parquet file in the table's data directory 2. Serializes
   * ExloRecords to GenericRecords using Iceberg's schema 3. Writes records using Iceberg's Parquet
   * writer 4. Stages the DataFile in an AppendFiles transaction builder
   *
   * The staged file is NOT committed until commitTransaction is called. This allows batching
   * multiple writes into a single snapshot.
   *
   * This is the core ~80% of code that was duplicated across catalog implementations.
   *
   * @param table
   *   Iceberg Table instance (from CatalogOps.loadTable)
   * @param records
   *   Batch of EXLO records to write
   * @return
   *   Metadata about the written Parquet file
   */
  def writeAndStageRecords(
    table: Table,
    records: Chunk[ExloRecord]
  ): IO[ExloError, DataFile]

  /**
   * Commit all staged files as a new snapshot with state metadata.
   *
   * This operation: 1. Retrieves the staged AppendFiles builder 2. Sets exlo.state and
   * exlo.state.version in snapshot summary 3. Commits the transaction, creating a new snapshot 4.
   * Clears the staged builder
   *
   * If no files were staged, creates an empty snapshot with just the state metadata. This handles
   * the case where we want to checkpoint state even when no data was extracted.
   *
   * @param table
   *   Iceberg Table instance (from CatalogOps.loadTable)
   * @param state
   *   Current connector state to persist in snapshot metadata
   * @param stateVersion
   *   Monotonic version number for state evolution
   * @return
   *   Effect that succeeds when snapshot is committed
   */
  def commitTransaction(
    table: Table,
    state: String,
    stateVersion: Long
  ): IO[ExloError, Unit]

  /**
   * Read the most recent snapshot's state metadata.
   *
   * Returns the connector state persisted in the latest snapshot's summary. Used for incremental
   * sync to resume from last checkpoint.
   *
   * @param table
   *   Iceberg Table instance (from CatalogOps.loadTable)
   * @return
   *   Tuple of (state: String, stateVersion: Long) or None if no snapshots exist
   */
  def readSnapshotSummary(
    table: Table
  ): IO[ExloError, Option[(String, Long)]]

object IcebergWriter:

  /**
   * Accessor for writeAndStageRecords.
   *
   * Use: `IcebergWriter.writeAndStageRecords(table, records)`
   */
  def writeAndStageRecords(
    table: Table,
    records: Chunk[ExloRecord]
  ): ZIO[IcebergWriter, ExloError, DataFile] =
    ZIO.serviceWithZIO[IcebergWriter](_.writeAndStageRecords(table, records))

  /**
   * Accessor for commitTransaction.
   *
   * Use: `IcebergWriter.commitTransaction(table, state, stateVersion)`
   */
  def commitTransaction(
    table: Table,
    state: String,
    stateVersion: Long
  ): ZIO[IcebergWriter, ExloError, Unit] =
    ZIO.serviceWithZIO[IcebergWriter](_.commitTransaction(table, state, stateVersion))

  /**
   * Accessor for readSnapshotSummary.
   *
   * Use: `IcebergWriter.readSnapshotSummary(table)`
   */
  def readSnapshotSummary(
    table: Table
  ): ZIO[IcebergWriter, ExloError, Option[(String, Long)]] =
    ZIO.serviceWithZIO[IcebergWriter](_.readSnapshotSummary(table))

  /**
   * Live implementation - shared Iceberg writer.
   *
   * Stateful: Holds a Ref to track staged AppendFiles transactions. This allows batching multiple
   * writes before committing a single snapshot.
   */
  case class Live(
    appenderRef: Ref[Option[FileAppender]]
  ) extends IcebergWriter:

    def writeAndStageRecords(
      table: Table,
      records: Chunk[ExloRecord]
    ): IO[ExloError, DataFile] =
      (for {
        schema <- ZIO.succeed(IcebergCatalog.buildExloSchema())

        // PHASE 1: Write physical Parquet file
        icebergDataFile <- ZIO.attempt {
          // Create a temporary file in the table's data directory
          val tableLocation = table.location()
          val fileName      = s"${JUUID.randomUUID()}.parquet"
          val filePath      = new File(s"$tableLocation/data/$fileName")
          val outputFile: OutputFile = Files.localOutput(filePath)

          // Create Parquet writer using Iceberg's GenericParquetWriter
          // In Iceberg 1.10.0+, buildWriter was renamed to create() and takes (Schema, MessageType)
          val dataWriter: DataWriter[GenericRecord] = Parquet
            .writeData(outputFile)
            .schema(schema)
            .createWriterFunc((msgType: MessageType) => GenericParquetWriter.create(schema, msgType))
            .overwrite()
            .withSpec(PartitionSpec.unpartitioned())
            .build()

          try {
            // Convert ExloRecords to GenericRecords and write
            val genericRecord = GenericRecord.create(schema)
            records.foreach { exloRecord =>
              // In Iceberg 1.10.0+, UUIDType expects java.util.UUID objects directly
              // No need to convert to byte arrays anymore

              // Convert Instant to OffsetDateTime for Iceberg's timestamp with timezone
              val committedAtOdt =
                java.time.OffsetDateTime.ofInstant(exloRecord.committedAt, java.time.ZoneOffset.UTC)
              val recordedAtOdt  =
                java.time.OffsetDateTime.ofInstant(exloRecord.recordedAt, java.time.ZoneOffset.UTC)

              val rec = genericRecord.copy(
                Map(
                  "commit_id"             -> exloRecord.commitId,    // Direct UUID in 1.10.0+
                  "connector_id"          -> exloRecord.connectorId, // String, not UUID
                  "sync_id"               -> exloRecord.syncId,      // Direct UUID in 1.10.0+
                  "committed_at"          -> committedAtOdt,
                  "recorded_at"           -> recordedAtOdt,
                  "connector_version"     -> exloRecord.connectorVersion,
                  "connector_config_hash" -> exloRecord.connectorConfigHash,
                  "stream_config_hash"    -> exloRecord.streamConfigHash,
                  "state_version"         -> exloRecord.stateVersion,
                  "payload"               -> exloRecord.payload
                ).asJava
              )
              dataWriter.write(rec)
            }
          } finally dataWriter.close()

          dataWriter.toDataFile()
        }

        // PHASE 2: Stage DataFile in AppendFiles builder
        _               <- appenderRef.updateAndGet {
          case Some(existing) =>
            Some(FileAppender(FileAppender.unwrap(existing).appendFile(icebergDataFile)))
          case None           =>
            val appender = table.newAppend()
            Some(FileAppender(appender.appendFile(icebergDataFile)))
        }
      } yield DataFile(
        path = icebergDataFile.location(),
        recordCount = icebergDataFile.recordCount(),
        sizeBytes = icebergDataFile.fileSizeInBytes()
      )).mapError(ExloError.IcebergWriteError.apply)

    def commitTransaction(
      table: Table,
      state: String,
      stateVersion: Long
    ): IO[ExloError, Unit] =
      (for {
        maybeAppender <- appenderRef.get
        _             <- maybeAppender match {
          case Some(appender) =>
            // Commit staged files with state metadata
            ZIO.attempt {
              val appenderWithState = FileAppender
                .unwrap(appender)
                .set("exlo.state", state)
                .set("exlo.state.version", stateVersion.toString)
              appenderWithState.commit()
            } *> appenderRef.set(None)

          case None =>
            // No staged files, but still need to persist state!
            // Create an empty append operation (no files added)
            ZIO.attempt {
              table
                .newAppend()
                .set("exlo.state", state)
                .set("exlo.state.version", stateVersion.toString)
                .commit()
            }
        }
      } yield ()).mapError(ExloError.IcebergWriteError.apply)

    def readSnapshotSummary(
      table: Table
    ): IO[ExloError, Option[(String, Long)]] =
      ZIO
        .attempt {
          // Get current snapshot (null if table is empty)
          Option(table.currentSnapshot()).flatMap { snapshot =>
            val summary = snapshot.summary().asScala.toMap
            for {
              state        <- summary.get("exlo.state")
              versionStr   <- summary.get("exlo.state.version")
              stateVersion <- versionStr.toLongOption
            } yield (state, stateVersion)
          }
        }
        .mapError(ExloError.StateReadError.apply)

  object Live:

    /**
     * Create an IcebergWriter layer.
     *
     * Creates a scoped layer with a Ref for tracking staged AppendFiles transactions. The Ref is
     * created per-scope to ensure transaction isolation.
     *
     * @return
     *   ZLayer that provides IcebergWriter
     */
    val layer: ZLayer[Any, Nothing, IcebergWriter] =
      ZLayer.scoped {
        Ref.make[Option[FileAppender]](None).map(Live(_))
      }
