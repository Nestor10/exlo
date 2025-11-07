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
   * This operation: 1. Retrieves the staged AppendFiles builder 2. Sets exlo.state,
   * exlo.state.version, and exlo.state.stream_name in snapshot summary 3. Commits the transaction,
   * creating a new snapshot 4. Clears the staged builder
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
   * @param streamName
   *   Stream name to store in snapshot metadata for validation
   * @return
   *   Effect that succeeds when snapshot is committed
   */
  def commitTransaction(
    table: Table,
    state: String,
    stateVersion: Long,
    streamName: String
  ): IO[ExloError, Unit]

  /**
   * Read the most recent snapshot's state metadata.
   *
   * Returns the connector state, version, and stream name persisted in the latest snapshot's
   * summary. Used for incremental sync to resume from last checkpoint.
   *
   * @param table
   *   Iceberg Table instance (from CatalogOps.loadTable)
   * @return
   *   Tuple of (state: String, stateVersion: Long, streamName: String) or None if no snapshots exist
   */
  def readSnapshotSummary(
    table: Table
  ): IO[ExloError, Option[(String, Long, String)]]

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
   * Use: `IcebergWriter.commitTransaction(table, state, stateVersion, streamName)`
   */
  def commitTransaction(
    table: Table,
    state: String,
    stateVersion: Long,
    streamName: String
  ): ZIO[IcebergWriter, ExloError, Unit] =
    ZIO.serviceWithZIO[IcebergWriter](_.commitTransaction(table, state, stateVersion, streamName))

  /**
   * Accessor for readSnapshotSummary.
   *
   * Use: `IcebergWriter.readSnapshotSummary(table)`
   */
  def readSnapshotSummary(
    table: Table
  ): ZIO[IcebergWriter, ExloError, Option[(String, Long, String)]] =
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
                  "stream_name"           -> exloRecord.streamName,
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
      stateVersion: Long,
      streamName: String
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
                .set("exlo.state.stream_name", streamName)
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
                .set("exlo.state.stream_name", streamName)
                .commit()
            }
        }
      } yield ()).mapError(ExloError.IcebergWriteError.apply)

    def readSnapshotSummary(
      table: Table
    ): IO[ExloError, Option[(String, Long, String)]] =
      ZIO
        .attempt {
          // Get current snapshot (null if table is empty)
          Option(table.currentSnapshot()).flatMap { snapshot =>
            val summary = snapshot.summary().asScala.toMap
            for {
              state        <- summary.get("exlo.state")
              versionStr   <- summary.get("exlo.state.version")
              streamName   <- summary.get("exlo.state.stream_name")
              stateVersion <- versionStr.toLongOption
            } yield (state, stateVersion, streamName)
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

  /**
   * Captured write operation for testing.
   *
   * Represents a single call to writeAndStageRecords.
   *
   * @param records
   *   The records that were written
   * @param recordCount
   *   Number of records in this write
   * @param payloads
   *   Just the payload strings (user data)
   */
  case class WriteOperation(
    records: Chunk[ExloRecord],
    recordCount: Long,
    payloads: Chunk[String]
  )

  /**
   * Captured commit operation for testing.
   *
   * Represents a single call to commitTransaction.
   *
   * @param state
   *   The state that was committed
   * @param stateVersion
   *   The state version that was committed
   * @param streamName
   *   The stream name that was committed
   * @param writeCount
   *   Number of writes staged before this commit
   */
  case class CommitOperation(
    state: String,
    stateVersion: Long,
    streamName: String,
    writeCount: Int
  )

  /**
   * In-memory test implementation of IcebergWriter.
   *
   * Captures all write and commit operations in Refs for test assertions. Does
   * not perform any actual I/O or interact with Iceberg.
   *
   * Perfect for testing connectors (especially YAML connectors) without
   * needing a real Iceberg catalog.
   *
   * Example usage:
   * {{{
   * test("yaml connector extracts records") {
   *   for {
   *     writer <- ZIO.service[IcebergWriter.InMemory]
   *
   *     // Run your connector extraction...
   *
   *     // Assert on captured operations
   *     writes <- writer.getWrites
   *     commits <- writer.getCommits
   *
   *     _ <- assertTrue(writes.length == 1)
   *     _ <- assertTrue(writes.head.recordCount == 10)
   *     _ <- assertTrue(commits.length == 1)
   *     _ <- assertTrue(commits.head.state.contains("cursor"))
   *   } yield ()
   * }.provide(IcebergWriter.InMemory.layer)
   * }}}
   *
   * @param writesRef
   *   Captured write operations
   * @param commitsRef
   *   Captured commit operations
   * @param stagedWritesRef
   *   Currently staged (uncommitted) writes
   * @param currentStateRef
   *   Current snapshot state (from last commit)
   */
  case class InMemory(
    writesRef: Ref[Chunk[WriteOperation]],
    commitsRef: Ref[Chunk[CommitOperation]],
    stagedWritesRef: Ref[Chunk[WriteOperation]],
    currentStateRef: Ref[Option[(String, Long, String)]]
  ) extends IcebergWriter:

    def writeAndStageRecords(
      table: Table,
      records: Chunk[ExloRecord]
    ): IO[ExloError, DataFile] =
      for {
        writeOp <- ZIO.succeed(
          WriteOperation(
            records = records,
            recordCount = records.size.toLong,
            payloads = records.map(_.payload)
          )
        )

        // Capture the write
        _       <- writesRef.update(_ :+ writeOp)
        _       <- stagedWritesRef.update(_ :+ writeOp)
      } yield DataFile(
        path = s"test://data/${java.util.UUID.randomUUID()}.parquet",
        recordCount = records.size.toLong,
        sizeBytes = records.map(_.payload.length).sum.toLong * 2 // Rough estimate
      )

    def commitTransaction(
      table: Table,
      state: String,
      stateVersion: Long,
      streamName: String
    ): IO[ExloError, Unit] =
      for {
        staged <- stagedWritesRef.get

        commitOp = CommitOperation(
          state = state,
          stateVersion = stateVersion,
          streamName = streamName,
          writeCount = staged.length
        )

        // Capture the commit
        _ <- commitsRef.update(_ :+ commitOp)

        // Update current state
        _ <- currentStateRef.set(Some((state, stateVersion, streamName)))

        // Clear staged writes
        _ <- stagedWritesRef.set(Chunk.empty)
      } yield ()

    def readSnapshotSummary(
      table: Table
    ): IO[ExloError, Option[(String, Long, String)]] =
      currentStateRef.get

    /** Get all captured write operations. */
    def getWrites: UIO[Chunk[WriteOperation]] =
      writesRef.get

    /** Get all captured commit operations. */
    def getCommits: UIO[Chunk[CommitOperation]] =
      commitsRef.get

    /** Get currently staged (uncommitted) writes. */
    def getStagedWrites: UIO[Chunk[WriteOperation]] =
      stagedWritesRef.get

    /** Get total number of records written across all operations. */
    def getTotalRecordCount: UIO[Long] =
      writesRef.get.map(_.map(_.recordCount).sum)

    /** Get all payloads (user data strings) from all writes. */
    def getAllPayloads: UIO[Chunk[String]] =
      writesRef.get.map(_.flatMap(_.payloads))

    /** Reset all captured state (useful between tests). */
    def reset: UIO[Unit] =
      writesRef.set(Chunk.empty) *>
        commitsRef.set(Chunk.empty) *>
        stagedWritesRef.set(Chunk.empty) *>
        currentStateRef.set(None)

  object InMemory:

    /**
     * Create an in-memory test IcebergWriter layer.
     *
     * Use this in tests instead of the Live layer to capture operations
     * without real Iceberg I/O.
     */
    val layer: ZLayer[Any, Nothing, IcebergWriter] =
      ZLayer {
        for {
          writesRef       <- Ref.make(Chunk.empty[WriteOperation])
          commitsRef      <- Ref.make(Chunk.empty[CommitOperation])
          stagedWritesRef <- Ref.make(Chunk.empty[WriteOperation])
          currentStateRef <- Ref.make[Option[(String, Long, String)]](None)
        } yield InMemory(writesRef, commitsRef, stagedWritesRef, currentStateRef)
      }

    /**
     * Create layer and return handle to the InMemory instance.
     *
     * Useful when you need direct access to test assertions:
     * {{{
     * val (layer, handle) = IcebergWriter.InMemory.layerWithHandle
     * test("...") {
     *   for {
     *     // Run connector...
     *     writes <- handle.getWrites
     *     _ <- assertTrue(writes.nonEmpty)
     *   } yield ()
     * }.provide(layer)
     * }}}
     */
    def layerWithHandle: UIO[(ZLayer[Any, Nothing, IcebergWriter], InMemory)] =
      for {
        writesRef       <- Ref.make(Chunk.empty[WriteOperation])
        commitsRef      <- Ref.make(Chunk.empty[CommitOperation])
        stagedWritesRef <- Ref.make(Chunk.empty[WriteOperation])
        currentStateRef <- Ref.make[Option[(String, Long, String)]](None)
        instance = InMemory(writesRef, commitsRef, stagedWritesRef, currentStateRef)
        layer    = ZLayer.succeed[IcebergWriter](instance)
      } yield (layer, instance)
