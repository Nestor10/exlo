package exlo.infra

import exlo.domain.*
import org.apache.iceberg.AppendFiles
import org.apache.iceberg.Schema
import org.apache.iceberg.types.Types
import zio.*

/**
 * Opaque type for Iceberg file append builder.
 *
 * This wraps AppendFiles, which is the builder/staging object returned by
 * table.newAppend(). It accumulates DataFile entries via appendFile() until
 * commit() is called.
 *
 * Hides Iceberg SDK types from the application layer while maintaining full
 * type safety within the infrastructure layer.
 *
 * Application services see this as an opaque type they cannot manipulate.
 * Infrastructure code works with the real Iceberg AppendFiles internally.
 */
opaque type FileAppender = AppendFiles

object FileAppender:

  /**
   * Wrap an Iceberg AppendFiles in the opaque type.
   * Only used internally by catalog implementations.
   */
  private[infra] def apply(underlying: AppendFiles): FileAppender =
    underlying

  /**
   * Unwrap to get the underlying Iceberg AppendFiles builder.
   * Only used internally by catalog implementations.
   */
  private[infra] def unwrap(appender: FileAppender): AppendFiles = appender

/**
 * Infrastructure service for interacting with Iceberg catalog.
 *
 * Composed service combining: - CatalogOps: Catalog-specific metadata operations (loadTable,
 * tableExists, createTable) - IcebergWriter: Generic Iceberg SDK operations (Parquet writing,
 * transaction staging, commit)
 *
 * This composition follows Zionomicon Ch 17 service composition principles: - CatalogOps varies by
 * catalog type (Nessie, Glue, Hive, JDBC, Databricks) - IcebergWriter is shared across all
 * catalogs (eliminates ~80% code duplication)
 *
 * Architecture: User calls IcebergCatalog → Delegates to CatalogOps for table lookup → Passes
 * Table to IcebergWriter for data operations
 *
 * Use IcebergCatalogSelector.layer() for dynamic catalog selection based on StorageConfig.
 */
trait IcebergCatalog:

  /**
   * Get the storage location for a table.
   *
   * Returns the base path where data files should be written. This respects
   * any custom location specified during table creation.
   *
   * Example: "s3://bucket/warehouse/namespace/table/"
   *
   * @param namespace
   *   Iceberg namespace
   * @param tableName
   *   Iceberg table name
   * @return
   *   Absolute path to table's data location
   */
  def getTableLocation(
    namespace: String,
    tableName: String
  ): IO[ExloError, String]

  /**
   * Check if a table exists in the catalog.
   *
   * @param namespace
   *   Iceberg namespace
   * @param tableName
   *   Iceberg table name
   * @return
   *   True if table exists, false otherwise
   */
  def tableExists(
    namespace: String,
    tableName: String
  ): IO[ExloError, Boolean]

  /**
   * Create a new Iceberg table with EXLO schema.
   *
   * The table schema is fixed to ExloRecord structure:
   * - commitId: UUID
   * - connectorId: String
   * - syncId: UUID
   * - committedAt: Timestamp
   * - recordedAt: Timestamp
   * - connectorVersion: String
   * - connectorConfigHash: String
   * - streamConfigHash: String
   * - stateVersion: Long
   * - payload: String (user data)
   *
   * Tables are created at warehouse-based location unless custom location provided.
   *
   * @param namespace
   *   Iceberg namespace
   * @param tableName
   *   Iceberg table name
   * @param customLocation
   *   Optional custom location for table (None = use warehouse convention)
   * @return
   *   Effect that succeeds if table created or already exists
   */
  def createTable(
    namespace: String,
    tableName: String,
    customLocation: Option[String] = None
  ): IO[ExloError, Unit]

  /**
   * Read EXLO state metadata from the table's current snapshot.
   *
   * Returns the connector state, version, and stream name persisted in the latest snapshot's
   * summary. Used for incremental sync to resume from last checkpoint.
   *
   * @param namespace
   *   Iceberg namespace
   * @param tableName
   *   Iceberg table name
   * @return
   *   Tuple of (state: String, stateVersion: Long, streamName: String) or None if no snapshots exist
   */
  def readSnapshotSummary(
    namespace: String,
    tableName: String
  ): IO[ExloError, Option[(String, Long, String)]]

  /**
   * Write a batch of ExloRecords to a Parquet file and stage it in the active transaction.
   *
   * This combines PHASE 1 (physical write) and PHASE 2 (metadata staging) of the Iceberg pattern:
   * - Physically writes records to Parquet file using Iceberg writer APIs
   * - Stages the DataFile metadata in the active transaction for this table
   * - Creates a new transaction if none exists for this table
   *
   * Does NOT commit - call commitTransaction when checkpoint arrives.
   *
   * @param namespace
   *   Iceberg namespace
   * @param tableName
   *   Iceberg table name
   * @param records
   *   Batch of ExloRecords to write and stage
   * @return
   *   DataFile metadata for the written Parquet file
   */
  def writeAndStageRecords(
    namespace: String,
    tableName: String,
    records: Chunk[ExloRecord]
  ): IO[ExloError, DataFile]

  /**
   * Commit all staged files for a table with state metadata atomically.
   *
   * This is the final phase of the Iceberg commit pattern:
   * - Adds state metadata to snapshot summary
   * - Atomically commits all files staged since last commit
   * - Clears the transaction for this table
   *
   * Uses Optimistic Concurrency Control - Iceberg SDK handles retries automatically.
   *
   * @param namespace
   *   Iceberg namespace
   * @param tableName
   *   Iceberg table name
   * @param state
   *   State as JSON string to store in snapshot summary
   * @param stateVersion
   *   State version for invalidation tracking
   * @param streamName
   *   Stream name to store in snapshot summary for validation
   * @return
   *   Effect that succeeds if commit succeeds
   */
  def commitTransaction(
    namespace: String,
    tableName: String,
    state: String,
    stateVersion: Long,
    streamName: String
  ): IO[ExloError, Unit]

object IcebergCatalog:

  /**
   * Build the fixed Iceberg schema for ExloRecord.
   *
   * Uses unique field IDs for each column (Iceberg requirement).
   * Iceberg tracks columns by ID, not name, enabling safe schema evolution.
   *
   * This schema is shared across all catalog implementations since ExloRecord
   * structure is fixed by the framework.
   */
  private[infra] def buildExloSchema(): Schema =
    new Schema(
      Types.NestedField.required(1, "commit_id", Types.UUIDType.get()),
      Types.NestedField.required(2, "connector_id", Types.StringType.get()),
      Types.NestedField.required(3, "sync_id", Types.UUIDType.get()),
      Types.NestedField.required(4, "committed_at", Types.TimestampType.withZone()),
      Types.NestedField.required(5, "recorded_at", Types.TimestampType.withZone()),
      Types.NestedField.required(6, "connector_version", Types.StringType.get()),
      Types.NestedField.required(7, "connector_config_hash", Types.StringType.get()),
      Types.NestedField.required(8, "stream_config_hash", Types.StringType.get()),
      Types.NestedField.required(9, "stream_name", Types.StringType.get()),
      Types.NestedField.required(10, "state_version", Types.LongType.get()),
      Types.NestedField.required(11, "payload", Types.StringType.get())
    )

  /**
   * Accessor for getTableLocation.
   *
   * Use: `IcebergCatalog.getTableLocation(namespace, tableName)`
   */
  def getTableLocation(
    namespace: String,
    tableName: String
  ): ZIO[IcebergCatalog, ExloError, String] =
    ZIO.serviceWithZIO[IcebergCatalog](_.getTableLocation(namespace, tableName))

  /**
   * Accessor for tableExists.
   *
   * Use: `IcebergCatalog.tableExists(namespace, tableName)`
   */
  def tableExists(
    namespace: String,
    tableName: String
  ): ZIO[IcebergCatalog, ExloError, Boolean] =
    ZIO.serviceWithZIO[IcebergCatalog](_.tableExists(namespace, tableName))

  /**
   * Accessor for createTable.
   *
   * Use: `IcebergCatalog.createTable(namespace, tableName, customLocation)`
   */
  def createTable(
    namespace: String,
    tableName: String,
    customLocation: Option[String] = None
  ): ZIO[IcebergCatalog, ExloError, Unit] =
    ZIO.serviceWithZIO[IcebergCatalog](_.createTable(namespace, tableName, customLocation))

  /**
   * Accessor for readSnapshotSummary.
   *
   * Use: `IcebergCatalog.readSnapshotSummary(namespace, tableName)`
   */
  def readSnapshotSummary(
    namespace: String,
    tableName: String
  ): ZIO[IcebergCatalog, ExloError, Option[(String, Long, String)]] =
    ZIO.serviceWithZIO[IcebergCatalog](_.readSnapshotSummary(namespace, tableName))

  /**
   * Live implementation - composed from CatalogOps and IcebergWriter.
   *
   * This is the key composition: catalog-specific operations delegate to CatalogOps, generic
   * Iceberg operations delegate to IcebergWriter. All catalog types (Nessie, Glue, Hive, JDBC,
   * Databricks) share the same composition logic, varying only in their CatalogOps implementation.
   */
  case class Live(
    catalogOps: CatalogOps,
    writer: IcebergWriter
  ) extends IcebergCatalog:

    // Catalog-specific operations - delegate to CatalogOps
    def getTableLocation(
      namespace: String,
      tableName: String
    ): IO[ExloError, String] =
      catalogOps.getTableLocation(namespace, tableName)

    def tableExists(
      namespace: String,
      tableName: String
    ): IO[ExloError, Boolean] =
      catalogOps.tableExists(namespace, tableName)

    def createTable(
      namespace: String,
      tableName: String,
      customLocation: Option[String]
    ): IO[ExloError, Unit] =
      catalogOps.createTable(namespace, tableName, customLocation)

    // Generic Iceberg operations - load table via CatalogOps, then use IcebergWriter
    def readSnapshotSummary(
      namespace: String,
      tableName: String
    ): IO[ExloError, Option[(String, Long, String)]] =
      for {
        table   <- catalogOps.loadTable(namespace, tableName)
        summary <- writer.readSnapshotSummary(table)
      } yield summary

    def writeAndStageRecords(
      namespace: String,
      tableName: String,
      records: Chunk[ExloRecord]
    ): IO[ExloError, DataFile] =
      for {
        table    <- catalogOps.loadTable(namespace, tableName)
        dataFile <- writer.writeAndStageRecords(table, records)
      } yield dataFile

    def commitTransaction(
      namespace: String,
      tableName: String,
      state: String,
      stateVersion: Long,
      streamName: String
    ): IO[ExloError, Unit] =
      for {
        table <- catalogOps.loadTable(namespace, tableName)
        _     <- writer.commitTransaction(table, state, stateVersion, streamName)
      } yield ()

  object Live:

    /**
     * Create IcebergCatalog layer from CatalogOps and IcebergWriter layers.
     *
     * This is the composition point - any CatalogOps implementation (Nessie, Glue, etc.) + shared
     * IcebergWriter = complete IcebergCatalog.
     *
     * Use: `NessieCatalogOps.layer(...) ++ IcebergWriterLive.layer >>> IcebergCatalog.Live.layer`
     */
    val layer: ZLayer[CatalogOps & IcebergWriter, Nothing, IcebergCatalog] =
      ZLayer.fromFunction(Live(_, _))

  /**
   * In-memory stub implementation for testing.
   *
   * Simulates Iceberg behavior without requiring actual catalog infrastructure: - Tracks snapshot
   * summaries in memory - Accumulates staged files in memory - Simulates transaction commit by
   * updating state and clearing staged files
   */
  case class Stub(
    warehousePath: String = "s3://test-bucket/warehouse",
    var snapshotSummaries: Map[String, Map[String, String]] = Map.empty,
    var writtenFiles: Map[String, Chunk[DataFile]] = Map.empty // Keyed by "namespace.tableName"
  ) extends IcebergCatalog:

    def getTableLocation(
      namespace: String,
      tableName: String
    ): IO[ExloError, String] =
      ZIO.succeed(s"$warehousePath/$namespace/$tableName/")

    def tableExists(
      namespace: String,
      tableName: String
    ): IO[ExloError, Boolean] =
      ZIO.succeed(true)

    def createTable(
      namespace: String,
      tableName: String,
      customLocation: Option[String]
    ): IO[ExloError, Unit] =
      ZIO.unit // No-op in tests - assume table already exists

    def readSnapshotSummary(
      namespace: String,
      tableName: String
    ): IO[ExloError, Option[(String, Long, String)]] =
      val key = s"$namespace.$tableName"
      ZIO.succeed {
        snapshotSummaries.get(key).flatMap { summary =>
          for {
            state        <- summary.get("exlo.state")
            versionStr   <- summary.get("exlo.state.version")
            streamName   <- summary.get("exlo.state.stream_name")
            stateVersion <- versionStr.toLongOption
          } yield (state, stateVersion, streamName)
        }
      }

    def writeAndStageRecords(
      namespace: String,
      tableName: String,
      records: Chunk[ExloRecord]
    ): IO[ExloError, DataFile] =
      ZIO.succeed {
        val key      = s"$namespace.$tableName"
        val fileId   = java.util.UUID.randomUUID().toString
        val dataFile = DataFile(
          path = s"$warehousePath/$namespace/$tableName/data/$fileId.parquet",
          recordCount = records.size.toLong,
          sizeBytes = records.map(_.payload.length).sum.toLong * 2 // Rough estimate
        )
        // Accumulate files for this table (simulating transaction staging)
        writtenFiles = writtenFiles.updated(
          key,
          writtenFiles.getOrElse(key, Chunk.empty) :+ dataFile
        )
        dataFile
      }

    def commitTransaction(
      namespace: String,
      tableName: String,
      state: String,
      stateVersion: Long,
      streamName: String
    ): IO[ExloError, Unit] =
      ZIO.succeed {
        val key = s"$namespace.$tableName"
        // Update snapshot summary with state metadata
        snapshotSummaries = snapshotSummaries.updated(
          key,
          Map(
            "exlo.state"             -> state,
            "exlo.state.version"     -> stateVersion.toString,
            "exlo.state.stream_name" -> streamName
          )
        )
        // Clear staged files for this table (simulating transaction commit)
        writtenFiles = writtenFiles.updated(key, Chunk.empty)
      }

  object Stub:

    val layer: ZLayer[Any, Nothing, IcebergCatalog] =
      ZLayer.succeed(Stub())

    def withWarehouse(path: String): ZLayer[Any, Nothing, IcebergCatalog] =
      ZLayer.succeed(Stub(path))
