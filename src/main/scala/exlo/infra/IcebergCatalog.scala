package exlo.infra

import exlo.domain.DataFile
import exlo.domain.ExloError
import exlo.domain.ExloRecord
import org.apache.iceberg.AppendFiles
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
   * Only used internally by IcebergCatalog.Live.
   */
  private[infra] def apply(underlying: AppendFiles): FileAppender =
    underlying

  /**
   * Unwrap to get the underlying Iceberg AppendFiles builder.
   * Only used internally by IcebergCatalog.Live.
   */
  private[infra] def unwrap(appender: FileAppender): AppendFiles = appender

/**
 * Infrastructure service for interacting with Iceberg catalog.
 *
 * Provides low-level Iceberg operations. This is infrastructure (not business logic),
 * so it lives in the `infra` package.
 *
 * Application services (e.g., Table in exlo.service) depend on this infrastructure
 * service to implement their business logic.
 *
 * Abstracts over different catalog implementations (Nessie, Hive, REST, etc.)
 * using the standard trait + Live + Stub pattern.
 *
 * Key Responsibilities:
 * - Catalog operations (table existence, creation, metadata access)
 * - Physical file writing (Parquet encoding, S3 writes via Iceberg SDK)
 * - Snapshot summary reading (for state management)
 * - Transaction management (creating, staging files, committing with state)
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
   * Read snapshot summary metadata from the table's current snapshot.
   *
   * Returns key-value properties stored in the snapshot summary, including
   * EXLO's state metadata ("exlo.state", "exlo.state.version").
   *
   * @param namespace
   *   Iceberg namespace
   * @param tableName
   *   Iceberg table name
   * @return
   *   Map of snapshot summary properties, empty if no snapshot exists
   */
  def readSnapshotSummary(
    namespace: String,
    tableName: String
  ): IO[ExloError, Map[String, String]]

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
   * @return
   *   Effect that succeeds if commit succeeds
   */
  def commitTransaction(
    namespace: String,
    tableName: String,
    state: String,
    stateVersion: Long
  ): IO[ExloError, Unit]

object IcebergCatalog:

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
  ): ZIO[IcebergCatalog, ExloError, Map[String, String]] =
    ZIO.serviceWithZIO[IcebergCatalog](_.readSnapshotSummary(namespace, tableName))

  /**
   * Accessor for writeAndStageRecords.
   *
   * Use: `IcebergCatalog.writeAndStageRecords(namespace, tableName, records)`
   */
  def writeAndStageRecords(
    namespace: String,
    tableName: String,
    records: Chunk[ExloRecord]
  ): ZIO[IcebergCatalog, ExloError, DataFile] =
    ZIO.serviceWithZIO[IcebergCatalog](_.writeAndStageRecords(namespace, tableName, records))

  /**
   * Accessor for commitTransaction.
   *
   * Use: `IcebergCatalog.commitTransaction(namespace, tableName, state, stateVersion)`
   */
  def commitTransaction(
    namespace: String,
    tableName: String,
    state: String,
    stateVersion: Long
  ): ZIO[IcebergCatalog, ExloError, Unit] =
    ZIO.serviceWithZIO[IcebergCatalog](_.commitTransaction(namespace, tableName, state, stateVersion))

  /**
   * Live implementation - connects to real Iceberg catalog.
   *
   * One IcebergCatalog.Live instance per Table (one connection = one table).
   * This is the correct pattern as per EXLO architecture.
   *
   * Infrastructure dependencies (Phase 2):
   * - StorageConfig for catalog URI and Nessie configuration
   * - Iceberg Java SDK for catalog operations
   * - Nessie client for version control
   *
   * File Appender Management:
   * - Holds Ref[Option[FileAppender]] for this table's active append builder
   * - Builder created lazily on first writeAndStageRecords call (table.newAppend())
   * - Builder accumulates DataFile entries via appendFile() until commit()
   * - Builder cleared after commitTransaction
   *
   * Implementation pattern:
   * {{{
   * val catalog = new NessieCatalog()
   * catalog.initialize("nessie", Map(
   *   "uri" -> config.catalogUri,
   *   "warehouse" -> config.warehousePath,
   *   "ref" -> config.nessieDefaultBranch
   * ))
   * val table = catalog.loadTable(TableIdentifier.of(namespace, tableName))
   * val appender = table.newAppend()  // Creates the builder
   * appender.appendFile(dataFile)      // Stages a file
   * appender.commit()                  // Commits atomically
   * }}}
   *
   * TODO: Implement in Phase 2
   */
  case class Live(
    namespace: String,
    tableName: String,
    appenderRef: Ref[Option[FileAppender]]
  ) extends IcebergCatalog:

    def getTableLocation(
      namespace: String,
      tableName: String
    ): IO[ExloError, String] =
      ZIO.fail(
        ExloError.StateReadError(
          new NotImplementedError("Live implementation pending Phase 2")
        )
      )

    def tableExists(
      namespace: String,
      tableName: String
    ): IO[ExloError, Boolean] =
      ZIO.fail(
        ExloError.StateReadError(
          new NotImplementedError("Live implementation pending Phase 2")
        )
      )

    def createTable(
      namespace: String,
      tableName: String,
      customLocation: Option[String]
    ): IO[ExloError, Unit] =
      // Phase 2 implementation will:
      // 1. Check if table exists (skip if already exists)
      // 2. Build ExloRecord schema using Iceberg Schema API with unique field IDs
      // 3. Create table with schema at customLocation or warehouse path
      // 4. Set up partitioning if needed (e.g., by connectorId or date)
      ZIO.fail(
        ExloError.StateReadError(
          new NotImplementedError("Live implementation pending Phase 2")
        )
      )

    def readSnapshotSummary(
      namespace: String,
      tableName: String
    ): IO[ExloError, Map[String, String]] =
      // Phase 2: Load table, get current snapshot, extract summary properties
      // val table = catalog.loadTable(...)
      // val snapshot = table.currentSnapshot()
      // snapshot.summary().asScala.toMap
      ZIO.fail(
        ExloError.StateReadError(
          new NotImplementedError("Live implementation pending Phase 2")
        )
      )

    def writeAndStageRecords(
      namespace: String,
      tableName: String,
      records: Chunk[ExloRecord]
    ): IO[ExloError, DataFile] =
      // Phase 2 implementation:
      // 1. Write physical Parquet file (using Iceberg FileAppender/ParquetWriter)
      // 2. Get or create AppendFiles transaction for this table
      // 3. Stage the DataFile in the transaction
      // 4. Return DataFile metadata
      ZIO.fail(
        ExloError.IcebergWriteError(
          new NotImplementedError("Live implementation pending Phase 2")
        )
      )

    def commitTransaction(
      namespace: String,
      tableName: String,
      state: String,
      stateVersion: Long
    ): IO[ExloError, Unit] =
      // Phase 2 implementation:
      // 1. Get transaction for this table (if none exists, nothing to commit)
      // 2. Add state metadata to snapshot summary
      // 3. Commit transaction atomically (Iceberg handles OCC retries)
      // 4. Clear transaction from map
      ZIO.fail(
        ExloError.IcebergWriteError(
          new NotImplementedError("Live implementation pending Phase 2")
        )
      )

  object Live:

    /**
     * Create a Live IcebergCatalog instance for a specific table.
     *
     * One catalog instance per table (one connection = one table).
     * Each instance manages its own FileAppender for staged writes.
     *
     * @param namespace Iceberg namespace for this catalog
     * @param tableName Iceberg table name for this catalog
     * @return ZLayer providing IcebergCatalog for the specified table
     */
    def layer(namespace: String, tableName: String): ZLayer[Any, Nothing, IcebergCatalog] =
      ZLayer.scoped {
        for {
          appenderRef <- Ref.make[Option[FileAppender]](None)
        } yield Live(namespace, tableName, appenderRef)
      }

  /**
   * Default live layer for testing - requires namespace and tableName elsewhere.
   * Production code should use Live.layer(namespace, tableName) instead.
   */
  val live: ZLayer[Any, Nothing, IcebergCatalog] =
    Live.layer("default", "default_table")

  /**
   * Stub implementation for testing.
   *
   * Returns predictable locations based on warehouse convention.
   * Simulates snapshot summaries, file writes, and transaction management in memory.
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
    ): IO[ExloError, Map[String, String]] =
      val key = s"$namespace.$tableName"
      ZIO.succeed(snapshotSummaries.getOrElse(key, Map.empty))

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
      stateVersion: Long
    ): IO[ExloError, Unit] =
      ZIO.succeed {
        val key = s"$namespace.$tableName"
        // Update snapshot summary with state metadata
        snapshotSummaries = snapshotSummaries.updated(
          key,
          Map(
            "exlo.state"         -> state,
            "exlo.state.version" -> stateVersion.toString
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
