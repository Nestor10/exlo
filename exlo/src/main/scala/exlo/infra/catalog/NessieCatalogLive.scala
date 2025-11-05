package exlo.infra.catalog

import exlo.domain.*
import exlo.infra.FileAppender
import exlo.infra.IcebergCatalog
import org.apache.iceberg.Files
import org.apache.iceberg.PartitionSpec
import org.apache.iceberg.Table
import org.apache.iceberg.catalog.Catalog
import org.apache.iceberg.catalog.Namespace
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.data.GenericRecord
import org.apache.iceberg.data.parquet.GenericParquetWriter
import org.apache.iceberg.io.DataWriter
import org.apache.iceberg.io.OutputFile
import org.apache.iceberg.nessie.NessieCatalog
import org.apache.iceberg.parquet.Parquet
import org.apache.parquet.schema.MessageType
import zio.*

import java.io.File
import java.util.UUID as JUUID
import scala.jdk.CollectionConverters.*

/**
 * Nessie catalog implementation for IcebergCatalog.
 *
 * Nessie provides Git-like version control for Iceberg tables with production-safe
 * concurrent writes. It's the recommended catalog for cloud environments.
 *
 * Architecture:
 * - One NessieCatalogLive instance per table
 * - Holds reference to initialized Nessie catalog
 * - Manages FileAppender for transaction staging
 *
 * Nessie is the CATALOG (metadata) - data files can be stored anywhere:
 * - AWS S3 (using iceberg-aws dependency + S3FileIO)
 * - Google Cloud Storage (using iceberg-gcp dependency + GcsFileIO)
 * - Azure Blob Storage (using iceberg-azure dependency + ADLSFileIO)
 * - HDFS (using hadoop-hdfs dependency + HadoopFileIO)
 * - Local filesystem (for testing)
 *
 * Configuration is passed via NessieConfig.properties map.
 * Iceberg auto-selects FileIO based on warehouse path and properties.
 */
object NessieCatalogLive:

  /**
   * Create a NessieCatalogLive instance for a specific table.
   *
   * @param namespace Iceberg namespace
   * @param tableName Iceberg table name
   * @param warehouse Base path for Iceberg warehouse (e.g., "s3://bucket/warehouse")
   * @param storage Storage backend configuration (S3, GCS, Azure, etc.)
   * @param catalog Nessie catalog configuration (URI, branch, auth)
   * @return ZIO effect that produces an IcebergCatalog implementation
   */
  def make(
    namespace: String,
    tableName: String,
    warehouse: String,
    storage: StorageBackend,
    catalog: CatalogConfig.Nessie
  ): Task[IcebergCatalog] =
    for {
      // Initialize Nessie catalog with merged storage + catalog properties
      nessieCatalog <- initNessieCatalog(warehouse, storage, catalog)

      // Create file appender ref for this table
      appenderRef <- Ref.make[Option[FileAppender]](None)

    } yield Live(namespace, tableName, nessieCatalog, appenderRef)

  /**
   * Initialize a Nessie catalog.
   *
   * Combines storage backend properties with Nessie catalog properties.
   * Storage backend determines FileIO implementation (S3FileIO, GcsFileIO, etc.).
   *
   * @param warehouse Base path for Iceberg warehouse
   * @param storage Storage backend configuration (generates FileIO properties)
   * @param catalog Nessie catalog configuration (URI, branch, auth)
   * @return Initialized Nessie catalog
   */
  private def initNessieCatalog(
    warehouse: String,
    storage: StorageBackend,
    catalog: CatalogConfig.Nessie
  ): Task[Catalog] =
    ZIO.attempt {
      val nessieCatalog = new NessieCatalog()

      // Merge storage backend properties with Nessie catalog properties
      val storageProps = storage.toIcebergProperties
      val catalogProps = Map(
        "uri"       -> catalog.uri,
        "warehouse" -> warehouse,
        "ref"       -> catalog.branch
      )

      // Add auth token if provided
      val authProps = catalog.authToken match {
        case Some(token) =>
          Map("authentication.type" -> "BEARER", "authentication.token" -> token)
        case None        => Map.empty[String, String]
      }

      val allProps = storageProps ++ catalogProps ++ authProps

      nessieCatalog.initialize("nessie", allProps.asJava)
      nessieCatalog
    }

  /**
   * Live implementation - Nessie-backed IcebergCatalog.
   */
  private case class Live(
    namespace: String,
    tableName: String,
    catalog: Catalog,
    appenderRef: Ref[Option[FileAppender]]
  ) extends IcebergCatalog:

    def getTableLocation(
      namespace: String,
      tableName: String
    ): IO[ExloError, String] =
      ZIO
        .attempt {
          val tableId = TableIdentifier.of(namespace, tableName)
          val table   = catalog.loadTable(tableId)
          table.location()
        }
        .mapError(ExloError.StateReadError.apply)

    def tableExists(
      namespace: String,
      tableName: String
    ): IO[ExloError, Boolean] =
      ZIO
        .attempt {
          val tableId = TableIdentifier.of(namespace, tableName)
          catalog.tableExists(tableId)
        }
        .mapError(ExloError.StateReadError.apply)

    def createTable(
      namespace: String,
      tableName: String,
      customLocation: Option[String]
    ): IO[ExloError, Unit] =
      ZIO
        .attempt {
          val namespaceId = Namespace.of(namespace)
          val tableId     = TableIdentifier.of(namespace, tableName)

          // Create namespace if it doesn't exist (Nessie catalog supports namespaces)
          val nsCatalog = catalog.asInstanceOf[org.apache.iceberg.catalog.SupportsNamespaces]
          try
            nsCatalog.loadNamespaceMetadata(namespaceId)
          catch {
            case _: org.apache.iceberg.exceptions.NoSuchNamespaceException =>
              nsCatalog.createNamespace(namespaceId)
          }

          // Skip if table already exists
          if (!catalog.tableExists(tableId)) {
            val schema = IcebergCatalog.buildExloSchema()

            // Unpartitioned table - we'll rely on Iceberg's file-level metadata for pruning
            val spec = PartitionSpec.unpartitioned()

            // Create table with optional custom location
            customLocation match {
              case Some(location) =>
                val props = Map("location" -> location).asJava
                catalog.buildTable(tableId, schema).withPartitionSpec(spec).withProperties(props).create()
              case None           =>
                catalog.createTable(tableId, schema, spec)
            }
          }
        }
        .mapError(ExloError.StateReadError.apply)
        .unit

    def readSnapshotSummary(
      namespace: String,
      tableName: String
    ): IO[ExloError, Map[String, String]] =
      ZIO
        .attempt {
          val tableId = TableIdentifier.of(namespace, tableName)
          val table   = catalog.loadTable(tableId)

          // Get current snapshot (null if table is empty)
          Option(table.currentSnapshot()) match {
            case Some(snapshot) =>
              snapshot.summary().asScala.toMap
            case None           =>
              Map.empty[String, String]
          }
        }
        .mapError(ExloError.StateReadError.apply)

    def writeAndStageRecords(
      namespace: String,
      tableName: String,
      records: Chunk[ExloRecord]
    ): IO[ExloError, DataFile] =
      (for {
        // Load table and schema
        tableId <- ZIO.succeed(TableIdentifier.of(namespace, tableName))
        table   <- ZIO.attempt(catalog.loadTable(tableId))
        schema  <- ZIO.succeed(IcebergCatalog.buildExloSchema())

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
              val committedAtOdt = java.time.OffsetDateTime.ofInstant(exloRecord.committedAt, java.time.ZoneOffset.UTC)
              val recordedAtOdt  = java.time.OffsetDateTime.ofInstant(exloRecord.recordedAt, java.time.ZoneOffset.UTC)

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
      namespace: String,
      tableName: String,
      state: String,
      stateVersion: Long
    ): IO[ExloError, Unit] =
      (for {
        tableId       <- ZIO.succeed(TableIdentifier.of(namespace, tableName))
        table         <- ZIO.attempt(catalog.loadTable(tableId))
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
