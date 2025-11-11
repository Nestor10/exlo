package exlo.infra.catalog

import exlo.domain.*
import exlo.infra.CatalogOps
import org.apache.iceberg.PartitionSpec
import org.apache.iceberg.Table
import org.apache.iceberg.catalog.Catalog
import org.apache.iceberg.catalog.Namespace
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.nessie.NessieCatalog
import zio.*

import scala.jdk.CollectionConverters.*

/**
 * Nessie catalog operations implementation.
 *
 * Provides catalog-specific metadata operations for Nessie: - Initializing Nessie catalog with
 * authentication - Loading tables from Nessie's Git-like version control - Creating tables with
 * namespace management - Checking table existence
 *
 * Nessie is the CATALOG (metadata) - data files can be stored anywhere: - AWS S3 (using iceberg-aws
 * dependency + S3FileIO) - Google Cloud Storage (using iceberg-gcp dependency + GcsFileIO) - Azure
 * Blob Storage (using iceberg-azure dependency + ADLSFileIO) - HDFS (using hadoop-hdfs dependency +
 * HadoopFileIO) - Local filesystem (for testing)
 *
 * Configuration is passed via NessieConfig.properties map. Iceberg auto-selects FileIO based on
 * warehouse path and properties.
 */
object NessieCatalogOps:

  /**
   * Create a NessieCatalogOps layer.
   *
   * @param warehouse
   *   Base path for Iceberg warehouse (e.g., "s3://bucket/warehouse")
   * @param storage
   *   Storage backend configuration (S3, GCS, Azure, etc.)
   * @param catalog
   *   Nessie catalog configuration (URI, branch, auth)
   * @return
   *   ZLayer that provides CatalogOps
   */
  def layer(
    warehouse: String,
    storage: StorageBackend,
    catalog: CatalogConfig.Nessie
  ): ZLayer[Any, Throwable, CatalogOps] =
    ZLayer.scoped {
      ZIO.acquireRelease(
        initNessieCatalog(warehouse, storage, catalog).map(Live(_))
      )(_ => ZIO.unit) // Catalog doesn't need explicit cleanup
    }

  /**
   * Initialize a Nessie catalog.
   *
   * Combines storage backend properties with Nessie catalog properties. Storage backend determines
   * FileIO implementation (S3FileIO, GcsFileIO, etc.).
   *
   * @param warehouse
   *   Base path for Iceberg warehouse
   * @param storage
   *   Storage backend configuration (generates FileIO properties)
   * @param catalog
   *   Nessie catalog configuration (URI, branch, auth)
   * @return
   *   Initialized Nessie catalog
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
   * Live implementation - Nessie-backed CatalogOps.
   */
  private case class Live(
    catalog: Catalog
  ) extends CatalogOps:

    def loadTable(
      namespace: String,
      tableName: String
    ): IO[ExloError, Table] =
      ZIO
        .attempt {
          val tableId = TableIdentifier.of(namespace, tableName)
          catalog.loadTable(tableId)
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
          try nsCatalog.loadNamespaceMetadata(namespaceId)
          catch {
            case _: org.apache.iceberg.exceptions.NoSuchNamespaceException =>
              nsCatalog.createNamespace(namespaceId)
          }

          // Skip if table already exists
          if (!catalog.tableExists(tableId)) {
            val schema = exlo.infra.IcebergCatalog.buildExloSchema()

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
