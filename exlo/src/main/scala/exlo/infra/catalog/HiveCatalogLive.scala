package exlo.infra.catalog

import exlo.domain.*
import exlo.infra.FileAppender
import exlo.infra.IcebergCatalog
import org.apache.iceberg.catalog.Catalog
import zio.*

import scala.jdk.CollectionConverters.*

/**
 * Hive catalog implementation for IcebergCatalog.
 *
 * Hive metastore is a traditional choice for on-premise Hadoop environments.
 * Supports concurrent writes via metastore locking.
 */
object HiveCatalogLive:

  /**
   * Create a HiveCatalogLive instance for a specific table.
   */
  def make(
    namespace: String,
    tableName: String,
    warehouse: String,
    storage: StorageBackend,
    catalog: CatalogConfig.Hive
  ): Task[IcebergCatalog] =
    for {
      hiveCatalog <- initHiveCatalog(warehouse, storage, catalog)
      appenderRef <- Ref.make[Option[FileAppender]](None)
    } yield Live(namespace, tableName, hiveCatalog, appenderRef)

  /**
   * Initialize a Hive catalog using reflection.
   *
   * Once we add iceberg-hive-metastore dependency, this becomes:
   * {{{
   * val catalog = new HiveCatalog()
   * catalog.initialize("hive", props.asJava)
   * }}}
   */
  private def initHiveCatalog(
    warehouse: String,
    storage: StorageBackend,
    catalog: CatalogConfig.Hive
  ): Task[Catalog] =
    ZIO.attempt {
      val catalogClass = Class.forName("org.apache.iceberg.hive.HiveCatalog")
      val hiveCatalog  = catalogClass.getDeclaredConstructor().newInstance().asInstanceOf[Catalog]

      val storageProps = storage.toIcebergProperties
      val catalogProps = Map(
        "warehouse" -> warehouse,
        "uri"       -> catalog.uri
      )

      val allProps = storageProps ++ catalogProps

      val initMethod = catalogClass.getMethod("initialize", classOf[String], classOf[java.util.Map[String, String]])
      initMethod.invoke(hiveCatalog, "hive", allProps.asJava)
      hiveCatalog
    }

  private case class Live(
    namespace: String,
    tableName: String,
    catalog: Catalog,
    appenderRef: Ref[Option[FileAppender]]
  ) extends IcebergCatalog:

    def getTableLocation(namespace: String, tableName: String): IO[ExloError, String] =
      ZIO.fail(ExloError.StateReadError(new NotImplementedError("Hive implementation pending")))

    def tableExists(namespace: String, tableName: String): IO[ExloError, Boolean] =
      ZIO.fail(ExloError.StateReadError(new NotImplementedError("Hive implementation pending")))

    def createTable(namespace: String, tableName: String, customLocation: Option[String]): IO[ExloError, Unit] =
      ZIO.fail(ExloError.StateReadError(new NotImplementedError("Hive implementation pending")))

    def readSnapshotSummary(namespace: String, tableName: String): IO[ExloError, Map[String, String]] =
      ZIO.fail(ExloError.StateReadError(new NotImplementedError("Hive implementation pending")))

    def writeAndStageRecords(
      namespace: String,
      tableName: String,
      records: Chunk[ExloRecord]
    ): IO[ExloError, DataFile] =
      ZIO.fail(ExloError.IcebergWriteError(new NotImplementedError("Hive implementation pending")))

    def commitTransaction(
      namespace: String,
      tableName: String,
      state: String,
      stateVersion: Long
    ): IO[ExloError, Unit] =
      ZIO.fail(ExloError.IcebergWriteError(new NotImplementedError("Hive implementation pending")))
