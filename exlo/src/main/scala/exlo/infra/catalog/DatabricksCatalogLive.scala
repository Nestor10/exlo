package exlo.infra.catalog

import exlo.domain.*
import exlo.infra.FileAppender
import exlo.infra.IcebergCatalog
import org.apache.iceberg.catalog.Catalog
import zio.*

import scala.jdk.CollectionConverters.*

/**
 * Databricks Unity Catalog implementation for IcebergCatalog.
 *
 * Unity Catalog is Databricks' managed catalog service with built-in governance
 * and multi-cloud support. Best suited for Databricks environments.
 */
object DatabricksCatalogLive:

  /**
   * Create a DatabricksCatalogLive instance for a specific table.
   */
  def make(
    namespace: String,
    tableName: String,
    warehouse: String,
    storage: StorageBackend,
    catalog: CatalogConfig.Databricks
  ): Task[IcebergCatalog] =
    for {
      databricksCatalog <- initDatabricksCatalog(warehouse, storage, catalog)
      appenderRef       <- Ref.make[Option[FileAppender]](None)
    } yield Live(namespace, tableName, databricksCatalog, appenderRef)

  /**
   * Initialize a Databricks catalog using reflection.
   *
   * Once we add iceberg-databricks dependency, this becomes:
   * {{{
   * val catalog = new DatabricksCatalog()
   * catalog.initialize("databricks", props.asJava)
   * }}}
   */
  private def initDatabricksCatalog(
    warehouse: String,
    storage: StorageBackend,
    catalog: CatalogConfig.Databricks
  ): Task[Catalog] =
    ZIO.attempt {
      val catalogClass      = Class.forName("org.apache.iceberg.databricks.DatabricksCatalog")
      val databricksCatalog = catalogClass.getDeclaredConstructor().newInstance().asInstanceOf[Catalog]

      val storageProps = storage.toIcebergProperties
      val catalogProps = Map(
        "warehouse"    -> warehouse,
        "uri"          -> catalog.workspace,
        "catalog-name" -> catalog.catalogName
      )

      val allProps = storageProps ++ catalogProps

      val initMethod = catalogClass.getMethod("initialize", classOf[String], classOf[java.util.Map[String, String]])
      initMethod.invoke(databricksCatalog, "databricks", allProps.asJava)
      databricksCatalog
    }

  private case class Live(
    namespace: String,
    tableName: String,
    catalog: Catalog,
    appenderRef: Ref[Option[FileAppender]]
  ) extends IcebergCatalog:

    def getTableLocation(namespace: String, tableName: String): IO[ExloError, String] =
      ZIO.fail(ExloError.StateReadError(new NotImplementedError("Databricks implementation pending")))

    def tableExists(namespace: String, tableName: String): IO[ExloError, Boolean] =
      ZIO.fail(ExloError.StateReadError(new NotImplementedError("Databricks implementation pending")))

    def createTable(namespace: String, tableName: String, customLocation: Option[String]): IO[ExloError, Unit] =
      ZIO.fail(ExloError.StateReadError(new NotImplementedError("Databricks implementation pending")))

    def readSnapshotSummary(namespace: String, tableName: String): IO[ExloError, Map[String, String]] =
      ZIO.fail(ExloError.StateReadError(new NotImplementedError("Databricks implementation pending")))

    def writeAndStageRecords(
      namespace: String,
      tableName: String,
      records: Chunk[ExloRecord]
    ): IO[ExloError, DataFile] =
      ZIO.fail(ExloError.IcebergWriteError(new NotImplementedError("Databricks implementation pending")))

    def commitTransaction(
      namespace: String,
      tableName: String,
      state: String,
      stateVersion: Long
    ): IO[ExloError, Unit] =
      ZIO.fail(ExloError.IcebergWriteError(new NotImplementedError("Databricks implementation pending")))
