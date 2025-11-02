package exlo.infra.catalog

import exlo.domain.*
import exlo.infra.FileAppender
import exlo.infra.IcebergCatalog
import org.apache.iceberg.catalog.Catalog
import zio.*

import scala.jdk.CollectionConverters.*

/**
 * AWS Glue catalog implementation for IcebergCatalog.
 *
 * AWS Glue provides a managed Hive-compatible metastore for Iceberg tables.
 * Best suited for AWS environments using S3 storage.
 *
 * NOTE: Requires iceberg-aws dependency (not yet added to build.sbt).
 * Using reflection for now to avoid adding the dependency prematurely.
 */
object GlueCatalogLive:

  /**
   * Create a GlueCatalogLive instance for a specific table.
   */
  def make(
    namespace: String,
    tableName: String,
    warehouse: String,
    storage: StorageBackend,
    catalog: CatalogConfig.Glue
  ): Task[IcebergCatalog] =
    for {
      glueCatalog <- initGlueCatalog(warehouse, storage, catalog)
      appenderRef <- Ref.make[Option[FileAppender]](None)
    } yield Live(namespace, tableName, glueCatalog, appenderRef)

  /**
   * Initialize a Glue catalog using reflection.
   *
   * Once we add iceberg-aws dependency, this becomes:
   * {{{
   * val catalog = new GlueCatalog()
   * catalog.initialize("glue", props.asJava)
   * }}}
   */
  private def initGlueCatalog(
    warehouse: String,
    storage: StorageBackend,
    catalog: CatalogConfig.Glue
  ): Task[Catalog] =
    ZIO.attempt {
      val catalogClass = Class.forName("org.apache.iceberg.aws.glue.GlueCatalog")
      val glueCatalog  = catalogClass.getDeclaredConstructor().newInstance().asInstanceOf[Catalog]

      // Merge storage properties (should be S3) with Glue catalog properties
      val storageProps = storage.toIcebergProperties
      val catalogProps = Map(
        "warehouse"   -> warehouse,
        "glue.region" -> catalog.region
      ) ++ catalog.catalogId.map("glue.id" -> _).toMap

      val allProps = storageProps ++ catalogProps

      val initMethod = catalogClass.getMethod("initialize", classOf[String], classOf[java.util.Map[String, String]])
      initMethod.invoke(glueCatalog, "glue", allProps.asJava)
      glueCatalog
    }

  private case class Live(
    namespace: String,
    tableName: String,
    catalog: Catalog,
    appenderRef: Ref[Option[FileAppender]]
  ) extends IcebergCatalog:

    def getTableLocation(namespace: String, tableName: String): IO[ExloError, String] =
      ZIO.fail(ExloError.StateReadError(new NotImplementedError("Glue implementation pending")))

    def tableExists(namespace: String, tableName: String): IO[ExloError, Boolean] =
      ZIO.fail(ExloError.StateReadError(new NotImplementedError("Glue implementation pending")))

    def createTable(namespace: String, tableName: String, customLocation: Option[String]): IO[ExloError, Unit] =
      ZIO.fail(ExloError.StateReadError(new NotImplementedError("Glue implementation pending")))

    def readSnapshotSummary(namespace: String, tableName: String): IO[ExloError, Map[String, String]] =
      ZIO.fail(ExloError.StateReadError(new NotImplementedError("Glue implementation pending")))

    def writeAndStageRecords(
      namespace: String,
      tableName: String,
      records: Chunk[ExloRecord]
    ): IO[ExloError, DataFile] =
      ZIO.fail(ExloError.IcebergWriteError(new NotImplementedError("Glue implementation pending")))

    def commitTransaction(
      namespace: String,
      tableName: String,
      state: String,
      stateVersion: Long
    ): IO[ExloError, Unit] =
      ZIO.fail(ExloError.IcebergWriteError(new NotImplementedError("Glue implementation pending")))
