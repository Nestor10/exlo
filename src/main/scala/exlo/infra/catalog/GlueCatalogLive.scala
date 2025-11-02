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
    config: GlueConfig
  ): Task[IcebergCatalog] =
    for {
      catalog     <- initGlueCatalog(warehouse, config)
      appenderRef <- Ref.make[Option[FileAppender]](None)
    } yield Live(namespace, tableName, catalog, appenderRef)

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
    config: GlueConfig
  ): Task[Catalog] =
    ZIO.attempt {
      val catalogClass = Class.forName("org.apache.iceberg.aws.glue.GlueCatalog")
      val catalog      = catalogClass.getDeclaredConstructor().newInstance().asInstanceOf[Catalog]

      val props = Map(
        "warehouse"   -> warehouse,
        "glue.region" -> config.region
      ) ++ config.catalogId.map("glue.id" -> _).toMap ++
        config.properties

      val initMethod = catalogClass.getMethod("initialize", classOf[String], classOf[java.util.Map[String, String]])
      initMethod.invoke(catalog, "glue", props.asJava)
      catalog
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
