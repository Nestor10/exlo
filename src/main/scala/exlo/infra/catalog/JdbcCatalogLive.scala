package exlo.infra.catalog

import exlo.domain.*
import exlo.infra.FileAppender
import exlo.infra.IcebergCatalog
import org.apache.iceberg.catalog.Catalog
import zio.*

import scala.jdk.CollectionConverters.*

/**
 * JDBC catalog implementation for IcebergCatalog.
 *
 * JDBC catalog stores Iceberg metadata in a relational database (Postgres, MySQL, etc.).
 * Good for environments without Hive or cloud-managed catalogs.
 */
object JdbcCatalogLive:

  /**
   * Create a JdbcCatalogLive instance for a specific table.
   */
  def make(
    namespace: String,
    tableName: String,
    warehouse: String,
    storage: StorageBackend,
    catalog: CatalogConfig.Jdbc
  ): Task[IcebergCatalog] =
    for {
      jdbcCatalog <- initJdbcCatalog(warehouse, storage, catalog)
      appenderRef <- Ref.make[Option[FileAppender]](None)
    } yield Live(namespace, tableName, jdbcCatalog, appenderRef)

  /**
   * Initialize a JDBC catalog using reflection.
   *
   * Once we add iceberg-jdbc dependency, this becomes:
   * {{{
   * val catalog = new JdbcCatalog()
   * catalog.initialize("jdbc", props.asJava)
   * }}}
   */
  private def initJdbcCatalog(
    warehouse: String,
    storage: StorageBackend,
    catalog: CatalogConfig.Jdbc
  ): Task[Catalog] =
    ZIO.attempt {
      val catalogClass = Class.forName("org.apache.iceberg.jdbc.JdbcCatalog")
      val jdbcCatalog  = catalogClass.getDeclaredConstructor().newInstance().asInstanceOf[Catalog]

      val storageProps = storage.toIcebergProperties
      val catalogProps = Map(
        "warehouse"     -> warehouse,
        "uri"           -> catalog.uri,
        "jdbc.user"     -> catalog.username,
        "jdbc.password" -> catalog.password
      )

      val allProps = storageProps ++ catalogProps

      val initMethod = catalogClass.getMethod("initialize", classOf[String], classOf[java.util.Map[String, String]])
      initMethod.invoke(jdbcCatalog, "jdbc", allProps.asJava)
      jdbcCatalog
    }

  private case class Live(
    namespace: String,
    tableName: String,
    catalog: Catalog,
    appenderRef: Ref[Option[FileAppender]]
  ) extends IcebergCatalog:

    def getTableLocation(namespace: String, tableName: String): IO[ExloError, String] =
      ZIO.fail(ExloError.StateReadError(new NotImplementedError("JDBC implementation pending")))

    def tableExists(namespace: String, tableName: String): IO[ExloError, Boolean] =
      ZIO.fail(ExloError.StateReadError(new NotImplementedError("JDBC implementation pending")))

    def createTable(namespace: String, tableName: String, customLocation: Option[String]): IO[ExloError, Unit] =
      ZIO.fail(ExloError.StateReadError(new NotImplementedError("JDBC implementation pending")))

    def readSnapshotSummary(namespace: String, tableName: String): IO[ExloError, Map[String, String]] =
      ZIO.fail(ExloError.StateReadError(new NotImplementedError("JDBC implementation pending")))

    def writeAndStageRecords(
      namespace: String,
      tableName: String,
      records: Chunk[ExloRecord]
    ): IO[ExloError, DataFile] =
      ZIO.fail(ExloError.IcebergWriteError(new NotImplementedError("JDBC implementation pending")))

    def commitTransaction(
      namespace: String,
      tableName: String,
      state: String,
      stateVersion: Long
    ): IO[ExloError, Unit] =
      ZIO.fail(ExloError.IcebergWriteError(new NotImplementedError("JDBC implementation pending")))
