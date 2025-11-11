package exlo.infra

import exlo.domain.ExloError
import org.apache.iceberg.Table
import zio.*

/** Catalog-specific metadata operations.
  *
  * This service encapsulates the catalog-specific parts of Iceberg table management: - Table lookup
  * and existence checking - Table creation and namespace management - Access to the underlying
  * Iceberg Table object
  *
  * Different catalog implementations (Nessie, Glue, Hive, etc.) provide different implementations of
  * this trait, while all share the same IcebergWriter for data operations.
  *
  * This follows the Zionomicon Ch 17 service composition principle: separate concerns into focused
  * services that can be composed.
  *
  * Design Decision: Returns org.apache.iceberg.Table because IcebergWriter needs it for generic
  * operations (Parquet writing, appending files). The Table is an Iceberg SDK type but it's the
  * interface between catalog-specific and generic operations.
  */
trait CatalogOps:

  /** Load a table from the catalog.
    *
    * This is the key operation that bridges catalog-specific lookup to generic Iceberg operations.
    * The returned Table object is used by IcebergWriter for data operations.
    *
    * @param namespace
    *   Iceberg namespace
    * @param tableName
    *   Iceberg table name
    * @return
    *   Iceberg Table instance
    */
  def loadTable(
      namespace: String,
      tableName: String
  ): IO[ExloError, Table]

  /** Check if a table exists in the catalog.
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

  /** Create a new table in the catalog.
    *
    * Creates the table with EXLO's fixed schema and unpartitioned spec. If the table already exists,
    * this is a no-op.
    *
    * @param namespace
    *   Iceberg namespace
    * @param tableName
    *   Iceberg table name
    * @param customLocation
    *   Optional custom storage location for the table
    * @return
    *   Effect that succeeds if table created or already exists
    */
  def createTable(
      namespace: String,
      tableName: String,
      customLocation: Option[String]
  ): IO[ExloError, Unit]

  /** Get the storage location for a table.
    *
    * Returns the base path where data files are stored. Useful for debugging and logging.
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

object CatalogOps:

  /** Accessor for loadTable.
    *
    * Use: `CatalogOps.loadTable(namespace, tableName)`
    */
  def loadTable(
      namespace: String,
      tableName: String
  ): ZIO[CatalogOps, ExloError, Table] =
    ZIO.serviceWithZIO[CatalogOps](_.loadTable(namespace, tableName))

  /** Accessor for tableExists.
    *
    * Use: `CatalogOps.tableExists(namespace, tableName)`
    */
  def tableExists(
      namespace: String,
      tableName: String
  ): ZIO[CatalogOps, ExloError, Boolean] =
    ZIO.serviceWithZIO[CatalogOps](_.tableExists(namespace, tableName))

  /** Accessor for createTable.
    *
    * Use: `CatalogOps.createTable(namespace, tableName, customLocation)`
    */
  def createTable(
      namespace: String,
      tableName: String,
      customLocation: Option[String] = None
  ): ZIO[CatalogOps, ExloError, Unit] =
    ZIO.serviceWithZIO[CatalogOps](_.createTable(namespace, tableName, customLocation))

  /** Accessor for getTableLocation.
    *
    * Use: `CatalogOps.getTableLocation(namespace, tableName)`
    */
  def getTableLocation(
      namespace: String,
      tableName: String
  ): ZIO[CatalogOps, ExloError, String] =
    ZIO.serviceWithZIO[CatalogOps](_.getTableLocation(namespace, tableName))
