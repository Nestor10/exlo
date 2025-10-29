package exlo.service

import exlo.domain.ExloError
import zio.*

/** Service for atomically committing records to Iceberg table.
  *
  * Moves records from temporary storage (.temp) to the main table and updates
  * snapshot metadata.
  */
trait TableCommitter:
  
  /** Commit pending records to Iceberg table.
    *
    * @param namespace
    *   Iceberg namespace
    * @param tableName
    *   Iceberg table name
    */
  def commit(namespace: String, tableName: String): IO[ExloError, Unit]

object TableCommitter:
  
  /** Accessor method for commit.
    *
    * Use: `TableCommitter.commit(namespace, tableName)`
    */
  def commit(
    namespace: String,
    tableName: String
  ): ZIO[TableCommitter, ExloError, Unit] =
    ZIO.serviceWithZIO[TableCommitter](_.commit(namespace, tableName))

  /** Live implementation - commits to Iceberg.
    *
    * TODO: Implement in Phase 2
    */
  case class Live() extends TableCommitter:
    def commit(namespace: String, tableName: String): IO[ExloError, Unit] =
      ZIO.fail(
        ExloError.IcebergWriteError(
          new NotImplementedError("Live implementation pending Phase 2")
        )
      )

  val live: ZLayer[Any, Nothing, TableCommitter] =
    ZLayer.succeed(Live())

  /** Stub implementation for testing - tracks commits in memory. */
  case class Stub(var commitCount: Int = 0) extends TableCommitter:
    def commit(namespace: String, tableName: String): IO[ExloError, Unit] =
      ZIO.succeed { commitCount += 1 }

  object Stub:
    val layer: ZLayer[Any, Nothing, TableCommitter] =
      ZLayer.succeed(Stub())
