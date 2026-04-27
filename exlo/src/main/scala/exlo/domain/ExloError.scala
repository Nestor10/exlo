package exlo.domain

sealed abstract class ExloError(message: String, cause: Throwable | Null)
    extends Exception(message, cause)

object ExloError:

  final case class StorageError(message: String, cause: Throwable)
      extends ExloError(message, cause)

  final case class StateError(message: String, cause: Throwable)
      extends ExloError(message, cause)

  final case class ConnectorFailure(message: String, cause: Throwable | Null = null)
      extends ExloError(message, cause)

  /**
   * The starting snapshot for an incremental scan is no longer an ancestor of the table's
   * current snapshot — typically because `expire_snapshots` ran past it. This is not
   * recoverable without operator action: the child connector cannot continue contiguous
   * ingestion. Surfaced loudly to the orchestration layer; manual state reset and
   * backfill required.
   */
  final case class SnapshotExpired(snapshotId: Long, cause: Throwable)
      extends ExloError(
        s"snapshot $snapshotId is no longer an ancestor of the current snapshot — " +
          "expired by table maintenance. Manual state reset and backfill required.",
        cause
      )
