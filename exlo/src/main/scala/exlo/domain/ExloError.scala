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
