package exlo.domain

/**
 * Error types for EXLO framework operations.
 *
 * Users can extend this with their own error types if needed.
 */
sealed trait ExloError

object ExloError:

  /**
   * Error from external API calls.
   *
   * @param status
   *   HTTP status code
   * @param message
   *   Error message from API
   */
  case class ApiError(status: Int, message: String) extends ExloError

  /**
   * Error reading state from Iceberg.
   *
   * @param cause
   *   Underlying exception
   */
  case class StateReadError(cause: Throwable) extends ExloError

  /**
   * Error writing to Iceberg (records or state).
   *
   * @param cause
   *   Underlying exception
   */
  case class IcebergWriteError(cause: Throwable) extends ExloError

  /**
   * Configuration error (invalid or missing config).
   *
   * @param message
   *   Description of configuration issue
   */
  case class ConfigurationError(message: String) extends ExloError
