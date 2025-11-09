package exlo.yaml.service

import exlo.yaml.domain.YamlRuntimeError
import exlo.yaml.spec.*
import zio.*

/**
 * Service for interpreting ErrorHandler ADTs into ZIO retry policies.
 *
 * Design:
 *   - Pure interpreter: converts error handler specs into Schedule policies
 *   - Testable without HTTP: can verify retry behavior with mock failures
 *   - Compositional: injected via ZIO environment like other services
 *
 * Following Zionomicon Chapter 17 patterns for service-oriented design.
 */
trait ErrorHandlerService:
  /**
   * Build a retry schedule from an ErrorHandler specification.
   *
   * @param errorHandler
   *   Error handler configuration from YAML
   * @return
   *   ZIO Schedule for retry policy
   */
  def buildRetryPolicy(errorHandler: ErrorHandler): Schedule[Any, Throwable, Any]

  /**
   * Check if an error should be retried based on response filters.
   *
   * @param error
   *   The error to check
   * @param filters
   *   List of response filters
   * @return
   *   Action to take (RETRY, FAIL, IGNORE, SUCCESS)
   */
  def matchResponseFilters(error: Throwable, filters: List[HttpResponseFilter]): UIO[ResponseAction]

object ErrorHandlerService:

  /**
   * Live implementation of ErrorHandlerService.
   */
  case class Live() extends ErrorHandlerService:

    /**
     * Build retry policy from ErrorHandler spec.
     *
     * Strategy:
     *   1. DefaultErrorHandler → single retry policy with backoff
     *   2. CompositeErrorHandler → chain multiple policies (try each in order)
     */
    def buildRetryPolicy(errorHandler: ErrorHandler): Schedule[Any, Throwable, Any] =
      errorHandler match
        case default: ErrorHandler.DefaultErrorHandler =>
          buildDefaultPolicy(default)

        case composite: ErrorHandler.CompositeErrorHandler =>
          // Composite: try each handler in sequence
          // Use first matching handler's policy
          composite.errorHandlers
            .map(buildDefaultPolicy)
            .reduceOption(_ || _) // Try first policy, fallback to second, etc.
            .getOrElse(Schedule.stop) // No handlers = no retry

    /**
     * Build retry policy for DefaultErrorHandler.
     *
     * Combines:
     *   1. Response filter matching (which errors to retry)
     *   2. Backoff strategy (how long to wait)
     *   3. Max retries limit
     */
    private def buildDefaultPolicy(handler: ErrorHandler.DefaultErrorHandler): Schedule[Any, Throwable, Any] =
      // 1. Filter: only retry if response filters match
      val filterSchedule = Schedule.recurWhileZIO[Any, Throwable] { error =>
        matchResponseFilters(error, handler.responseFilters).map(_ == ResponseAction.RETRY)
      }

      // 2. Backoff: build backoff schedule from strategies
      val backoffSchedule = handler.backoffStrategies match
        case Nil        =>
          // Default: exponential backoff with factor 5 (Airbyte default)
          buildBackoffSchedule(BackoffStrategy.ExponentialBackoff(5))
        case strategies =>
          // Use first strategy (fallback pattern handled in backoff builder)
          strategies
            .map(buildBackoffSchedule)
            .reduceOption(_ || _) // Try first backoff, fallback to second
            .getOrElse(Schedule.exponential(1.second))

      // 3. Max retries
      val maxRetriesSchedule = Schedule.recurs(handler.maxRetries)

      // Combine: filter AND backoff AND max retries
      filterSchedule && backoffSchedule && maxRetriesSchedule

    /**
     * Build a backoff schedule from a BackoffStrategy.
     */
    private def buildBackoffSchedule(strategy: BackoffStrategy): Schedule[Any, Any, Any] =
      strategy match
        case BackoffStrategy.ExponentialBackoff(factor) =>
          // factor * (2 ^ attempt_number) seconds
          // ZIO's exponential starts at base duration and doubles each time
          Schedule.exponential(factor.second)

        case BackoffStrategy.ConstantBackoff(backoffTime) =>
          // Convert Double seconds to Duration
          val durationMillis = (backoffTime * 1000).toLong
          Schedule.fixed(Duration.fromMillis(durationMillis))

        case BackoffStrategy.WaitTimeFromHeader(header, regex) =>
          // TODO: Extract wait time from response headers
          // For now, fallback to default exponential
          // This requires access to the HTTP response, not just the error
          Schedule.exponential(1.second)

        case BackoffStrategy.WaitUntilTimeFromHeader(header, regex, minWait) =>
          // TODO: Extract timestamp from response headers
          // For now, fallback to default exponential
          Schedule.exponential(1.second)

    /**
     * Match error against response filters to determine action.
     *
     * Strategy:
     *   1. Iterate through filters in order
     *   2. Return action for first matching filter
     *   3. Default: FAIL if no filters match
     */
    def matchResponseFilters(error: Throwable, filters: List[HttpResponseFilter]): UIO[ResponseAction] =
      error match
        case YamlRuntimeError.HttpError(_, status, body) =>
          ZIO.succeed {
            filters
              .find { filter =>
                // Check HTTP status code match
                val statusMatches = filter.httpCodes.contains(status)

                // Check error message match
                val messageMatches = filter.errorMessageContains match
                  case Some(substring) => body.contains(substring)
                  case None            => false

                // TODO: Check predicate match (requires template evaluation)
                // For now, just use status and message matching
                statusMatches || messageMatches
              }
              .map(_.action)
              .getOrElse(ResponseAction.FAIL) // No match = fail
          }

        case _ =>
          // Non-HTTP errors: check if any filter handles them
          // For now, default to FAIL
          ZIO.succeed(ResponseAction.FAIL)

  /**
   * ZLayer for live ErrorHandlerService.
   */
  val live: ULayer[ErrorHandlerService] = ZLayer.succeed(Live())

  /**
   * Accessor methods for service.
   */
  def buildRetryPolicy(errorHandler: ErrorHandler): URIO[ErrorHandlerService, Schedule[Any, Throwable, Any]] =
    ZIO.serviceWith[ErrorHandlerService](_.buildRetryPolicy(errorHandler))

  def matchResponseFilters(
    error: Throwable,
    filters: List[HttpResponseFilter]
  ): URIO[ErrorHandlerService, ResponseAction] =
    ZIO.serviceWithZIO[ErrorHandlerService](_.matchResponseFilters(error, filters))
