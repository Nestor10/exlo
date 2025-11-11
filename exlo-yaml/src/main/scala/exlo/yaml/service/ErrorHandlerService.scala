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
  case class Live(runtimeContext: Option[RuntimeContext]) extends ErrorHandlerService:

    /**
     * Build retry policy from ErrorHandler spec.
     *
     * Strategy:
     *   1. DefaultErrorHandler → single retry policy with backoff
     *   2. CompositeErrorHandler → chain multiple policies (try each in order)
     *
     * Note: For header-based backoff strategies, we need to extract delay from
     * the error at retry time. This is done using a combination of Schedule.recurWhileZIO
     * and explicit ZIO.sleep() calls within the retry logic.
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
            .reduceOption(_ || _) // Union: use shorter delay, continue while either wants to
            .getOrElse(Schedule.exponential(1.second))

      // 3. Max retries
      val maxRetriesSchedule = Schedule.recurs(handler.maxRetries)

      // Combine: filter AND backoff AND max retries
      filterSchedule && backoffSchedule && maxRetriesSchedule

    /**
     * Build a backoff schedule from a BackoffStrategy.
     *
     * Returns Schedule[Any, Any, Any] - flexible signature that works with
     * ZIO's schedule composition operators.
     */
    private def buildBackoffSchedule(strategy: BackoffStrategy): Schedule[Any, Any, Any] =
      strategy match
        case BackoffStrategy.ExponentialBackoff(factor) =>
          // factor * (2 ^ attempt_number) seconds
          // ZIO's exponential starts at base duration and doubles each time
          Schedule.exponential(factor.second)

        case BackoffStrategy.ConstantBackoff(backoffTime) =>
          // Convert Double seconds to Duration and create a spaced schedule
          val duration = Duration.fromMillis((backoffTime * 1000).toLong)
          Schedule.spaced(duration)

        case strategy @ BackoffStrategy.WaitTimeFromHeader(header, regex) =>
          // Extract wait time (in seconds) from response header via RuntimeContext
          // Example: Retry-After: 120 (wait 120 seconds)
          buildHeaderBasedSchedule(extractWaitTimeFromContext(header, regex))

        case strategy @ BackoffStrategy.WaitUntilTimeFromHeader(header, regex, minWait) =>
          // Extract timestamp from response header via RuntimeContext
          // Example: X-Rate-Limit-Reset: 1699564800 (Unix timestamp)
          buildHeaderBasedSchedule(extractWaitUntilFromContext(header, regex, minWait))

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
        case YamlRuntimeError.HttpError(_, status, body, _) =>
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
     * Build a schedule for header-based backoff strategies.
     *
     * Strategy: Read headers from RuntimeContext and extract delay dynamically.
     * The extractor function reads from RuntimeContext (via ZIO effect) to get the
     * delay from response headers stored by HttpClient.
     *
     * Uses Schedule.delayed to dynamically compute delays on each retry based on
     * header values extracted from RuntimeContext.
     */
    private def buildHeaderBasedSchedule(
      extractor: UIO[Option[Duration]]
    ): Schedule[Any, Any, Any] =
      runtimeContext match
        case Some(ctx) =>
          // Create a schedule that queries RuntimeContext for delay on each retry
          // Use Schedule.recurWhile to continue retrying, then modifyDelayZIO to
          // extract dynamic delays from RuntimeContext
          (Schedule.fixed(1.second) && Schedule.recurs(10))
            .modifyDelayZIO { (_, _) =>
              // Extract delay from RuntimeContext headers on each retry
              extractor.map(_.getOrElse(1.second))
            }

        case None =>
          // No RuntimeContext - fallback to exponential backoff
          Schedule.exponential(1.second) && Schedule.recurs(10)

    /**
     * Extract wait time from RuntimeContext headers.
     *
     * Reads the last response headers from RuntimeContext and extracts the wait time.
     * Example: Retry-After: 120 (wait 120 seconds)
     *
     * @param headerName Name of header containing wait time (e.g., "Retry-After")
     * @param regex Optional regex to extract numeric value from header
     * @return ZIO effect that extracts duration from RuntimeContext
     */
    private def extractWaitTimeFromContext(
      headerName: String,
      regex: Option[String]
    ): UIO[Option[Duration]] =
      runtimeContext match
        case Some(ctx) =>
          ctx.getHeaders.map { headers =>
            val headerValue = headers
              .find { case (name, _) => name.equalsIgnoreCase(headerName) }
              .map(_._2)

            headerValue.flatMap { value =>
              val extractedValue = regex match
                case Some(pattern) =>
                  pattern.r.findFirstMatchIn(value).flatMap { m =>
                    if m.groupCount > 0 then Some(m.group(1)) else Some(value)
                  }
                case None          =>
                  Some(value.trim)

              extractedValue
                .flatMap(_.toDoubleOption)
                .map(seconds => Duration.fromMillis((seconds * 1000).toLong))
            }
          }
        case None      =>
          ZIO.succeed(None)

    /**
     * Extract wait time from response headers (pure function for error inspection).
     *
     * @param headerName Name of header containing wait time (e.g., "Retry-After")
     * @param regex Optional regex to extract numeric value from header
     * @return Function that extracts duration from error
     */
    private def extractWaitTimeFromHeader(
      headerName: String,
      regex: Option[String]
    ): Throwable => Option[Duration] = { error =>
      error match
        case YamlRuntimeError.HttpError(_, _, _, headers) =>
          val headerValue = headers
            .find { case (name, _) => name.equalsIgnoreCase(headerName) }
            .map(_._2)

          headerValue.flatMap { value =>
            val extractedValue = regex match
              case Some(pattern) =>
                pattern.r.findFirstMatchIn(value).flatMap { m =>
                  if m.groupCount > 0 then Some(m.group(1)) else Some(value)
                }
              case None          =>
                Some(value.trim)

            extractedValue
              .flatMap(_.toDoubleOption)
              .map(seconds => Duration.fromMillis((seconds * 1000).toLong))
          }
        case _                                            =>
          None
    }

    /**
     * Extract wait-until timestamp from RuntimeContext headers.
     *
     * Reads the last response headers from RuntimeContext and extracts a timestamp,
     * then calculates the duration until that time.
     * Example: X-RateLimit-Reset: 1699564800 (Unix timestamp)
     *
     * @param headerName Name of header containing timestamp
     * @param regex Optional regex to extract timestamp from header
     * @param minWait Minimum wait time in seconds
     * @return ZIO effect that extracts duration from RuntimeContext
     */
    private def extractWaitUntilFromContext(
      headerName: String,
      regex: Option[String],
      minWait: Option[Double]
    ): UIO[Option[Duration]] =
      runtimeContext match
        case Some(ctx) =>
          ctx.getHeaders.map { headers =>
            val headerValue = headers
              .find { case (name, _) => name.equalsIgnoreCase(headerName) }
              .map(_._2)

            headerValue.flatMap { value =>
              val extractedValue = regex match
                case Some(pattern) =>
                  pattern.r.findFirstMatchIn(value).flatMap { m =>
                    if m.groupCount > 0 then Some(m.group(1)) else Some(value)
                  }
                case None          =>
                  Some(value.trim)

              extractedValue.flatMap { str =>
                str.toLongOption.map { timestampSeconds =>
                  val now        = java.lang.System.currentTimeMillis()
                  val targetTime = timestampSeconds * 1000
                  val waitMillis = Math.max(0, targetTime - now)

                  val finalWaitMillis = minWait match
                    case Some(minSeconds) =>
                      Math.max(waitMillis, (minSeconds * 1000).toLong)
                    case None             =>
                      waitMillis

                  Duration.fromMillis(finalWaitMillis)
                }
              }
            }
          }
        case None      =>
          ZIO.succeed(None)

    /**
     * Extract wait-until timestamp from response headers (pure function for error inspection).
     *
     * @param headerName Name of header containing timestamp
     * @param regex Optional regex to extract timestamp from header
     * @param minWait Minimum wait time in seconds
     * @return Function that extracts duration from error
     */
    private def extractWaitUntilFromHeader(
      headerName: String,
      regex: Option[String],
      minWait: Option[Double]
    ): Throwable => Option[Duration] = { error =>
      error match
        case YamlRuntimeError.HttpError(_, _, _, headers) =>
          val headerValue = headers
            .find { case (name, _) => name.equalsIgnoreCase(headerName) }
            .map(_._2)

          headerValue.flatMap { value =>
            val extractedValue = regex match
              case Some(pattern) =>
                pattern.r.findFirstMatchIn(value).flatMap { m =>
                  if m.groupCount > 0 then Some(m.group(1)) else Some(value)
                }
              case None          =>
                Some(value.trim)

            extractedValue.flatMap { str =>
              str.toLongOption.map { timestampSeconds =>
                val now        = java.lang.System.currentTimeMillis()
                val targetTime = timestampSeconds * 1000
                val waitMillis = Math.max(0, targetTime - now)

                val finalWaitMillis = minWait match
                  case Some(minSeconds) =>
                    Math.max(waitMillis, (minSeconds * 1000).toLong)
                  case None             =>
                    waitMillis

                Duration.fromMillis(finalWaitMillis)
              }
            }
          }
        case _                                            =>
          None
    }

    /**
     * Extract wait time from response headers.
     *
     * @param error The error containing response headers
     * @param headerName Name of header containing wait time (e.g., "Retry-After")
     * @param regex Optional regex to extract numeric value from header
     * @return Wait duration, or None if header not found
     */
    private def extractWaitTimeFromHeaders(
      error: Throwable,
      headerName: String,
      regex: Option[String]
    ): UIO[Option[Duration]] =
      error match
        case YamlRuntimeError.HttpError(_, _, _, headers) =>
          ZIO.succeed {
            // Header lookup is case-insensitive per HTTP spec
            val headerValue = headers
              .find { case (name, _) => name.equalsIgnoreCase(headerName) }
              .map(_._2)

            headerValue.flatMap { value =>
              // Apply regex if provided, otherwise use full value
              val extractedValue = regex match
                case Some(pattern) =>
                  // Extract first capturing group
                  pattern.r.findFirstMatchIn(value).flatMap { m =>
                    if m.groupCount > 0 then Some(m.group(1)) else Some(value)
                  }
                case None          =>
                  Some(value.trim)

              // Parse as seconds (Double to support fractional seconds)
              extractedValue.flatMap { str =>
                str.toDoubleOption.map(seconds => Duration.fromMillis((seconds * 1000).toLong))
              }
            }
          }

        case _ =>
          ZIO.succeed(None)

    /**
     * Extract wait-until timestamp from response headers.
     *
     * @param error The error containing response headers
     * @param headerName Name of header containing timestamp
     * @param regex Optional regex to extract timestamp from header
     * @param minWait Minimum wait time in seconds
     * @return Wait duration until timestamp, or None if header not found
     */
    private def extractWaitUntilFromHeaders(
      error: Throwable,
      headerName: String,
      regex: Option[String],
      minWait: Option[Double]
    ): UIO[Option[Duration]] =
      error match
        case YamlRuntimeError.HttpError(_, _, _, headers) =>
          ZIO.succeed {
            // Header lookup is case-insensitive per HTTP spec
            val headerValue = headers
              .find { case (name, _) => name.equalsIgnoreCase(headerName) }
              .map(_._2)

            headerValue.flatMap { value =>
              // Apply regex if provided, otherwise use full value
              val extractedValue = regex match
                case Some(pattern) =>
                  pattern.r.findFirstMatchIn(value).flatMap { m =>
                    if m.groupCount > 0 then Some(m.group(1)) else Some(value)
                  }
                case None          =>
                  Some(value.trim)

              // Parse as Unix timestamp (seconds since epoch)
              extractedValue.flatMap { str =>
                str.toLongOption.map { timestampSeconds =>
                  // Calculate duration from now until timestamp
                  val now        = java.lang.System.currentTimeMillis()
                  val targetTime = timestampSeconds * 1000 // Convert to millis
                  val waitMillis = Math.max(0, targetTime - now)

                  // Apply minWait if specified
                  val finalWaitMillis = minWait match
                    case Some(minSeconds) =>
                      Math.max(waitMillis, (minSeconds * 1000).toLong)
                    case None             =>
                      waitMillis

                  Duration.fromMillis(finalWaitMillis)
                }
              }
            }
          }

        case _ =>
          ZIO.succeed(None)

  /**
   * ZLayer for live ErrorHandlerService without RuntimeContext.
   *
   * Legacy layer - header-based backoff will fall back to exponential.
   */
  val live: ULayer[ErrorHandlerService] = ZLayer.succeed(Live(None))

  /**
   * ZLayer for live ErrorHandlerService with RuntimeContext.
   *
   * Enables header-based backoff strategies.
   */
  val liveWithContext: URLayer[RuntimeContext, ErrorHandlerService] =
    ZLayer {
      for ctx <- ZIO.service[RuntimeContext]
      yield Live(Some(ctx))
    }

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
