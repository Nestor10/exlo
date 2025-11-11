package exlo.yaml.service

import exlo.yaml.domain.YamlRuntimeError
import exlo.yaml.spec.*
import zio.*
import zio.test.*
import zio.test.TestAspect.*

/**
 * Unit tests for ErrorHandlerService.
 *
 * Key testing strategy:
 *   - Test retry logic WITHOUT mocking HTTP endpoints
 *   - Use simple counter effects to verify retry behavior
 *   - Use TestClock to verify backoff timing
 *   - Test response filter matching in isolation
 *
 * This demonstrates the power of our compositional design: error handling is
 * pure logic that can be tested independently of HTTP infrastructure.
 */
object ErrorHandlerServiceSpec extends ZIOSpecDefault:

  def spec = suite("ErrorHandlerService")(
    suite("matchResponseFilters")(
      test("matches HTTP 429 status code") {
        val filters = List(
          HttpResponseFilter(ResponseAction.RETRY, httpCodes = List(429))
        )
        val error   = YamlRuntimeError.HttpError("GET /api/users", 429, "Rate limit exceeded")

        for
          service <- ZIO.service[ErrorHandlerService]
          action  <- service.matchResponseFilters(error, filters)
        yield assertTrue(action == ResponseAction.RETRY)
      },
      test("matches HTTP 5xx status codes") {
        val filters = List(
          HttpResponseFilter(ResponseAction.RETRY, httpCodes = List(500, 502, 503))
        )
        val error   = YamlRuntimeError.HttpError("GET /api/data", 502, "Bad Gateway")

        for
          service <- ZIO.service[ErrorHandlerService]
          action  <- service.matchResponseFilters(error, filters)
        yield assertTrue(action == ResponseAction.RETRY)
      },
      test("matches error message substring") {
        val filters = List(
          HttpResponseFilter(
            ResponseAction.IGNORE,
            errorMessageContains = Some("not found")
          )
        )
        val error   = YamlRuntimeError.HttpError("GET /api/missing", 404, "Resource not found")

        for
          service <- ZIO.service[ErrorHandlerService]
          action  <- service.matchResponseFilters(error, filters)
        yield assertTrue(action == ResponseAction.IGNORE)
      },
      test("returns FAIL when no filters match") {
        val filters = List(
          HttpResponseFilter(ResponseAction.RETRY, httpCodes = List(429))
        )
        val error   = YamlRuntimeError.HttpError("GET /api/data", 403, "Forbidden")

        for
          service <- ZIO.service[ErrorHandlerService]
          action  <- service.matchResponseFilters(error, filters)
        yield assertTrue(action == ResponseAction.FAIL)
      },
      test("returns first matching filter action") {
        val filters = List(
          HttpResponseFilter(ResponseAction.IGNORE, httpCodes = List(404)),
          HttpResponseFilter(ResponseAction.RETRY, httpCodes = List(404)) // Should not be used
        )
        val error   = YamlRuntimeError.HttpError("GET /api/data", 404, "Not Found")

        for
          service <- ZIO.service[ErrorHandlerService]
          action  <- service.matchResponseFilters(error, filters)
        yield assertTrue(action == ResponseAction.IGNORE) // First filter wins
      },
      test("handles non-HTTP errors") {
        val filters = List(
          HttpResponseFilter(ResponseAction.RETRY, httpCodes = List(500))
        )
        val error   = new RuntimeException("Network timeout")

        for
          service <- ZIO.service[ErrorHandlerService]
          action  <- service.matchResponseFilters(error, filters)
        yield assertTrue(action == ResponseAction.FAIL) // Non-HTTP errors fail by default
      }
    ),
    suite("buildRetryPolicy - DefaultErrorHandler")(
      test("retries on matching HTTP status code") {
        val handler = ErrorHandler.DefaultErrorHandler(
          maxRetries = 3,
          backoffStrategies = List(BackoffStrategy.ExponentialBackoff(1)), // 1s base
          responseFilters = List(
            HttpResponseFilter(ResponseAction.RETRY, httpCodes = List(429))
          )
        )

        for
          service <- ZIO.service[ErrorHandlerService]
          policy = service.buildRetryPolicy(handler)
          attemptCount <- Ref.make(0)

          // Simulate failing effect that counts attempts
          effect = attemptCount.updateAndGet(_ + 1).flatMap { count =>
            if count < 3 then ZIO.fail(YamlRuntimeError.HttpError("GET /api", 429, "Rate limited"))
            else ZIO.succeed("success")
          }

          // Fork the retry and advance clock
          fiber  <- effect.retry(policy).fork
          _      <- TestClock.adjust(1.second)  // First retry
          _      <- TestClock.adjust(2.seconds) // Second retry
          result <- fiber.join
          count  <- attemptCount.get
        yield assertTrue(
          result == "success",
          count == 3 // Original attempt + 2 retries
        )
      },
      test("stops retrying after max retries") {
        val handler = ErrorHandler.DefaultErrorHandler(
          maxRetries = 2,
          backoffStrategies = List(BackoffStrategy.ExponentialBackoff(1)),
          responseFilters = List(
            HttpResponseFilter(ResponseAction.RETRY, httpCodes = List(500))
          )
        )

        for
          service <- ZIO.service[ErrorHandlerService]
          policy = service.buildRetryPolicy(handler)
          attemptCount <- Ref.make(0)

          // Effect that always fails
          effect = attemptCount.updateAndGet(_ + 1).flatMap { _ =>
            ZIO.fail(YamlRuntimeError.HttpError("GET /api", 500, "Server error"))
          }

          // Fork and advance clock for retries
          fiber  <- effect.retry(policy).fork
          _      <- TestClock.adjust(1.second)  // First retry
          _      <- TestClock.adjust(2.seconds) // Second retry
          result <- fiber.join.either
          count  <- attemptCount.get
        yield assertTrue(
          result.isLeft, // Should fail after max retries
          count == 3 // Original + 2 retries = 3 attempts
        )
      },
      test("does not retry non-matching errors") {
        val handler = ErrorHandler.DefaultErrorHandler(
          maxRetries = 5,
          backoffStrategies = List(BackoffStrategy.ExponentialBackoff(1)),
          responseFilters = List(
            HttpResponseFilter(ResponseAction.RETRY, httpCodes = List(429))
          )
        )

        for
          service <- ZIO.service[ErrorHandlerService]
          policy = service.buildRetryPolicy(handler)
          attemptCount <- Ref.make(0)

          // 403 is not in retry list
          effect = attemptCount.updateAndGet(_ + 1).flatMap { _ =>
            ZIO.fail(YamlRuntimeError.HttpError("GET /api", 403, "Forbidden"))
          }

          result <- effect.retry(policy).either
          count  <- attemptCount.get
        yield assertTrue(
          result.isLeft,
          count == 1 // Only original attempt, no retries
        )
      },
      test("uses exponential backoff timing") {
        val handler = ErrorHandler.DefaultErrorHandler(
          maxRetries = 3,
          backoffStrategies = List(BackoffStrategy.ExponentialBackoff(2)), // 2s base
          responseFilters = List(
            HttpResponseFilter(ResponseAction.RETRY, httpCodes = List(500))
          )
        )

        for
          service <- ZIO.service[ErrorHandlerService]
          policy = service.buildRetryPolicy(handler)
          attemptCount <- Ref.make(0)

          // Effect that always fails
          effect = attemptCount.updateAndGet(_ + 1).flatMap { _ =>
            ZIO.fail(YamlRuntimeError.HttpError("GET /api", 500, "Error"))
          }

          // Run with TestClock to verify timing
          fiber  <- effect.retry(policy).fork
          _      <- TestClock.adjust(2.seconds) // First retry after 2s (2^0 * 2s)
          _      <- TestClock.adjust(4.seconds) // Second retry after 4s (2^1 * 2s)
          _      <- TestClock.adjust(8.seconds) // Third retry after 8s (2^2 * 2s)
          result <- fiber.join.either
          count  <- attemptCount.get
        yield assertTrue(
          result.isLeft,
          count == 4 // Original + 3 retries
        )
      }, // Need live clock for fiber timing
      test("uses constant backoff timing") {
        val handler = ErrorHandler.DefaultErrorHandler(
          maxRetries = 2,
          backoffStrategies = List(BackoffStrategy.ConstantBackoff(3.0)), // 3s constant
          responseFilters = List(
            HttpResponseFilter(ResponseAction.RETRY, httpCodes = List(503))
          )
        )

        for
          service <- ZIO.service[ErrorHandlerService]
          policy = service.buildRetryPolicy(handler)
          attemptCount <- Ref.make(0)

          effect = attemptCount.updateAndGet(_ + 1).flatMap { _ =>
            ZIO.fail(YamlRuntimeError.HttpError("GET /api", 503, "Service Unavailable"))
          }

          fiber  <- effect.retry(policy).fork
          _      <- TestClock.adjust(3.seconds) // First retry after 3s
          _      <- TestClock.adjust(3.seconds) // Second retry after 3s
          result <- fiber.join.either
          count  <- attemptCount.get
        yield assertTrue(
          result.isLeft,
          count == 3 // Original + 2 retries
        )
      }
    ),
    suite("buildRetryPolicy - CompositeErrorHandler")(
      test("chains multiple handlers") {
        val handler = ErrorHandler.CompositeErrorHandler(
          errorHandlers = List(
            // First handler: retry 429 with 1s backoff
            ErrorHandler.DefaultErrorHandler(
              maxRetries = 2,
              backoffStrategies = List(BackoffStrategy.ExponentialBackoff(1)),
              responseFilters = List(
                HttpResponseFilter(ResponseAction.RETRY, httpCodes = List(429))
              )
            ),
            // Second handler: retry 5xx with 2s backoff
            ErrorHandler.DefaultErrorHandler(
              maxRetries = 3,
              backoffStrategies = List(BackoffStrategy.ExponentialBackoff(2)),
              responseFilters = List(
                HttpResponseFilter(ResponseAction.RETRY, httpCodes = (500 to 599).toList)
              )
            )
          )
        )

        for
          service <- ZIO.service[ErrorHandlerService]
          policy = service.buildRetryPolicy(handler)
          attemptCount <- Ref.make(0)

          // Effect that fails with 500
          effect = attemptCount.updateAndGet(_ + 1).flatMap { count =>
            if count < 3 then ZIO.fail(YamlRuntimeError.HttpError("GET /api", 500, "Server error"))
            else ZIO.succeed("success")
          }

          // Fork and advance clock
          fiber  <- effect.retry(policy).fork
          _      <- TestClock.adjust(2.seconds) // First retry (uses second handler)
          _      <- TestClock.adjust(4.seconds) // Second retry
          result <- fiber.join
          count  <- attemptCount.get
        yield assertTrue(
          result == "success",
          count == 3 // Should use second handler
        )
      }
    ),
    suite("buildRetryPolicy - default behavior")(
      test("uses Airbyte default when no handler specified") {
        // Default: retry 429 and 5xx, exponential backoff factor 5, max 5 retries
        val defaultHandler = ErrorHandler.DefaultErrorHandler(
          maxRetries = 5,
          backoffStrategies = List(BackoffStrategy.ExponentialBackoff(5)),
          responseFilters = List(
            HttpResponseFilter(ResponseAction.RETRY, httpCodes = List(429)),
            HttpResponseFilter(ResponseAction.RETRY, httpCodes = (500 to 599).toList)
          )
        )

        for
          service <- ZIO.service[ErrorHandlerService]
          policy = service.buildRetryPolicy(defaultHandler)
          attemptCount <- Ref.make(0)

          // 429 should be retried
          effect429 = attemptCount.updateAndGet(_ + 1).flatMap { count =>
            if count < 2 then ZIO.fail(YamlRuntimeError.HttpError("GET /api", 429, "Rate limited"))
            else ZIO.succeed("success")
          }

          fiber429  <- effect429.retry(policy).fork
          _         <- TestClock.adjust(5.seconds) // First retry
          result429 <- fiber429.join
          count429  <- attemptCount.get

          // Reset for 5xx test
          _ <- attemptCount.set(0)
          effect5xx = attemptCount.updateAndGet(_ + 1).flatMap { count =>
            if count < 2 then ZIO.fail(YamlRuntimeError.HttpError("GET /api", 503, "Service unavailable"))
            else ZIO.succeed("success")
          }

          fiber5xx  <- effect5xx.retry(policy).fork
          _         <- TestClock.adjust(5.seconds) // First retry
          result5xx <- fiber5xx.join
          count5xx  <- attemptCount.get
        yield assertTrue(
          result429 == "success",
          count429 == 2,
          result5xx == "success",
          count5xx == 2
        )
      }
    ),
    suite("edge cases")(
      test("handles empty response filters") {
        val handler = ErrorHandler.DefaultErrorHandler(
          maxRetries = 3,
          backoffStrategies = List(BackoffStrategy.ExponentialBackoff(1)),
          responseFilters = List.empty // No filters = no retries
        )

        for
          service <- ZIO.service[ErrorHandlerService]
          policy = service.buildRetryPolicy(handler)
          attemptCount <- Ref.make(0)

          effect = attemptCount.updateAndGet(_ + 1).flatMap { _ =>
            ZIO.fail(YamlRuntimeError.HttpError("GET /api", 500, "Error"))
          }

          result <- effect.retry(policy).either
          count  <- attemptCount.get
        yield assertTrue(
          result.isLeft,
          count == 1 // No retries with empty filters
        )
      },
      test("handles empty backoff strategies (uses default)") {
        val handler = ErrorHandler.DefaultErrorHandler(
          maxRetries = 2,
          backoffStrategies = List.empty, // Empty = use default exponential(5)
          responseFilters = List(
            HttpResponseFilter(ResponseAction.RETRY, httpCodes = List(500))
          )
        )

        for
          service <- ZIO.service[ErrorHandlerService]
          policy = service.buildRetryPolicy(handler)
          attemptCount <- Ref.make(0)

          effect = attemptCount.updateAndGet(_ + 1).flatMap { count =>
            if count < 2 then ZIO.fail(YamlRuntimeError.HttpError("GET /api", 500, "Error"))
            else ZIO.succeed("success")
          }

          fiber  <- effect.retry(policy).fork
          _      <- TestClock.adjust(5.seconds) // Default exponential(5) backoff
          result <- fiber.join
        yield assertTrue(result == "success")
      },
      test("handles empty composite error handlers") {
        val handler = ErrorHandler.CompositeErrorHandler(
          errorHandlers = List.empty // No handlers
        )

        for
          service <- ZIO.service[ErrorHandlerService]
          policy = service.buildRetryPolicy(handler)
          attemptCount <- Ref.make(0)

          effect = attemptCount.updateAndGet(_ + 1).flatMap { _ =>
            ZIO.fail(YamlRuntimeError.HttpError("GET /api", 500, "Error"))
          }

          result <- effect.retry(policy).either
          count  <- attemptCount.get
        yield assertTrue(
          result.isLeft,
          count == 1 // No retries with empty handlers
        )
      }
    ),
    suite("header-based backoff - WaitTimeFromHeader")(
      test("extracts Retry-After header and uses delay") {
        val handler = ErrorHandler.DefaultErrorHandler(
          maxRetries = 3,
          backoffStrategies = List(
            BackoffStrategy.WaitTimeFromHeader("Retry-After", None)
          ),
          responseFilters = List(
            HttpResponseFilter(ResponseAction.RETRY, httpCodes = List(429))
          )
        )

        for
          // Create RuntimeContext with Retry-After header set to 5 seconds
          ctx <- ZIO.service[RuntimeContext]
          _   <- ctx.updateHeaders(Map("Retry-After" -> "5"))

          // Create ErrorHandlerService with RuntimeContext
          service <- ZIO.service[ErrorHandlerService]
          policy = service.buildRetryPolicy(handler)

          attemptCount <- Ref.make(0)

          // Effect that fails with 429
          effect = attemptCount.updateAndGet(_ + 1).flatMap { count =>
            if count < 2 then ZIO.fail(YamlRuntimeError.HttpError("GET /api", 429, "Rate limited"))
            else ZIO.succeed("success")
          }

          // Fork and advance clock by the header-specified delay
          fiber  <- effect.retry(policy).fork
          _      <- TestClock.adjust(5.seconds) // Should match Retry-After header
          result <- fiber.join
          count  <- attemptCount.get
        yield assertTrue(
          result == "success",
          count == 2 // Original attempt + 1 retry
        )
      }.provide(RuntimeContext.Stub.layer, ErrorHandlerService.liveWithContext),
      test("falls back to default delay when header missing") {
        val handler = ErrorHandler.DefaultErrorHandler(
          maxRetries = 2,
          backoffStrategies = List(
            BackoffStrategy.WaitTimeFromHeader("Retry-After", None)
          ),
          responseFilters = List(
            HttpResponseFilter(ResponseAction.RETRY, httpCodes = List(429))
          )
        )

        for
          // Create RuntimeContext with NO Retry-After header
          ctx <- ZIO.service[RuntimeContext]

          service <- ZIO.service[ErrorHandlerService]
          policy = service.buildRetryPolicy(handler)

          attemptCount <- Ref.make(0)

          effect = attemptCount.updateAndGet(_ + 1).flatMap { count =>
            if count < 2 then ZIO.fail(YamlRuntimeError.HttpError("GET /api", 429, "Rate limited"))
            else ZIO.succeed("success")
          }

          fiber  <- effect.retry(policy).fork
          _      <- TestClock.adjust(1.second) // Falls back to 1 second default
          result <- fiber.join
          count  <- attemptCount.get
        yield assertTrue(
          result == "success",
          count == 2
        )
      }.provide(RuntimeContext.Stub.layer, ErrorHandlerService.liveWithContext),
      test("extracts value with regex pattern") {
        val handler = ErrorHandler.DefaultErrorHandler(
          maxRetries = 2,
          backoffStrategies = List(
            // Extract number from "wait 120 seconds" format
            BackoffStrategy.WaitTimeFromHeader("X-Custom-Header", Some("""(\d+)"""))
          ),
          responseFilters = List(
            HttpResponseFilter(ResponseAction.RETRY, httpCodes = List(429))
          )
        )

        for
          ctx <- ZIO.service[RuntimeContext]
          _   <- ctx.updateHeaders(Map("X-Custom-Header" -> "wait 3 seconds"))

          service <- ZIO.service[ErrorHandlerService]
          policy = service.buildRetryPolicy(handler)

          attemptCount <- Ref.make(0)

          effect = attemptCount.updateAndGet(_ + 1).flatMap { count =>
            if count < 2 then ZIO.fail(YamlRuntimeError.HttpError("GET /api", 429, "Rate limited"))
            else ZIO.succeed("success")
          }

          fiber  <- effect.retry(policy).fork
          _      <- TestClock.adjust(3.seconds) // Should extract "3" from header
          result <- fiber.join
          count  <- attemptCount.get
        yield assertTrue(
          result == "success",
          count == 2
        )
      }.provide(RuntimeContext.Stub.layer, ErrorHandlerService.liveWithContext)
    ),
    suite("header-based backoff - WaitUntilTimeFromHeader")(
      test("extracts timestamp and calculates wait duration") {
        val handler = ErrorHandler.DefaultErrorHandler(
          maxRetries = 2,
          backoffStrategies = List(
            BackoffStrategy.WaitUntilTimeFromHeader("X-RateLimit-Reset", None, None)
          ),
          responseFilters = List(
            HttpResponseFilter(ResponseAction.RETRY, httpCodes = List(429))
          )
        )

        for
          // Set timestamp to 3 seconds in the future
          now <- Clock.currentTime(java.util.concurrent.TimeUnit.MILLISECONDS)
          futureTimestamp = (now + 3000) / 1000 // Convert to seconds

          ctx <- ZIO.service[RuntimeContext]
          _   <- ctx.updateHeaders(Map("X-RateLimit-Reset" -> futureTimestamp.toString))

          service <- ZIO.service[ErrorHandlerService]
          policy = service.buildRetryPolicy(handler)

          attemptCount <- Ref.make(0)

          effect = attemptCount.updateAndGet(_ + 1).flatMap { count =>
            if count < 2 then ZIO.fail(YamlRuntimeError.HttpError("GET /api", 429, "Rate limited"))
            else ZIO.succeed("success")
          }

          fiber  <- effect.retry(policy).fork
          _      <- TestClock.adjust(3.seconds) // Should wait until timestamp
          result <- fiber.join
          count  <- attemptCount.get
        yield assertTrue(
          result == "success",
          count == 2
        )
      }.provide(RuntimeContext.Stub.layer, ErrorHandlerService.liveWithContext),
      test("respects minWait parameter") {
        val handler = ErrorHandler.DefaultErrorHandler(
          maxRetries = 2,
          backoffStrategies = List(
            // minWait = 5 seconds
            BackoffStrategy.WaitUntilTimeFromHeader("X-RateLimit-Reset", None, Some(5.0))
          ),
          responseFilters = List(
            HttpResponseFilter(ResponseAction.RETRY, httpCodes = List(429))
          )
        )

        for
          // Set timestamp to 1 second in the future (less than minWait)
          now <- Clock.currentTime(java.util.concurrent.TimeUnit.MILLISECONDS)
          futureTimestamp = (now + 1000) / 1000

          ctx <- ZIO.service[RuntimeContext]
          _   <- ctx.updateHeaders(Map("X-RateLimit-Reset" -> futureTimestamp.toString))

          service <- ZIO.service[ErrorHandlerService]
          policy = service.buildRetryPolicy(handler)

          attemptCount <- Ref.make(0)

          effect = attemptCount.updateAndGet(_ + 1).flatMap { count =>
            if count < 2 then ZIO.fail(YamlRuntimeError.HttpError("GET /api", 429, "Rate limited"))
            else ZIO.succeed("success")
          }

          fiber  <- effect.retry(policy).fork
          _      <- TestClock.adjust(5.seconds) // Should use minWait, not calculated 1s
          result <- fiber.join
          count  <- attemptCount.get
        yield assertTrue(
          result == "success",
          count == 2
        )
      }.provide(RuntimeContext.Stub.layer, ErrorHandlerService.liveWithContext)
    )
  ).provide(ErrorHandlerService.live)

end ErrorHandlerServiceSpec
