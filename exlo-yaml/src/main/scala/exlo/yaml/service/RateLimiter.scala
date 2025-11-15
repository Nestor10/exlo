package exlo.yaml.service

import exlo.yaml.spec.CallRatePolicy
import zio.*

/**
 * Service for rate limiting HTTP requests.
 *
 * Prevents exceeding API quotas by tracking request timestamps and enforcing
 * limits proactively (before making requests).
 *
 * Supports:
 *   - Fixed window: N requests per window, resets at boundaries
 *   - Moving window: N requests in any rolling time period (more precise)
 *
 * Uses RuntimeContext for thread-safe request timestamp tracking.
 */
trait RateLimiter:

  /**
   * Check if a request is allowed under the rate limit policy.
   *
   * If not allowed, this method will wait until a request slot becomes
   * available.
   *
   * @param policy
   *   Rate limiting policy to enforce
   * @return
   *   Unit after waiting (if necessary)
   */
  def checkLimit(policy: CallRatePolicy): ZIO[RuntimeContext, Throwable, Unit]

object RateLimiter:

  /**
   * Live implementation using RuntimeContext for timestamp tracking.
   */
  case class Live() extends RateLimiter:

    override def checkLimit(policy: CallRatePolicy): ZIO[RuntimeContext, Throwable, Unit] =
      policy match
        case CallRatePolicy.FixedWindowCallRate(requestsPerWindow, windowSizeSeconds) =>
          checkFixedWindow(requestsPerWindow, windowSizeSeconds)

        case CallRatePolicy.MovingWindowCallRate(requestsPerWindow, windowSizeSeconds) =>
          checkMovingWindow(requestsPerWindow, windowSizeSeconds)

    /**
     * Fixed window rate limiting.
     *
     * Counts requests in the current window. If limit reached, waits until
     * next window boundary.
     */
    private def checkFixedWindow(requestsPerWindow: Int, windowSizeSeconds: Int): ZIO[RuntimeContext, Throwable, Unit] =
      for
        now <- Clock.instant
        windowDuration = Duration.fromSeconds(windowSizeSeconds)

        // Calculate current window start (floor to window boundary)
        windowStartEpochSecond = (now.getEpochSecond / windowSizeSeconds) * windowSizeSeconds
        windowStart            = java.time.Instant.ofEpochSecond(windowStartEpochSecond)

        // Get timestamps in current window
        timestamps <- RuntimeContext.getRequestTimestamps(windowDuration)
        currentWindowCount = timestamps.count(!_.isBefore(windowStart))

        // If limit reached, wait until next window
        _ <-
          if currentWindowCount >= requestsPerWindow then
            val nextWindowStart = java.time.Instant.ofEpochSecond(windowStartEpochSecond + windowSizeSeconds)
            val waitDuration    = java.time.Duration.between(now, nextWindowStart)
            ZIO.logDebug(
              s"Fixed window rate limit reached ($currentWindowCount/$requestsPerWindow). Waiting ${waitDuration.toMillis}ms until next window."
            ) *>
              Clock.sleep(Duration.fromJava(waitDuration))
          else ZIO.unit
      yield ()

    /**
     * Moving window rate limiting.
     *
     * Counts requests in the last N seconds. If limit reached, waits until
     * oldest request falls outside window.
     */
    private def checkMovingWindow(
      requestsPerWindow: Int,
      windowSizeSeconds: Int
    ): ZIO[RuntimeContext, Throwable, Unit] =
      val windowDuration = Duration.fromSeconds(windowSizeSeconds)

      RuntimeContext.getRequestTimestamps(windowDuration).flatMap { timestamps =>
        // If limit reached, wait until oldest request expires
        if timestamps.size >= requestsPerWindow then
          val oldestTimestamp = timestamps.minOption.getOrElse(java.time.Instant.now())
          val windowExpiry    = oldestTimestamp.plusSeconds(windowSizeSeconds)

          Clock.instant.flatMap { now =>
            val waitDuration = java.time.Duration.between(now, windowExpiry)

            if waitDuration.isNegative then
              // Oldest request already expired, no wait needed
              ZIO.unit
            else
              ZIO.logDebug(
                s"Moving window rate limit reached (${timestamps.size}/$requestsPerWindow). Waiting ${waitDuration.toMillis}ms."
              ) *>
                Clock.sleep(Duration.fromJava(waitDuration))
          }
        else ZIO.unit
      }

  object Live:
    val layer: ULayer[RateLimiter] = ZLayer.succeed(Live())

  /**
   * Accessor methods.
   */
  def checkLimit(policy: CallRatePolicy): ZIO[RateLimiter & RuntimeContext, Throwable, Unit] =
    ZIO.serviceWithZIO[RateLimiter](_.checkLimit(policy))
