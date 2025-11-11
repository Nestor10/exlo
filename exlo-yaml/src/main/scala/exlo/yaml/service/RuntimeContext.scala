package exlo.yaml.service

import com.fasterxml.jackson.databind.JsonNode
import exlo.yaml.domain.ConnectorConfig
import exlo.yaml.infra.ConfigLoader
import exlo.yaml.template.TemplateValue
import zio.*

import java.time.Instant

/**
 * Service for managing execution-scoped runtime state.
 *
 * Following Zionomicon patterns:
 *   - Chapter 9: Uses Ref[ExecutionState] for fiber-safe mutable state
 *   - Chapter 17: Service-oriented design with ZLayer dependency injection
 *
 * This service consolidates all runtime state that changes during stream
 * execution:
 *   - Response headers (for backoff strategies, rate limiting)
 *   - Incremental sync cursor
 *   - Pagination variables (page, offset, next_page_token)
 *   - Rate limit information
 *
 * Separates concerns:
 *   - Immutable config (set once at startup)
 *   - Mutable execution state (atomic updates via Ref)
 *
 * Benefits:
 *   - No more context threading through method parameters
 *   - Accessible from any service via ZIO environment
 *   - Enables header-based backoff strategies
 *   - Future-proof for rate limiting
 *   - Testable with mock implementations
 */
trait RuntimeContext:

  // === Immutable Config (Set Once) ===

  /**
   * Get connector configuration.
   *
   * @return
   *   Connector config from EXLO_CONNECTOR_CONFIG
   */
  def getConfig: UIO[ConnectorConfig]

  /**
   * Get initial state from extract parameter.
   *
   * @return
   *   Initial state JSON (for incremental sync)
   */
  def getInitialState: UIO[JsonNode]

  // === Mutable Execution State ===

  /**
   * Update response headers from last HTTP request.
   *
   * Called by HttpClient after every request (success or failure) to capture
   * headers like Retry-After, X-RateLimit-*, etc.
   *
   * @param headers
   *   HTTP response headers
   */
  def updateHeaders(headers: Map[String, String]): UIO[Unit]

  /**
   * Get last response headers.
   *
   * Used by ErrorHandlerService for header-based backoff strategies.
   *
   * @return
   *   Last HTTP response headers
   */
  def getHeaders: UIO[Map[String, String]]

  /**
   * Update incremental sync cursor.
   *
   * Called by YamlConnector as records are processed. Tracks the maximum
   * cursor value seen.
   *
   * @param cursor
   *   Cursor value from record (e.g., "2024-01-15T10:30:00Z", "12345")
   */
  def updateCursor(cursor: String): UIO[Unit]

  /**
   * Get current cursor value.
   *
   * Used for checkpoint serialization.
   *
   * @return
   *   Current cursor (None if fresh start)
   */
  def getCursor: UIO[Option[String]]

  /**
   * Set pagination variable.
   *
   * Called by YamlInterpreter as it iterates through pages. Updates variables
   * like "page", "offset", "next_page_token" that are used in request
   * templates.
   *
   * @param key
   *   Variable name (e.g., "page", "offset")
   * @param value
   *   Variable value (wrapped in TemplateValue)
   */
  def setPaginationVar(key: String, value: TemplateValue): UIO[Unit]

  /**
   * Get all pagination variables.
   *
   * @return
   *   Map of pagination variable names to values
   */
  def getPaginationVars: UIO[Map[String, TemplateValue]]

  /**
   * Update rate limit information from response headers.
   *
   * Extracts X-RateLimit-Remaining and X-RateLimit-Reset headers.
   *
   * @param remaining
   *   Number of requests remaining
   * @param resetAt
   *   Timestamp when rate limit resets
   */
  def updateRateLimitInfo(remaining: Int, resetAt: Instant): UIO[Unit]

  /**
   * Get current rate limit information.
   *
   * @return
   *   Rate limit info (None if not available)
   */
  def getRateLimitInfo: UIO[Option[RuntimeContext.RateLimitInfo]]

  /**
   * Reset pagination variables.
   *
   * Called between pages or when starting new pagination loop.
   */
  def resetPaginationVars: UIO[Unit]

  // === Rate Limiting ===

  /**
   * Record a request timestamp for rate limiting.
   *
   * Called by RequestExecutor after each request. Maintains a rolling window
   * of request timestamps.
   */
  def recordRequest: UIO[Unit]

  /**
   * Get recent request timestamps within a time window.
   *
   * Used for rate limit calculations.
   *
   * @param windowDuration
   *   Time window to look back
   * @return
   *   Timestamps of requests within the window
   */
  def getRequestTimestamps(windowDuration: Duration): UIO[Seq[Instant]]

  // === Convenience Methods ===

  /**
   * Get full template context for rendering Jinja2 templates.
   *
   * Combines:
   *   - config: Connector configuration (immutable)
   *   - state: Initial state + current cursor (mutable)
   *   - Pagination variables (page, offset, etc.) (mutable)
   *
   * This is used by TemplateEngine to render templates like:
   *   - `{{ config.api_key }}`
   *   - `{{ state.cursor }}`
   *   - `{{ page }}`
   *
   * @return
   *   Complete template context
   */
  def getTemplateContext: UIO[Map[String, TemplateValue]]

  /**
   * Get state JSON for checkpointing.
   *
   * Serializes current cursor to JSON format like {"cursor": "value"}.
   *
   * @return
   *   State JSON string
   */
  def getStateJson: UIO[String]

object RuntimeContext:

  /**
   * Mutable execution state.
   *
   * All fields that need to be consistent with each other are in a single
   * case class (Zionomicon Ch9 best practice).
   *
   * @param lastResponseHeaders
   *   Headers from most recent HTTP response
   * @param cursor
   *   Current cursor value (max seen so far)
   * @param paginationVars
   *   Variables for pagination (page, offset, next_page_token)
   * @param rateLimitInfo
   *   Rate limit state (if available from headers)
   * @param requestTimestamps
   *   Rolling window of recent request timestamps for rate limiting
   */
  case class ExecutionState(
    lastResponseHeaders: Map[String, String] = Map.empty,
    cursor: Option[String] = None,
    paginationVars: Map[String, TemplateValue] = Map.empty,
    rateLimitInfo: Option[RateLimitInfo] = None,
    requestTimestamps: scala.collection.immutable.Queue[Instant] = scala.collection.immutable.Queue.empty
  )

  /**
   * Rate limit information extracted from headers.
   *
   * @param remaining
   *   Number of requests remaining in current window
   * @param resetAt
   *   Timestamp when rate limit resets
   */
  case class RateLimitInfo(
    remaining: Int,
    resetAt: Instant
  )

  /**
   * Live implementation using Ref.
   *
   * @param config
   *   Connector configuration (immutable)
   * @param initialState
   *   Initial state JSON (immutable)
   * @param stateRef
   *   Mutable execution state (atomic updates via Ref)
   */
  case class Live(
    config: ConnectorConfig,
    initialState: JsonNode,
    stateRef: Ref[ExecutionState]
  ) extends RuntimeContext:

    override def getConfig: UIO[ConnectorConfig] =
      ZIO.succeed(config)

    override def getInitialState: UIO[JsonNode] =
      ZIO.succeed(initialState)

    override def updateHeaders(headers: Map[String, String]): UIO[Unit] =
      stateRef.update(_.copy(lastResponseHeaders = headers))

    override def getHeaders: UIO[Map[String, String]] =
      stateRef.get.map(_.lastResponseHeaders)

    override def updateCursor(cursor: String): UIO[Unit] =
      stateRef.update { state =>
        state.cursor match
          case Some(current) if current > cursor =>
            state // Keep current (it's newer)
          case _ =>
            state.copy(cursor = Some(cursor)) // Update to new max
      }

    override def getCursor: UIO[Option[String]] =
      stateRef.get.map(_.cursor)

    override def setPaginationVar(key: String, value: TemplateValue): UIO[Unit] =
      stateRef.update(state => state.copy(paginationVars = state.paginationVars + (key -> value)))

    override def getPaginationVars: UIO[Map[String, TemplateValue]] =
      stateRef.get.map(_.paginationVars)

    override def updateRateLimitInfo(remaining: Int, resetAt: Instant): UIO[Unit] =
      stateRef.update(_.copy(rateLimitInfo = Some(RateLimitInfo(remaining, resetAt))))

    override def getRateLimitInfo: UIO[Option[RateLimitInfo]] =
      stateRef.get.map(_.rateLimitInfo)

    override def resetPaginationVars: UIO[Unit] =
      stateRef.update(_.copy(paginationVars = Map.empty))

    override def recordRequest: UIO[Unit] =
      Clock.instant.flatMap { now =>
        stateRef.update(state => state.copy(requestTimestamps = state.requestTimestamps.enqueue(now)))
      }

    override def getRequestTimestamps(windowDuration: Duration): UIO[Seq[Instant]] =
      Clock.instant.flatMap { now =>
        stateRef.get.flatMap { state =>
          val cutoff        = now.minusMillis(windowDuration.toMillis)
          // Filter timestamps within window and prune old ones
          val (old, recent) = state.requestTimestamps.partition(_.isBefore(cutoff))
          // Update state to remove old timestamps (cleanup)
          if old.nonEmpty then stateRef.update(_.copy(requestTimestamps = recent)).as(recent.toSeq)
          else ZIO.succeed(recent.toSeq)
        }
      }

    override def getTemplateContext: UIO[Map[String, TemplateValue]] =
      for
        state <- stateRef.get

        // Build state object with cursor
        stateObj = state.cursor match
          case Some(cursorValue) =>
            TemplateValue.Obj(
              Map(
                "cursor" -> Some(TemplateValue.Str(cursorValue))
              )
            )
          case None              =>
            TemplateValue.Obj(Map.empty)
      yield Map(
        "config" -> TemplateValue.fromJsonNode(config.node),
        "state"  -> stateObj
      ) ++ state.paginationVars // Add pagination vars at top level

    override def getStateJson: UIO[String] =
      stateRef.get.map { state =>
        state.cursor match
          case Some(cursor) =>
            s"""{"cursor": "$cursor"}"""
          case None         =>
            "{}"
      }

  /**
   * Stub implementation for testing.
   *
   * Allows configuring initial state with test data.
   *
   * @param config
   *   Test connector configuration
   * @param initialState
   *   Test initial state JSON
   * @param stateRef
   *   Mutable execution state (atomic updates via Ref)
   */
  case class Stub(
    config: ConnectorConfig,
    initialState: JsonNode,
    stateRef: Ref[ExecutionState]
  ) extends RuntimeContext:

    override def getConfig: UIO[ConnectorConfig] =
      ZIO.succeed(config)

    override def getInitialState: UIO[JsonNode] =
      ZIO.succeed(initialState)

    override def updateHeaders(headers: Map[String, String]): UIO[Unit] =
      stateRef.update(_.copy(lastResponseHeaders = headers))

    override def getHeaders: UIO[Map[String, String]] =
      stateRef.get.map(_.lastResponseHeaders)

    override def updateCursor(cursor: String): UIO[Unit] =
      stateRef.update { state =>
        state.cursor match
          case Some(current) if current > cursor =>
            state // Keep current (it's newer)
          case _ =>
            state.copy(cursor = Some(cursor)) // Update to new max
      }

    override def getCursor: UIO[Option[String]] =
      stateRef.get.map(_.cursor)

    override def setPaginationVar(key: String, value: TemplateValue): UIO[Unit] =
      stateRef.update(state => state.copy(paginationVars = state.paginationVars + (key -> value)))

    override def getPaginationVars: UIO[Map[String, TemplateValue]] =
      stateRef.get.map(_.paginationVars)

    override def updateRateLimitInfo(remaining: Int, resetAt: Instant): UIO[Unit] =
      stateRef.update(_.copy(rateLimitInfo = Some(RateLimitInfo(remaining, resetAt))))

    override def getRateLimitInfo: UIO[Option[RateLimitInfo]] =
      stateRef.get.map(_.rateLimitInfo)

    override def resetPaginationVars: UIO[Unit] =
      stateRef.update(_.copy(paginationVars = Map.empty))

    override def recordRequest: UIO[Unit] =
      Clock.instant.flatMap { now =>
        stateRef.update(state => state.copy(requestTimestamps = state.requestTimestamps.enqueue(now)))
      }

    override def getRequestTimestamps(windowDuration: Duration): UIO[Seq[Instant]] =
      Clock.instant.flatMap { now =>
        stateRef.get.flatMap { state =>
          val cutoff        = now.minusMillis(windowDuration.toMillis)
          // Filter timestamps within window and prune old ones
          val (old, recent) = state.requestTimestamps.partition(_.isBefore(cutoff))
          // Update state to remove old timestamps (cleanup)
          if old.nonEmpty then stateRef.update(_.copy(requestTimestamps = recent)).as(recent.toSeq)
          else ZIO.succeed(recent.toSeq)
        }
      }

    override def getTemplateContext: UIO[Map[String, TemplateValue]] =
      for
        state <- stateRef.get

        // Build state object with cursor
        stateObj = state.cursor match
          case Some(cursorValue) =>
            TemplateValue.Obj(
              Map(
                "cursor" -> Some(TemplateValue.Str(cursorValue))
              )
            )
          case None              =>
            TemplateValue.Obj(Map.empty)
      yield Map(
        "config" -> TemplateValue.fromJsonNode(config.node),
        "state"  -> stateObj
      ) ++ state.paginationVars // Add pagination vars at top level

    override def getStateJson: UIO[String] =
      stateRef.get.map { state =>
        state.cursor match
          case Some(cursor) =>
            s"""{"cursor": "$cursor"}"""
          case None         =>
            "{}"
      }

  object Stub:

    /**
     * Create test layer with empty config and state.
     *
     * Use this as the default for most tests.
     */
    val layer: ULayer[RuntimeContext] =
      ZLayer {
        for
          emptyConfig <- ZIO.succeed(
            ConnectorConfig(new com.fasterxml.jackson.databind.ObjectMapper().createObjectNode())
          )
          emptyState  <- ZIO.succeed(
            new com.fasterxml.jackson.databind.ObjectMapper().createObjectNode()
          )
          stateRef    <- Ref.make(ExecutionState())
        yield Stub(emptyConfig, emptyState, stateRef)
      }

  object Live:

    /**
     * Create ZLayer for RuntimeContext.
     *
     * Requires ConnectorConfig and initial state string as inputs.
     *
     * @param initialStateStr
     *   Initial state as JSON string (from extract parameter)
     */
    def layer(initialStateStr: String): ZLayer[ConnectorConfig, Throwable, RuntimeContext] =
      ZLayer {
        for
          config <- ZIO.service[ConnectorConfig]

          // Parse initial state JSON
          initialStateJson <- ZIO.attempt {
            if initialStateStr.isEmpty then
              // Empty state
              new com.fasterxml.jackson.databind.ObjectMapper().createObjectNode()
            else
              val mapper = new com.fasterxml.jackson.databind.ObjectMapper()
              mapper.readTree(initialStateStr)
          }

          // Parse cursor from initial state
          initialCursor = Option(initialStateJson.get("cursor")).map(_.asText())

          // Create Ref with initial execution state
          stateRef <- Ref.make(ExecutionState(cursor = initialCursor))
        yield Live(config, initialStateJson, stateRef)
      }

    /**
     * Create RuntimeContext layer by loading config from environment.
     *
     * Config is loaded from EXLO_CONNECTOR_CONFIG env var via ConfigLoader.
     * Initial state starts empty and is set via updateCursor in extract().
     *
     * This is the standard layer for production use.
     */
    val layer: ZLayer[Any, Throwable, RuntimeContext] =
      ZLayer {
        for
          // Load config from environment
          config <- ConfigLoader.load

          // Start with empty state (will be set in extract())
          emptyState <- ZIO.succeed(
            new com.fasterxml.jackson.databind.ObjectMapper().createObjectNode()
          )

          stateRef <- Ref.make(ExecutionState())
        yield Live(config, emptyState, stateRef)
      }

  /**
   * Accessor methods for service.
   */

  def getConfig: URIO[RuntimeContext, ConnectorConfig] =
    ZIO.serviceWithZIO[RuntimeContext](_.getConfig)

  def getInitialState: URIO[RuntimeContext, JsonNode] =
    ZIO.serviceWithZIO[RuntimeContext](_.getInitialState)

  def updateHeaders(headers: Map[String, String]): URIO[RuntimeContext, Unit] =
    ZIO.serviceWithZIO[RuntimeContext](_.updateHeaders(headers))

  def getHeaders: URIO[RuntimeContext, Map[String, String]] =
    ZIO.serviceWithZIO[RuntimeContext](_.getHeaders)

  def updateCursor(cursor: String): URIO[RuntimeContext, Unit] =
    ZIO.serviceWithZIO[RuntimeContext](_.updateCursor(cursor))

  def getCursor: URIO[RuntimeContext, Option[String]] =
    ZIO.serviceWithZIO[RuntimeContext](_.getCursor)

  def setPaginationVar(key: String, value: TemplateValue): URIO[RuntimeContext, Unit] =
    ZIO.serviceWithZIO[RuntimeContext](_.setPaginationVar(key, value))

  def getPaginationVars: URIO[RuntimeContext, Map[String, TemplateValue]] =
    ZIO.serviceWithZIO[RuntimeContext](_.getPaginationVars)

  def updateRateLimitInfo(remaining: Int, resetAt: Instant): URIO[RuntimeContext, Unit] =
    ZIO.serviceWithZIO[RuntimeContext](_.updateRateLimitInfo(remaining, resetAt))

  def getRateLimitInfo: URIO[RuntimeContext, Option[RateLimitInfo]] =
    ZIO.serviceWithZIO[RuntimeContext](_.getRateLimitInfo)

  def resetPaginationVars: URIO[RuntimeContext, Unit] =
    ZIO.serviceWithZIO[RuntimeContext](_.resetPaginationVars)

  def recordRequest: URIO[RuntimeContext, Unit] =
    ZIO.serviceWithZIO[RuntimeContext](_.recordRequest)

  def getRequestTimestamps(windowDuration: Duration): URIO[RuntimeContext, Seq[Instant]] =
    ZIO.serviceWithZIO[RuntimeContext](_.getRequestTimestamps(windowDuration))

  def getTemplateContext: URIO[RuntimeContext, Map[String, TemplateValue]] =
    ZIO.serviceWithZIO[RuntimeContext](_.getTemplateContext)

  def getStateJson: URIO[RuntimeContext, String] =
    ZIO.serviceWithZIO[RuntimeContext](_.getStateJson)
