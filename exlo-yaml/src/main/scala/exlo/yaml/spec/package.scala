package exlo.yaml

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.annotation.JsonTypeInfo.As
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id

/**
 * Domain models for Airbyte-style YAML connector specifications.
 *
 * These are pure data types (ADTs) that represent the declarative connector spec.
 * Following FP principles: immutable, no behavior, just data.
 *
 * The interpreter (in `exlo.yaml.interpreter`) transforms these ADTs into ZIO effects.
 */
package object spec:

  import com.fasterxml.jackson.annotation.{JsonProperty, JsonSubTypes, JsonTypeInfo}
  import com.fasterxml.jackson.databind.annotation.JsonDeserialize

  /**
   * HTTP method for API requests.
   */
  enum HttpMethod:
    case GET, POST

  /**
   * Authentication configuration for API requests.
   *
   * Sealed trait ensures exhaustive pattern matching at compile time.
   */
  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
  @JsonSubTypes(
    Array(
      new JsonSubTypes.Type(value = classOf[Auth.NoAuth.type], name = "NoAuth"),
      new JsonSubTypes.Type(value = classOf[Auth.ApiKey], name = "ApiKey"),
      new JsonSubTypes.Type(value = classOf[Auth.ApiKeyAuthenticator], name = "ApiKeyAuthenticator"),
      new JsonSubTypes.Type(value = classOf[Auth.Bearer], name = "Bearer"),
      new JsonSubTypes.Type(value = classOf[Auth.BearerAuthenticator], name = "BearerAuthenticator"),
      new JsonSubTypes.Type(value = classOf[Auth.OAuth], name = "OAuth"),
      new JsonSubTypes.Type(value = classOf[Auth.SelectiveAuth], name = "SelectiveAuthenticator")
    )
  )
  sealed trait Auth

  object Auth:
    /**
     * No authentication required.
     */
    case object NoAuth extends Auth

    /**
     * API key authentication via header.
     *
     * @param header
     *   Header name (e.g., "X-API-Key")
     * @param token
     *   API token value (supports Jinja templates: "{{ config.api_key }}")
     */
    case class ApiKey(header: String, token: String) extends Auth

    /**
     * Airbyte-compatible ApiKeyAuthenticator with inject_into configuration.
     *
     * Maps to ApiKey internally, extracting header name from inject_into.
     *
     * @param apiToken
     *   API token value (supports Jinja templates: "{{ config['api_key'] }}")
     * @param injectInto
     *   Configuration for where to inject the API key
     */
    case class ApiKeyAuthenticator(
      @JsonProperty("api_token") apiToken: String,
      @JsonProperty("inject_into") injectInto: InjectInto
    ) extends Auth

    /**
     * Configuration for where to inject authentication credentials.
     *
     * Used by ApiKeyAuthenticator to specify header name and location.
     *
     * @param fieldName
     *   Header or query parameter name (e.g., "X-Api-Key")
     * @param injectInto
     *   Location to inject: "header" or "request_parameter"
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    case class InjectInto(
      @JsonProperty("field_name") fieldName: String,
      @JsonProperty("inject_into") injectInto: String
    )

    /**
     * Bearer token authentication.
     *
     * @param token
     *   Bearer token value (supports Jinja templates: "{{ config.token }}")
     */
    case class Bearer(token: String) extends Auth

    /**
     * Airbyte-compatible BearerAuthenticator.
     *
     * Maps to Bearer internally, but uses Airbyte's field name.
     *
     * @param apiToken
     *   Bearer token value (supports Jinja templates: "{{ config.credentials.api_token }}")
     */
    case class BearerAuthenticator(@JsonProperty("api_token") apiToken: String) extends Auth

    /**
     * OAuth 2.0 Client Credentials flow.
     *
     * Automatically obtains and refreshes access tokens.
     *
     * @param tokenUrl
     *   OAuth token endpoint URL
     * @param clientId
     *   OAuth client ID (supports Jinja templates: "{{ config.client_id }}")
     * @param clientSecret
     *   OAuth client secret (supports Jinja templates: "{{ config.client_secret }}")
     * @param scopes
     *   Optional space-separated scopes (e.g., "read write")
     */
    case class OAuth(
      tokenUrl: String,
      clientId: String,
      clientSecret: String,
      scopes: Option[String] = None
    ) extends Auth

    /**
     * Selective authentication based on runtime config value.
     *
     * Reads a config path and uses it to select which authenticator to use.
     *
     * Example:
     * ```yaml
     * type: SelectiveAuthenticator
     * authenticator_selection_path: ["credentials", "option_title"]
     * authenticators:
     *   "API Token Credentials": "#/definitions/api_token_auth"
     *   "OAuth2.0": "#/definitions/oauth_auth"
     * ```
     *
     * If config.credentials.option_title == "API Token Credentials",
     * uses the api_token_auth authenticator.
     *
     * @param selectionPath
     *   JSON path to read from config (e.g., ["credentials", "option_title"])
     * @param authenticators
     *   Map from selector value to authenticator definition
     */
    case class SelectiveAuth(
      @JsonProperty("authenticator_selection_path") selectionPath: List[String],
      authenticators: Map[String, Auth]
    ) extends Auth

  /**
   * Response action for error handling.
   *
   * Determines how to handle HTTP responses based on status codes or error messages.
   */
  enum ResponseAction:
    case SUCCESS, FAIL, IGNORE, RETRY

  /**
   * HTTP response filter for error handling.
   *
   * Filters responses based on HTTP status codes, error messages, or custom predicates.
   *
   * @param action
   *   Action to take when filter matches
   * @param httpCodes
   *   List of HTTP status codes to match (e.g., [403, 404])
   * @param errorMessageContains
   *   Optional substring to match in error message
   * @param predicate
   *   Optional Jinja template boolean expression (e.g., "{{ 'code' in response }}")
   */
  @JsonIgnoreProperties(ignoreUnknown = true)
  case class HttpResponseFilter(
    action: ResponseAction,
    @JsonProperty("http_codes") httpCodes: List[Int] = List.empty,
    @JsonProperty("error_message_contains") errorMessageContains: Option[String] = None,
    predicate: Option[String] = None
  )

  /**
   * Backoff strategy for retries.
   *
   * Sealed trait for compile-time exhaustiveness checking.
   */
  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
  @JsonSubTypes(
    Array(
      new JsonSubTypes.Type(value = classOf[BackoffStrategy.ExponentialBackoff], name = "ExponentialBackoffStrategy"),
      new JsonSubTypes.Type(value = classOf[BackoffStrategy.ConstantBackoff], name = "ConstantBackoffStrategy"),
      new JsonSubTypes.Type(value = classOf[BackoffStrategy.WaitTimeFromHeader], name = "WaitTimeFromHeader"),
      new JsonSubTypes.Type(value = classOf[BackoffStrategy.WaitUntilTimeFromHeader], name = "WaitUntilTimeFromHeader")
    )
  )
  sealed trait BackoffStrategy

  object BackoffStrategy:
    /**
     * Exponential backoff with configurable factor.
     *
     * Wait time = factor * (2 ^ attempt_number) seconds
     *
     * @param factor
     *   Multiplier for exponential backoff (default 5)
     */
    case class ExponentialBackoff(factor: Int = 5) extends BackoffStrategy

    /**
     * Constant backoff with fixed wait time.
     *
     * @param backoffTimeInSeconds
     *   Fixed wait time between retries
     */
    case class ConstantBackoff(@JsonProperty("backoff_time_in_seconds") backoffTimeInSeconds: Double)
        extends BackoffStrategy

    /**
     * Extract wait time from response header.
     *
     * @param header
     *   Header name containing wait time in seconds
     * @param regex
     *   Optional regex to extract numeric value from header
     */
    case class WaitTimeFromHeader(header: String, regex: Option[String] = None) extends BackoffStrategy

    /**
     * Extract wait until timestamp from response header.
     *
     * @param header
     *   Header name containing timestamp
     * @param regex
     *   Optional regex to extract timestamp from header
     * @param minWait
     *   Minimum wait time in seconds
     */
    case class WaitUntilTimeFromHeader(
      header: String,
      regex: Option[String] = None,
      @JsonProperty("min_wait") minWait: Option[Double] = None
    ) extends BackoffStrategy

  /**
   * Error handler configuration.
   *
   * Sealed trait for compile-time exhaustiveness checking.
   */
  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
  @JsonSubTypes(
    Array(
      new JsonSubTypes.Type(value = classOf[ErrorHandler.DefaultErrorHandler], name = "DefaultErrorHandler"),
      new JsonSubTypes.Type(value = classOf[ErrorHandler.CompositeErrorHandler], name = "CompositeErrorHandler")
    )
  )
  sealed trait ErrorHandler

  object ErrorHandler:

    /**
     * Default error handler with configurable retries and backoff.
     *
     * @param maxRetries
     *   Maximum number of retry attempts (default 5)
     * @param backoffStrategies
     *   List of backoff strategies to try in order (fallback pattern)
     * @param responseFilters
     *   List of filters to determine response handling
     */
    case class DefaultErrorHandler(
      @JsonProperty("max_retries") maxRetries: Int = 5,
      @JsonProperty("backoff_strategies") backoffStrategies: List[BackoffStrategy] = List.empty,
      @JsonProperty("response_filters") responseFilters: List[HttpResponseFilter] = List.empty
    ) extends ErrorHandler

    /**
     * Composite error handler for different error types.
     *
     * Sequentially iterates through handlers, enabling different retry mechanisms for different errors.
     *
     * @param errorHandlers
     *   List of error handlers to try in order
     */
    case class CompositeErrorHandler(@JsonProperty("error_handlers") errorHandlers: List[DefaultErrorHandler])
        extends ErrorHandler

  /**
   * Rate limiting policy for API calls.
   *
   * Prevents exceeding API quotas by proactively throttling requests.
   */
  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
  @JsonSubTypes(
    Array(
      new JsonSubTypes.Type(value = classOf[CallRatePolicy.FixedWindowCallRate], name = "FixedWindowCallRate"),
      new JsonSubTypes.Type(value = classOf[CallRatePolicy.MovingWindowCallRate], name = "MovingWindowCallRate")
    )
  )
  sealed trait CallRatePolicy

  object CallRatePolicy:

    /**
     * Fixed window rate limiting.
     *
     * Allows N requests per window, resets at window boundaries.
     *
     * Example: 10 requests per 60 seconds, resets at 0:00, 1:00, 2:00, etc.
     *
     * @param requestsPerWindow
     *   Maximum requests allowed in window
     * @param windowSizeSeconds
     *   Window duration in seconds
     */
    case class FixedWindowCallRate(
      @JsonProperty("requests_per_window") requestsPerWindow: Int,
      @JsonProperty("window_size_seconds") windowSizeSeconds: Int
    ) extends CallRatePolicy

    /**
     * Moving/rolling window rate limiting.
     *
     * Allows N requests in any rolling time window (more precise).
     *
     * Example: 10 requests per any 60-second period.
     *
     * @param requestsPerWindow
     *   Maximum requests allowed in window
     * @param windowSizeSeconds
     *   Window duration in seconds
     */
    case class MovingWindowCallRate(
      @JsonProperty("requests_per_window") requestsPerWindow: Int,
      @JsonProperty("window_size_seconds") windowSizeSeconds: Int
    ) extends CallRatePolicy

  /**
   * HTTP request specification.
   *
   * @param url
   *   Full URL (supports Jinja templates: "https://{{ config.shop }}.example.com/api")
   * @param method
   *   HTTP method (GET or POST)
   * @param headers
   *   Additional headers (values support Jinja templates)
   * @param params
   *   Query parameters (values support Jinja templates)
   * @param auth
   *   Authentication configuration
   * @param body
   *   Request body for POST/PUT requests (supports Jinja templates for JSON)
   * @param errorHandler
   *   Optional error handler configuration (default: retry 5xx and 429 with exponential backoff)
   * @param callRatePolicy
   *   Optional rate limiting policy to prevent exceeding API quotas
   */
  case class Requester(
    url: String,
    method: HttpMethod = HttpMethod.GET,
    headers: Map[String, String] = Map.empty,
    params: Map[String, String] = Map.empty,
    auth: Auth = Auth.NoAuth,
    body: Option[String] = None,
    @JsonProperty("error_handler") errorHandler: Option[ErrorHandler] = None,
    @JsonProperty("call_rate_policy") callRatePolicy: Option[CallRatePolicy] = None
  )

  /**
   * Pagination strategy for multi-page API responses.
   *
   * Sealed trait for compile-time exhaustiveness checking.
   */
  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
  @JsonSubTypes(
    Array(
      new JsonSubTypes.Type(value = classOf[PaginationStrategy.NoPagination.type], name = "NoPagination"),
      new JsonSubTypes.Type(value = classOf[PaginationStrategy.PageIncrement], name = "PageIncrement"),
      new JsonSubTypes.Type(value = classOf[PaginationStrategy.OffsetIncrement], name = "OffsetIncrement"),
      new JsonSubTypes.Type(value = classOf[PaginationStrategy.CursorPagination], name = "CursorPagination")
    )
  )
  sealed trait PaginationStrategy

  object PaginationStrategy:
    /**
     * No pagination (single page response).
     */
    case object NoPagination extends PaginationStrategy

    /**
     * Page number based pagination.
     *
     * Example: /api/users?page=1, /api/users?page=2
     *
     * @param pageSize
     *   Number of records per page
     * @param startFrom
     *   Initial page number (usually 0 or 1)
     */
    case class PageIncrement(pageSize: Int, startFrom: Int = 0) extends PaginationStrategy

    /**
     * Offset-based pagination.
     *
     * Example: /api/users?offset=0&limit=50, /api/users?offset=50&limit=50
     *
     * @param pageSize
     *   Number of records per page (the "limit")
     */
    case class OffsetIncrement(pageSize: Int) extends PaginationStrategy

    /**
     * Cursor-based pagination using a value from the response.
     *
     * Example: response contains {"next_page": "cursor_abc"} Most flexible and recommended for
     * Airbyte-style connectors.
     *
     * @param cursorValue
     *   Jinja template to extract cursor from response (e.g., "{{ response.next_page }}")
     * @param stopCondition
     *   Jinja template that evaluates to boolean (e.g., "{{ not response.next_page }}")
     */
    case class CursorPagination(cursorValue: String, stopCondition: String) extends PaginationStrategy

  /**
   * Record extraction configuration.
   *
   * Extracts records from JSON responses using JSONPath-style selectors.
   */
  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
  @JsonSubTypes(
    Array(
      new JsonSubTypes.Type(value = classOf[Extractor.DPath], name = "DpathExtractor")
    )
  )
  sealed trait Extractor

  object Extractor:
    /**
     * JSONPath-based extractor (Airbyte compatible).
     *
     * Example: ["data", "users", "*"] extracts all users from {"data": {"users": [...]}}
     *
     * @param field_path
     *   Path segments. Use "*" for array wildcard.
     */
    case class DPath(@JsonProperty("field_path") field_path: List[String]) extends Extractor

  /**
   * Record selector: extraction + optional filtering.
   *
   * @param extractor
   *   How to extract records from the response
   * @param filter
   *   Optional Jinja condition to filter records (e.g., "{{ record.age > 18 }}")
   */
  case class RecordSelector(
    extractor: Extractor,
    filter: Option[String] = None
  )

  /**
   * Complete stream specification.
   *
   * Defines how to fetch and process data from an API endpoint.
   *
   * @param name
   *   Stream name (e.g., "orders", "users")
   * @param requester
   *   HTTP request configuration
   * @param recordSelector
   *   How to extract records from responses
   * @param paginator
   *   Pagination strategy (optional, defaults to NoPagination)
   * @param cursorField
   *   Optional field name for incremental sync (e.g., "updated_at", "id").
   *   When set, EXLO will:
   *   1. Extract this field's value from each record
   *   2. Track the maximum value seen
   *   3. Save it in state as { "cursor": <max_value> }
   *   4. Make it available in templates as {{ state.cursor }}
   *
   *   Example: cursorField = "updated_at" for timestamp-based incremental sync
   *   Then URL can be: "/api/users?since={{ state.cursor | default('2020-01-01') }}"
   */
  case class StreamSpec(
    name: String,
    requester: Requester,
    recordSelector: RecordSelector,
    paginator: PaginationStrategy = PaginationStrategy.NoPagination,
    cursorField: Option[String] = None
  )

  /**
   * Top-level connector specification.
   *
   * Can contain multiple streams (Phase 2 feature).
   *
   * @param version
   *   Connector version string (e.g., "1.0.0")
   * @param streams
   *   List of stream configurations
   * @param definitions
   *   Optional definitions section for $ref resolution (ignored after parsing)
   */
  @JsonIgnoreProperties(ignoreUnknown = true)
  case class ConnectorSpec(
    @JsonProperty("version") version: String = "0.1.0",
    streams: List[StreamSpec]
  )
