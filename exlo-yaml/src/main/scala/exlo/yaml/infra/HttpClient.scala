package exlo.yaml.infra

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import exlo.yaml.domain.YamlRuntimeError
import exlo.yaml.service.RuntimeContext
import exlo.yaml.spec.HttpMethod
import zio.*
import zio.http.*

/**
 * HTTP client infrastructure.
 *
 * Pure infrastructure layer - wraps zio-http Client with no application-level
 * concerns. Handles GET/POST methods, headers, query parameters, and JSON
 * response parsing.
 *
 * Retry logic, rate limiting, and other business policies belong at the
 * application layer (YamlInterpreter) where they can be composed and
 * configured.
 *
 * Follows Zionomicon Chapter 14 patterns for resource management via Scope.
 */
trait HttpClient:

  /**
   * Execute HTTP request and return JSON response.
   *
   * Returns typed errors for different failure modes:
   *   - YamlRuntimeError.HttpError for HTTP failures (4xx, 5xx)
   *   - YamlRuntimeError.ParseError for JSON parsing failures
   *   - Network errors (connection failures, timeouts)
   *
   * Side effect: Stores response headers in RuntimeContext (if available) for
   * backoff strategies and rate limiting.
   *
   * Application layer should add retry logic as needed using .retry(schedule).
   *
   * @param url
   *   Target URL
   * @param method
   *   HTTP method (GET/POST)
   * @param headers
   *   Request headers
   * @param queryParams
   *   Query parameters
   * @param body
   *   Request body (for POST)
   * @return
   *   Parsed JSON response as Jackson JsonNode
   */
  def execute(
    url: String,
    method: HttpMethod,
    headers: Map[String, String],
    queryParams: Map[String, String],
    body: Option[String]
  ): IO[Throwable, JsonNode]

object HttpClient:

  /**
   * Accessor for execute.
   *
   * Use: `HttpClient.execute(url, method, headers, queryParams, body)`
   */
  def execute(
    url: String,
    method: HttpMethod,
    headers: Map[String, String],
    queryParams: Map[String, String],
    body: Option[String]
  ): ZIO[HttpClient, Throwable, JsonNode] =
    ZIO.serviceWithZIO[HttpClient](
      _.execute(url, method, headers, queryParams, body)
    )

  /** Live implementation using zio-http Client. */
  case class Live(client: Client, runtimeContext: Option[RuntimeContext]) extends HttpClient:

    private val jsonMapper = new ObjectMapper()

    override def execute(
      url: String,
      method: HttpMethod,
      headers: Map[String, String],
      queryParams: Map[String, String],
      body: Option[String]
    ): IO[Throwable, JsonNode] =
      ZIO.scoped {
        for
          // Build URL with query parameters
          fullUrl <- ZIO
            .fromEither(
              URL.decode(buildUrlWithParams(url, queryParams))
            )
            .mapError(e =>
              YamlRuntimeError.HttpError(
                url,
                0,
                s"Invalid URL: ${e.getMessage}"
              )
            )

          // Build request
          request         = method match
            case HttpMethod.GET =>
              Request.get(fullUrl).addHeaders(buildHeaders(headers))

            case HttpMethod.POST =>
              Request
                .post(fullUrl, Body.fromString(body.getOrElse("")))
                .addHeaders(buildHeaders(headers))

          // Execute request
          response <- client
            .request(request)
            .mapError(e =>
              YamlRuntimeError.HttpError(
                url,
                0,
                s"HTTP request failed: ${e.getMessage}"
              )
            )

          // Extract headers for RuntimeContext
          responseHeaders = response.headers.toList.map(header => header.headerName -> header.renderedValue).toMap

          // Store headers in RuntimeContext (if available)
          _        <- runtimeContext match
            case Some(ctx) => ctx.updateHeaders(responseHeaders)
            case None      => ZIO.unit

          // Check status code
          _        <- ZIO.when(!response.status.isSuccess) {
            response.body.asString.flatMap { bodyText =>
              ZIO.fail(
                YamlRuntimeError.HttpError(
                  url,
                  response.status.code,
                  bodyText,
                  responseHeaders
                )
              )
            }
          }

          // Parse JSON response using Jackson
          bodyText <- response.body.asString
          jsonNode <- ZIO
            .attempt(jsonMapper.readTree(bodyText))
            .mapError(e =>
              YamlRuntimeError.ParseError(
                url,
                s"Failed to parse JSON: ${e.getMessage}"
              )
            )
        yield jsonNode
      }

    private def buildUrlWithParams(
      url: String,
      params: Map[String, String]
    ): String =
      if params.isEmpty then url
      else
        val queryString = params.map { case (k, v) => s"$k=$v" }.mkString("&")
        s"$url?$queryString"

    private def buildHeaders(headers: Map[String, String]): Headers =
      Headers(headers.map { case (k, v) => Header.Custom(k, v) }.toList*)

  object Live:

    /**
     * ZLayer for HttpClient without RuntimeContext.
     *
     * Legacy layer - headers won't be stored for backoff strategies.
     */
    val layer: ZLayer[Client, Nothing, HttpClient] =
      ZLayer {
        for client <- ZIO.service[Client]
        yield Live(client, None)
      }

    /**
     * ZLayer for HttpClient with RuntimeContext.
     *
     * Headers will be stored in RuntimeContext for backoff strategies.
     */
    val layerWithContext: ZLayer[Client & RuntimeContext, Nothing, HttpClient] =
      ZLayer {
        for
          client <- ZIO.service[Client]
          ctx    <- ZIO.service[RuntimeContext]
        yield Live(client, Some(ctx))
      }
