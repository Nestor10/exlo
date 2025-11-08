package exlo.yaml.service

import exlo.yaml.domain.YamlRuntimeError
import exlo.yaml.spec.HttpMethod
import io.circe.Json
import io.circe.parser.*
import zio.*
import zio.http.*

/**
 * Service for executing HTTP requests.
 *
 * Handles GET/POST methods, headers, query parameters, and JSON response
 * parsing. Manages zio-http Client resources following Zionomicon Chapter 14
 * patterns.
 */
trait HttpClient:

  /**
   * Execute HTTP request and return JSON response.
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
   *   Parsed JSON response
   */
  def execute(
    url: String,
    method: HttpMethod,
    headers: Map[String, String],
    queryParams: Map[String, String],
    body: Option[String]
  ): IO[Throwable, Json]

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
  ): ZIO[HttpClient, Throwable, Json] =
    ZIO.serviceWithZIO[HttpClient](
      _.execute(url, method, headers, queryParams, body)
    )

  /** Live implementation using zio-http Client. */
  case class Live(client: Client) extends HttpClient:

    override def execute(
      url: String,
      method: HttpMethod,
      headers: Map[String, String],
      queryParams: Map[String, String],
      body: Option[String]
    ): IO[Throwable, Json] =
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
          request = method match
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

          // Check status code
          _        <- ZIO.when(!response.status.isSuccess) {
            response.body.asString.flatMap { bodyText =>
              ZIO.fail(
                YamlRuntimeError.HttpError(
                  url,
                  response.status.code,
                  bodyText
                )
              )
            }
          }

          // Parse JSON response
          bodyText <- response.body.asString
          json     <- ZIO
            .fromEither(parse(bodyText))
            .mapError(e =>
              YamlRuntimeError.ParseError(
                url,
                s"Failed to parse JSON: ${e.getMessage}"
              )
            )
        yield json
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
     * ZLayer for HttpClient.
     *
     * Depends on zio.http.Client. Use Client.default or custom client
     * configuration.
     */
    val layer: ZLayer[Client, Nothing, HttpClient] =
      ZLayer.fromFunction(Live.apply)
