package exlo.yaml.service

import com.fasterxml.jackson.databind.JsonNode
import exlo.yaml.infra.HttpClient
import exlo.yaml.spec.Requester
import exlo.yaml.template.TemplateValue
import zio.*

/**
 * Service that executes HTTP requests with all cross-cutting concerns:
 * - Template rendering (URL, headers, params, body)
 * - Authentication
 * - Error handling and retry policies
 * - Rate limiting (future)
 *
 * This consolidates request execution logic that was previously duplicated
 * across all pagination strategies in YamlInterpreter.
 */
trait RequestExecutor:

  /**
   * Execute an HTTP request with all processing steps.
   *
   * @param requester The request specification from YAML
   * @param context Template context for rendering dynamic values
   * @return The response body as JsonNode
   */
  def execute(
    requester: Requester,
    context: Map[String, TemplateValue]
  ): IO[Throwable, JsonNode]

object RequestExecutor:

  case class Live(
    httpClient: HttpClient,
    templateEngine: TemplateEngine,
    authenticator: Authenticator,
    errorHandlerService: ErrorHandlerService
  ) extends RequestExecutor:

    import exlo.yaml.spec.{ErrorHandler, BackoffStrategy, ResponseAction, HttpResponseFilter}

    /**
     * Default error handler matching Airbyte behavior:
     * - Retry on 429 (rate limit)
     * - Retry on 5xx (server errors)
     * - Max 5 retries
     * - Exponential backoff with factor 5
     */
    private val defaultErrorHandler = ErrorHandler.DefaultErrorHandler(
      maxRetries = 5,
      backoffStrategies = List(BackoffStrategy.ExponentialBackoff(5)),
      responseFilters = List(
        HttpResponseFilter(ResponseAction.RETRY, httpCodes = List(429)),
        HttpResponseFilter(ResponseAction.RETRY, httpCodes = (500 to 599).toList)
      )
    )

    def execute(
      requester: Requester,
      context: Map[String, TemplateValue]
    ): IO[Throwable, JsonNode] =
      for
        // Render templates
        url             <- templateEngine.render(requester.url, context)
        renderedHeaders <- ZIO
          .foreach(requester.headers.toList) {
            case (key, value) =>
              templateEngine.render(value, context).map(key -> _)
          }
          .map(_.toMap)
        renderedParams  <- ZIO
          .foreach(requester.params.toList) {
            case (key, value) =>
              templateEngine.render(value, context).map(key -> _)
          }
          .map(_.toMap)

        // Authenticate
        headersWithAuth <- authenticator.authenticate(requester.auth, renderedHeaders)

        // Render body if present
        renderedBody <- requester.body match
          case Some(bodyTemplate) =>
            templateEngine.render(bodyTemplate, context).map(Some(_))
          case None               =>
            ZIO.succeed(None)

        // Build retry policy
        errorHandler = requester.errorHandler.getOrElse(defaultErrorHandler)
        retryPolicy = errorHandlerService.buildRetryPolicy(errorHandler)

        // Execute with retry
        response <- httpClient
          .execute(
            url = url,
            method = requester.method,
            headers = headersWithAuth,
            queryParams = renderedParams,
            body = renderedBody
          )
          .retry(retryPolicy)
          .tapError(error => ZIO.logError(s"Request failed after retries: $error"))
      yield response

  val live: URLayer[HttpClient & TemplateEngine & Authenticator & ErrorHandlerService, RequestExecutor] =
    ZLayer.fromFunction(Live.apply)
