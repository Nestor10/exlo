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
   * Context (config, state, pagination vars) is read from RuntimeContext.
   *
   * @param requester The request specification from YAML
   * @return The response body as JsonNode
   */
  def execute(
    requester: Requester
  ): ZIO[RuntimeContext, Throwable, JsonNode]

object RequestExecutor:

  case class Live(
    httpClient: HttpClient,
    templateEngine: TemplateEngine,
    authenticator: Authenticator,
    errorHandlerService: ErrorHandlerService,
    rateLimiter: RateLimiter
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
      requester: Requester
    ): ZIO[RuntimeContext, Throwable, JsonNode] =
      for
        // Check rate limit before executing request
        _               <- requester.callRatePolicy match
          case Some(policy) => rateLimiter.checkLimit(policy)
          case None         => ZIO.unit

        // Render templates (reads context from RuntimeContext)
        url             <- templateEngine.render(requester.url)
        renderedHeaders <- ZIO
          .foreach(requester.headers.toList) {
            case (key, value) =>
              templateEngine.render(value).map(key -> _)
          }
          .map(_.toMap)
        renderedParams  <- ZIO
          .foreach(requester.params.toList) {
            case (key, value) =>
              templateEngine.render(value).map(key -> _)
          }
          .map(_.toMap)

        // Authenticate
        headersWithAuth <- authenticator.authenticate(requester.auth, renderedHeaders)

        // Render body if present
        renderedBody <- requester.body match
          case Some(bodyTemplate) =>
            templateEngine.render(bodyTemplate).map(Some(_))
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

        // Record request timestamp after successful execution
        _        <- RuntimeContext.recordRequest
      yield response

  val live: URLayer[HttpClient & TemplateEngine & Authenticator & ErrorHandlerService & RateLimiter, RequestExecutor] =
    ZLayer.fromFunction(Live.apply)
