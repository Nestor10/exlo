package exlo.http

import exlo.domain.ExloError
import zio.*
import zio.http.{Client, Request, ZClient}
import zio.json.*
import zio.json.ast.Json

import java.io.IOException
import java.util.concurrent.TimeoutException

/**
 * Execute an HTTP request and return a parsed [[HttpResponse]].
 *
 * Defined as a service trait so the framework's HTTP layer can be mocked
 * cleanly in tests (provide a fake `HttpExec` layer) without spinning up a
 * real `zio.http.Server`. Production wiring is `HttpExec.live`, which uses
 * `zio.http.Client` under the hood.
 *
 * First-pass live impl: default retry on transient network failures
 * (IOException, TimeoutException) — three retries, 1s/2s/4s with jitter.
 * No auth, no status-based retry, no rate-limit. Port the rest from the
 * deleted code as needed.
 */
trait HttpExec:
  def run(req: Request): IO[ExloError, HttpResponse]

object HttpExec:

  /** Service accessor — call from connector code. */
  def run(req: Request): ZIO[HttpExec, ExloError, HttpResponse] =
    ZIO.serviceWithZIO[HttpExec](_.run(req))

  /** Construct a GET request from a String URL. Throws on a malformed URL
   *  (treated as a programmer error / defect). Pure helper — does not
   *  require the [[HttpExec]] service. */
  def get(url: String): Request =
    Request.get(
      zio.http.URL
        .decode(url)
        .getOrElse(throw new IllegalArgumentException(s"invalid URL: $url"))
    )

  /**
   * Decorator: stamp `Authorization: Bearer <token>` onto every request,
   * then delegate to the underlying [[HttpExec]]. Use as a drop-in
   * replacement for [[live]] in the layer set when the API takes a
   * static token (e.g., a personal access token).
   */
  def bearer(token: String): ZLayer[Client, Nothing, HttpExec] =
    ZLayer.fromZIO(
      ZIO.serviceWith[Client] { client =>
        val inner = liveImpl(client)
        new HttpExec:
          def run(req: Request): IO[ExloError, HttpResponse] =
            inner.run(req.addHeader("Authorization", s"Bearer $token"))
      }
    )

  /**
   * Decorator: retry the entire request when the response status matches
   * `pred`. Retries follow `schedule` (default: 10 attempts, exponential
   * backoff with factor 3, jittered).
   *
   * Composes via `>>>` over a producing layer:
   *
   * {{{
   *   override val httpExec = HttpExec.oauth(flow) >>> HttpExec.retryOnStatus(_ == 429)
   * }}}
   *
   * The retry happens at this decorator's level, so the inner HttpExec
   * (and its OAuth/Bearer header injection) runs fresh on each attempt
   * — the token is re-fetched if it expired between attempts.
   */
  def retryOnStatus(
      pred: Int => Boolean,
      schedule: Schedule[Any, Any, Any] = defaultStatusRetrySchedule
  ): ZLayer[HttpExec, Nothing, HttpExec] =
    ZLayer.fromFunction { (inner: HttpExec) =>
      new HttpExec:
        def run(req: Request): IO[ExloError, HttpResponse] =
          inner.run(req).flatMap { resp =>
            if pred(resp.status) then
              ZIO.logAnnotate("event", "http.retry") {
                ZIO.logAnnotate("http_status", resp.status.toString) {
                  ZIO.logAnnotate("http_url", req.url.encode) {
                    ZIO.logWarning("retryable HTTP status; will retry")
                  }
                }
              } *> ZIO.fail(ExloError.ConnectorFailure(
                s"http status ${resp.status} matches retry predicate; will retry"
              ))
            else ZIO.succeed(resp)
          }.retry(schedule)
    }

  private val defaultStatusRetrySchedule: Schedule[Any, Any, Any] =
    (Schedule.exponential(1.second, factor = 3.0) && Schedule.recurs(10)).jittered

  /**
   * Decorator: fetch + cache an OAuth 2 access token via [[TokenManager]],
   * stamp it as `Authorization: Bearer <token>` on every request. Tokens
   * auto-refresh when expiry is within a 60-second margin. See
   * [[OAuthFlow]] for supported grants (ClientCredentials, RefreshToken,
   * Password/ROPC).
   */
  def oauth(flow: OAuthFlow): ZLayer[Client, Nothing, HttpExec] =
    ZLayer.fromZIO(
      for
        client  <- ZIO.service[Client]
        manager <- TokenManager.make(flow, client)
      yield
        val inner = liveImpl(client)
        new HttpExec:
          def run(req: Request): IO[ExloError, HttpResponse] =
            for
              tok  <- manager.token
              resp <- inner.run(req.addHeader("Authorization", s"Bearer $tok"))
            yield resp
    )

  private def liveImpl(client: Client): HttpExec = new HttpExec:
    def run(req: Request): IO[ExloError, HttpResponse] =
      val effect: ZIO[Client, ExloError, HttpResponse] =
        for
          resp <- ZClient
                    .batched(req)
                    .retry(defaultRetrySchedule)
                    .mapError(t =>
                      ExloError.ConnectorFailure(s"http request failed: ${t.getMessage}", t)
                    )
          bodyStr <- resp.body.asString.mapError(t =>
                       ExloError.ConnectorFailure(s"could not read response body: ${t.getMessage}", t)
                     )
          json <- ZIO.fromEither(bodyStr.fromJson[Json]).mapError(msg =>
                    ExloError.ConnectorFailure(
                      s"response body is not valid JSON: $msg",
                      new RuntimeException(msg)
                    )
                  )
          headers = resp.headers.toList.map(h => h.headerName -> h.renderedValue).toMap
        yield HttpResponse(resp.status.code, headers, bodyStr, json)
      effect.provideEnvironment(ZEnvironment(client))

  /** Production impl. */
  val live: ZLayer[Client, Nothing, HttpExec] =
    ZLayer.fromFunction(liveImpl(_))

  private def isTransient(t: Throwable): Boolean = t match
    case _: IOException      => true
    case _: TimeoutException => true
    case _                   => false

  private val defaultRetrySchedule: Schedule[Any, Throwable, Any] =
    (Schedule.recurWhile[Throwable](isTransient)
      && Schedule.exponential(1.second)
      && Schedule.recurs(3)).jittered
