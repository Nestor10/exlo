package exlo.http

import zio.*
import zio.http.*

/**
 * Cross-cutting HTTP execution config shared by [[HttpExtract]] and [[HttpSlice]] builders.
 * Bundles headers (auth, custom) and a retry schedule applied to each request.
 *
 * Two ways the schedule fires:
 *   - `retrySchedule` retries on `Throwable` failures (network errors, timeouts) — these are
 *     true failures from zio-http's perspective.
 *   - `retryOnStatus` (when set) lifts certain HTTP responses into synthetic failures so they
 *     also flow through the retry schedule. Use for 5xx, 429, etc.
 */
private[http] final case class HttpExecConfig(
    headers: Chunk[Header] = Chunk.empty,
    retrySchedule: Option[Schedule[Any, Any, Any]] = None,
    retryOnStatus: Option[Status => Boolean] = None,
    oauthFlow: Option[OAuthFlow] = None
):
  def addHeader(h: Header): HttpExecConfig = copy(headers = headers :+ h)

  def withRetry(s: Schedule[Any, Any, Any]): HttpExecConfig =
    copy(retrySchedule = Some(s))

  def withRetryOnStatus(p: Status => Boolean): HttpExecConfig =
    copy(retryOnStatus = Some(p))

  def withOAuth(flow: OAuthFlow): HttpExecConfig =
    copy(oauthFlow = Some(flow))

private[http] object HttpExec:

  /** Marker error used to lift a "retryable" response into the failure channel for `retry`. */
  private final case class RetryableResponse(response: Response) extends Throwable

  /**
   * Execute a single request with the given config and an optional `TokenManager`.
   * If `tokenManager` is set, an `Authorization: Bearer <fresh access token>` header is
   * added per request — `tokenManager.token` handles caching and refresh.
   */
  def execute(
      req: Request,
      cfg: HttpExecConfig,
      tokenManager: Option[TokenManager]
  ): ZIO[Client, Throwable, Response] =
    val withStaticHeaders = cfg.headers.foldLeft(req)((r, h) => r.addHeader(h))

    val withAuth: ZIO[Client, Throwable, Request] = tokenManager match
      case Some(tm) => tm.token.map(t => withStaticHeaders.addHeader(Header.Authorization.Bearer(t)))
      case None     => ZIO.succeed(withStaticHeaders)

    // Lift retryable responses into failures so the retry schedule covers them too.
    val sendOnce: ZIO[Client, Throwable, Response] =
      withAuth.flatMap(ZClient.batched).flatMap { resp =>
        cfg.retryOnStatus match
          case Some(p) if p(resp.status) => ZIO.fail(RetryableResponse(resp))
          case _                         => ZIO.succeed(resp)
      }

    val withRetries = cfg.retrySchedule match
      case Some(s) => sendOnce.retry(s)
      case None    => sendOnce

    // If the final outcome is a "retryable" response that exhausted the schedule, return it
    // rather than failing — the caller can inspect the status and decide.
    withRetries.catchSome { case RetryableResponse(r) => ZIO.succeed(r) }
