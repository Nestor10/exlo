package exlo.http

import io.netty.handler.codec.PrematureChannelClosureException
import zio.*
import zio.http.*
import zio.telemetry.opentelemetry.core.trace.Tracer

import java.io.IOException
import java.util.concurrent.TimeoutException

/**
 * Cross-cutting HTTP execution config shared by [[HttpExtract]] and [[HttpSlice]] builders.
 * Bundles headers (auth, custom) and a retry schedule applied to each request.
 *
 * Two ways the schedule fires:
 *   - `retrySchedule` retries on `Throwable` failures (network errors, timeouts) — these are
 *     true failures from zio-http's perspective.
 *   - `retryOnStatus` (when set) lifts certain HTTP responses into synthetic failures so they
 *     also flow through the retry schedule. Use for 5xx, 429, etc.
 *
 * **Default retry.** `retrySchedule` defaults to a small jittered exponential backoff
 * (3 retries, ~1s/2s/4s with jitter), gated to *transient network-class* throwables only:
 * `IOException` (covers connection resets, DNS blips), Netty's
 * `PrematureChannelClosureException` (load balancer / NAT closes a pooled connection mid
 * in-flight request), and `TimeoutException`. App-level errors (decoding, schema mismatch)
 * fail fast — we only retry the things that are physically transient.
 *
 * Why narrow rather than blanket-retry: the framework accepts any HTTP verb (GraphQL
 * sources will use POST), so we can't assume idempotence at the application level. But
 * TCP/connection failures are safe to retry under any verb: either the request never
 * reached the server, or the response was lost — and exlo's downstream is dedup-tolerant
 * (at-least-once is the contract).
 *
 * Connector authors override per-stream via `.retry(customSchedule)`.
 */
private[http] final case class HttpExecConfig(
    headers: Chunk[Header] = Chunk.empty,
    retrySchedule: Option[Schedule[Any, Throwable, Any]] =
      Some(
        (Schedule.recurWhile[Throwable](HttpExec.isTransient)
          && Schedule.exponential(1.second)
          && Schedule.recurs(3)).jittered
      ),
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

  /**
   * Predicate identifying transient network-class failures — those for which the request
   * either never reached the server or the response was lost. Safe to retry under any
   * HTTP verb (POSTs included) because no application-level state has been touched.
   */
  def isTransient(t: Throwable): Boolean = t match
    case _: IOException                      => true
    case _: PrematureChannelClosureException => true
    case _: TimeoutException                 => true
    case _                                   => false

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
  ): ZIO[Client & Tracer, Throwable, Response] =
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
    // rather than failing — the caller can inspect the status and decide. One span covers the
    // full attempt-and-retry cycle (retry deltas show up in span duration, not as child spans).
    val sendWithFallback = withRetries.catchSome { case RetryableResponse(r) => ZIO.succeed(r) }

    ZIO.serviceWithZIO[Tracer] { tracer =>
      tracer.span(s"HTTP ${req.method.name}") { span =>
        span.setAttribute("http.method", req.method.name) *>
          span.setAttribute("http.url", req.url.encode) *>
          sendWithFallback.tap(resp =>
            span.setAttribute("http.status_code", resp.status.code.toLong)
          )
      }
    }
