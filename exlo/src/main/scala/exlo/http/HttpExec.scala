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

  /** Production impl. */
  val live: ZLayer[Client, Nothing, HttpExec] =
    ZLayer.fromFunction { (client: Client) =>
      new HttpExec:
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
    }

  private def isTransient(t: Throwable): Boolean = t match
    case _: IOException      => true
    case _: TimeoutException => true
    case _                   => false

  private val defaultRetrySchedule: Schedule[Any, Throwable, Any] =
    (Schedule.recurWhile[Throwable](isTransient)
      && Schedule.exponential(1.second)
      && Schedule.recurs(3)).jittered
