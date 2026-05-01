package examples

import exlo.domain.ExloError
import exlo.http.{HttpExec, HttpResponse, HttpStream}
import exlo.http.HttpResponse.field
import exlo.runtime.{DataSink, FlushPolicy, Runner, StateStore}
import zio.*
import zio.http.{Request, URL}
import zio.json.*
import zio.json.ast.Json
import zio.test.*

/**
 * Litmus test for the [[HttpStream]] / [[exlo.HttpExloApp]] shape.
 *
 * Uses a fake [[HttpExec]] layer that returns canned responses based on the
 * URL — no real `zio.http.Server`, no port juggling, no network. Tests the
 * shape and the runner pipeline; the live `HttpExec.live` (which uses
 * `Client.default`) is exercised by ad-hoc smoke tests against real APIs
 * in a follow-up.
 */
object PokeApiSpec extends ZIOSpecDefault:

  /**
   * The connector. In production, this is `object PokeApi extends
   * HttpExloApp` with a hardcoded `pokeapi.co` URL. The litmus value is
   * how short and obvious it reads.
   */
  object PokeStream extends HttpStream:
    val name = "pokemon"

    def request(s: Map[String, String], c: Map[String, String]): Request =
      HttpExec.get(c.getOrElse("next", "https://pokeapi.co/api/v2/pokemon?offset=0"))

    def records(s: Map[String, String], c: Map[String, String], r: HttpResponse): Chunk[String] =
      r.json.field("results")
        .flatMap(_.asArray)
        .getOrElse(Chunk.empty)
        .map(_.toJson)

    def nextCtx(s: Map[String, String], c: Map[String, String], r: HttpResponse): Option[Map[String, String]] =
      r.json.field("next").flatMap(_.asString).map(url => Map("next" -> url))

  /** Two-page fake: offset=0 → 2 records + next URL; offset=2 → 1 record + null. */
  private def fakeHttpExec: ULayer[HttpExec] =
    ZLayer.succeed(
      new HttpExec:
        def run(req: Request): IO[ExloError, HttpResponse] =
          val url    = req.url.encode
          val offset = req.url.queryParams.queryParam("offset").getOrElse("0")
          val body = offset match
            case "0" =>
              """{"results":[{"name":"bulbasaur"},{"name":"ivysaur"}],""" +
                """"next":"https://pokeapi.co/api/v2/pokemon?offset=2"}"""
            case "2" =>
              """{"results":[{"name":"venusaur"}],"next":null}"""
            case _ =>
              throw new RuntimeException(s"unexpected offset $offset for $url")
          ZIO.fromEither(body.fromJson[Json])
            .mapError(msg => ExloError.ConnectorFailure(msg))
            .map(json => HttpResponse(200, Map.empty, body, json))
    )

  override def spec: Spec[TestEnvironment & Scope, Any] =
    suite("PokeApi (HttpStream litmus)")(
      test("paginates through 2 pages and emits 3 records") {
        val effect = for
          sink  <- DataSink.InMemory.make
          store <- StateStore.InMemory.make
          _     <- Runner.run(
                     PokeStream.asConnector("pokeapi", "1.0.0"),
                     "pokemon", "test-sync",
                     sink, store,
                     FlushPolicy(maxRows = 100, maxInterval = 100.millis)
                   )
          rows  <- sink.collected
        yield assertTrue(
          rows.size == 3,
          rows.map(_.value).exists(_.contains("bulbasaur")),
          rows.map(_.value).exists(_.contains("ivysaur")),
          rows.map(_.value).exists(_.contains("venusaur"))
        )

        effect.provide(fakeHttpExec)
      } @@ TestAspect.timeout(15.seconds)
    )
