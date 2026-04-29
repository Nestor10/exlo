package exlo.http

import exlo.Exlo
import exlo.runtime.{Destination, ExloState, SinkConfig, Telemetry}
import zio.*
import zio.http.*
import zio.json.*
import zio.test.*

/**
 * Tests for OAuth 2 token-fetching flows. Uses zio-http TestClient with a stubbed `/token`
 * endpoint and a `/protected` endpoint that requires `Authorization: Bearer <token>`.
 */
object OAuthSpec extends ZIOSpecDefault:

  final case class State(page: Int)
  final case class Page(records: List[String], next: Option[Int])
  given JsonCodec[Page] = DeriveJsonCodec.gen[Page]

  /** A token endpoint that issues tokens and tracks how many times it's been called. */
  private def tokenEndpoint(
      counter: Ref[Int],
      issuedToken: String = "issued-token-abc"
  ): Routes[Any, Response] =
    Routes(
      Method.POST / "token" -> handler { (req: Request) =>
        for
          n     <- counter.updateAndGet(_ + 1)
          // Token expires in 1 hour by default — well beyond any test timeline.
          body   = s"""{"access_token":"$issuedToken-$n","token_type":"Bearer","expires_in":3600,"refresh_token":"refresh-$n"}"""
        yield Response.json(body)
      }
    )

  /** A protected endpoint that 401s without a Bearer token. Returns one fake page. */
  private val protectedEndpoint: Routes[Any, Response] = Routes(
    Method.GET / "protected" -> handler { (req: Request) =>
      val auth = req.headers.get(Header.Authorization).map(_.renderedValue)
      if auth.exists(_.startsWith("Bearer issued-token-abc-")) then
        Response.json("""{"records":["alpha","beta"],"next":null}""")
      else Response.status(Status.Unauthorized)
    }
  )

  private val parsePage: Response => ZIO[Any, Throwable, Page] = resp =>
    resp.body.asString.flatMap(s =>
      ZIO.fromEither(s.fromJson[Page]).mapError(e => new RuntimeException(s"parse: $e"))
    )

  private def buildConnector(flow: OAuthFlow) = HttpExtract[State]
    .request(_ => Request.get(URL.decode("http://test.invalid/protected").toOption.get))
    .parse(parsePage)
    .records(p => Chunk.fromIterable(p.records))
    .advance((s, _) => s)
    .oauth(flow)
    .toConnector("oauth_test", "0.1.0")

  def spec = suite("OAuth")(
    test("ClientCredentials grant fetches a token and uses it as Bearer") {
      val flow = OAuthFlow.ClientCredentials(
        tokenUrl     = URL.decode("http://test.invalid/token").toOption.get,
        clientId     = "client-id",
        clientSecret = "client-secret"
      )
      for
        dest    <- Destination.InMemory.make[State]
        counter <- Ref.make(0)
        _       <- TestClient.addRoutes(tokenEndpoint(counter) ++ protectedEndpoint)
        _ <- Exlo
               .run(buildConnector(flow), State(1), SinkConfig.testing)
               .provideSome[Client](ZLayer.succeed[Destination[State]](dest) ++ Telemetry.noop)
        all       <- dest.allRecords
        tokenHits <- counter.get
      yield assertTrue(
        all == Chunk("alpha", "beta"),
        tokenHits == 1 // single fetch — token isn't expired during this short run
      )
    }.provide(TestClient.layer),
    test("RefreshToken grant exchanges the refresh token for an access token") {
      val flow = OAuthFlow.RefreshToken(
        tokenUrl     = URL.decode("http://test.invalid/token").toOption.get,
        clientId     = "client-id",
        clientSecret = "client-secret",
        refreshToken = "long-lived-rt"
      )
      for
        dest    <- Destination.InMemory.make[State]
        counter <- Ref.make(0)
        _       <- TestClient.addRoutes(tokenEndpoint(counter) ++ protectedEndpoint)
        _ <- Exlo
               .run(buildConnector(flow), State(1), SinkConfig.testing)
               .provideSome[Client](ZLayer.succeed[Destination[State]](dest) ++ Telemetry.noop)
        all <- dest.allRecords
      yield assertTrue(all == Chunk("alpha", "beta"))
    }.provide(TestClient.layer),
    test("Password grant exchanges username/password for an access token") {
      val flow = OAuthFlow.Password(
        tokenUrl     = URL.decode("http://test.invalid/token").toOption.get,
        clientId     = "client-id",
        clientSecret = Some("client-secret"),
        username     = "user@example.com",
        password     = "hunter2"
      )
      for
        dest    <- Destination.InMemory.make[State]
        counter <- Ref.make(0)
        _       <- TestClient.addRoutes(tokenEndpoint(counter) ++ protectedEndpoint)
        _ <- Exlo
               .run(buildConnector(flow), State(1), SinkConfig.testing)
               .provideSome[Client](ZLayer.succeed[Destination[State]](dest) ++ Telemetry.noop)
        all <- dest.allRecords
      yield assertTrue(all == Chunk("alpha", "beta"))
    }.provide(TestClient.layer),
    test("Password grant supports a custom grant_type and query-param injection") {
      // Capture exactly what the token endpoint received so we can assert the request was
      // shaped to match a non-standard ROPC layout: a custom grant_type in the body, with
      // username + client_id + grant_type pinned to the URL query string.
      final case class Captured(query: QueryParams, body: String)
      val flow = OAuthFlow.Password(
        tokenUrl    = URL.decode("http://test.invalid/token").toOption.get,
        clientId    = "custom-api-client",
        username    = "user@example.com",
        password    = "hunter2",
        grantType   = "custom-password",
        queryParams = Map(
          "username"   -> "user@example.com",
          "client_id"  -> "custom-api-client",
          "grant_type" -> "custom-password"
        )
      )
      val capturingTokenRoute: Ref[Option[Captured]] => Routes[Any, Response] = capture =>
        Routes(
          Method.POST / "token" -> handler { (req: Request) =>
            (for
              body <- req.body.asString
              _    <- capture.set(Some(Captured(req.url.queryParams, body)))
            yield Response.json(
              """{"access_token":"issued-token-abc-1","token_type":"Bearer","expires_in":3600}"""
            )).orDie
          }
        )
      for
        dest    <- Destination.InMemory.make[State]
        capture <- Ref.make(Option.empty[Captured])
        _       <- TestClient.addRoutes(capturingTokenRoute(capture) ++ protectedEndpoint)
        _ <- Exlo
               .run(buildConnector(flow), State(1), SinkConfig.testing)
               .provideSome[Client](ZLayer.succeed[Destination[State]](dest) ++ Telemetry.noop)
        all      <- dest.allRecords
        captured <- capture.get
      yield
        val c = captured.getOrElse(throw new AssertionError("token endpoint was not called"))
        assertTrue(
          all == Chunk("alpha", "beta"),
          // Custom grant_type lands in the POST body (form-encoded).
          c.body.contains("grant_type=custom-password"),
          c.body.contains("password=hunter2"),
          c.body.contains("client_id=custom-api-client"),
          c.body.contains("username=user%40example.com"),
          // queryParams are injected into the token URL — not stripped, not double-encoded.
          c.query.queryParam("username").contains("user@example.com"),
          c.query.queryParam("client_id").contains("custom-api-client"),
          c.query.queryParam("grant_type").contains("custom-password")
        )
    }.provide(TestClient.layer),
    test("token endpoint failure surfaces as a Throwable with the response body") {
      val flow = OAuthFlow.ClientCredentials(
        tokenUrl     = URL.decode("http://test.invalid/token").toOption.get,
        clientId     = "bad-client",
        clientSecret = "bad-secret"
      )
      val badTokenRoute = Routes(
        Method.POST / "token" -> handler { (_: Request) =>
          Response.status(Status.Unauthorized).copy(body = Body.fromString("invalid_client"))
        }
      )
      for
        dest <- Destination.InMemory.make[State]
        _    <- TestClient.addRoutes(badTokenRoute ++ protectedEndpoint)
        result <- Exlo
                    .run(buildConnector(flow), State(1), SinkConfig.testing)
                    .provideSome[Client](ZLayer.succeed[Destination[State]](dest) ++ Telemetry.noop)
                    .either
      yield assertTrue(
        result.left.exists { e =>
          val msg = Option(e.getMessage).getOrElse("")
          msg.contains("OAuth token endpoint") && msg.contains("401")
        }
      )
    }.provide(TestClient.layer)
  )
