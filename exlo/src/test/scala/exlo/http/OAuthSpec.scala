package exlo.http

import zio.*
import zio.http.*
import zio.test.*

/**
 * Smoke tests for the [[HttpExec.oauth]] decorator.
 *
 * Uses zio-http's `TestClient` with a stubbed `/token` endpoint and a
 * `/protected` endpoint that 401s without a `Bearer` header. Verifies
 * each flow successfully fetches a token, stamps it onto downstream
 * requests, and caches it across multiple calls.
 */
object OAuthSpec extends ZIOSpecDefault:

  /** Stubbed token endpoint. Counts hits in `counter`; issues
   *  predictable access + refresh tokens. */
  private def tokenRoutes(counter: Ref[Int]): Routes[Any, Response] =
    Routes(
      Method.POST / "token" -> handler { (req: Request) =>
        for
          n <- counter.updateAndGet(_ + 1)
          body =
            s"""{"access_token":"access-$n","token_type":"Bearer",""" +
              s""""expires_in":3600,"refresh_token":"refresh-$n"}"""
        yield Response.json(body)
      }
    )

  /** Protected endpoint: 401s without `Bearer access-*`; otherwise echoes
   *  the token in a JSON body for assertions. */
  private val protectedRoutes: Routes[Any, Response] =
    Routes(
      Method.GET / "protected" -> handler { (req: Request) =>
        val auth = req.headers.get(Header.Authorization).map(_.renderedValue)
        if auth.exists(_.startsWith("Bearer access-")) then
          Response.json(s"""{"got":"${auth.get}"}""")
        else Response.status(Status.Unauthorized)
      }
    )

  private val testRoutes: Ref[Int] => Routes[Any, Response] =
    counter => tokenRoutes(counter) ++ protectedRoutes

  private val protectedReq: Request =
    Request.get(URL.decode("http://test.invalid/protected").toOption.get)

  def spec = suite("HttpExec.oauth")(

    test("ClientCredentials: fetches a token, stamps Bearer, caches across calls") {
      val flow = OAuthFlow.ClientCredentials(
        tokenUrl     = URL.decode("http://test.invalid/token").toOption.get,
        clientId     = "id",
        clientSecret = "secret"
      )
      for
        counter <- Ref.make(0)
        _       <- TestClient.addRoutes(testRoutes(counter))
        execL    = HttpExec.oauth(flow)
        program = ZIO.scoped {
                    for
                      r1 <- HttpExec.run(protectedReq)
                      r2 <- HttpExec.run(protectedReq)
                      r3 <- HttpExec.run(protectedReq)
                    yield (r1, r2, r3)
                  }
        result   <- program.provideSome[Client](execL)
        hits     <- counter.get
      yield assertTrue(
        result._1.status == 200,
        result._2.status == 200,
        result._3.status == 200,
        // First fetch caches the token; subsequent calls reuse it (expires_in=3600).
        hits == 1,
        result._1.body.contains("Bearer access-1")
      )
    }.provide(TestClient.layer),

    test("RefreshToken: trades refresh_token for access_token") {
      val flow = OAuthFlow.RefreshToken(
        tokenUrl     = URL.decode("http://test.invalid/token").toOption.get,
        clientId     = "id",
        clientSecret = "secret",
        refreshToken = "long-lived-rt"
      )
      for
        counter <- Ref.make(0)
        _       <- TestClient.addRoutes(testRoutes(counter))
        execL    = HttpExec.oauth(flow)
        result  <- HttpExec.run(protectedReq).provideSome[Client](execL)
        hits    <- counter.get
      yield assertTrue(
        result.status == 200,
        hits == 1,
        result.body.contains("Bearer access-1")
      )
    }.provide(TestClient.layer),

    test("Password (ROPC): trades username/password for access_token") {
      val flow = OAuthFlow.Password(
        tokenUrl     = URL.decode("http://test.invalid/token").toOption.get,
        clientId     = "id",
        username     = "user",
        password     = "pass"
      )
      for
        counter <- Ref.make(0)
        _       <- TestClient.addRoutes(testRoutes(counter))
        execL    = HttpExec.oauth(flow)
        result  <- HttpExec.run(protectedReq).provideSome[Client](execL)
        hits    <- counter.get
      yield assertTrue(
        result.status == 200,
        hits == 1,
        result.body.contains("Bearer access-1")
      )
    }.provide(TestClient.layer)
  )
