package exlo.http

import exlo.Exlo
import exlo.runtime.{Destination, ExloState, SinkConfig}
import zio.*
import zio.http.*
import zio.json.*
import zio.test.*

object HttpExtractSpec extends ZIOSpecDefault:

  /** Connector state: which page we're on. */
  final case class State(page: Int)

  /** What the test endpoint returns. */
  final case class Page(records: List[String], next: Option[Int])
  given JsonDecoder[Page] = DeriveJsonDecoder.gen[Page]
  given JsonEncoder[Page] = DeriveJsonEncoder.gen[Page]

  val testRoutes: Routes[Any, Response] = Routes(
    Method.GET / "items" -> handler { (req: Request) =>
      val page = req.url.queryParams.queryParam("page").flatMap(_.toIntOption).getOrElse(1)
      val resp = Page(
        records = List(s"r-$page-1", s"r-$page-2"),
        next    = if page < 3 then Some(page + 1) else None
      )
      Response.json(resp.toJson)
    }
  )

  val parsePage: Response => ZIO[Any, Throwable, Page] = resp =>
    resp.body.asString.flatMap(s =>
      ZIO.fromEither(s.fromJson[Page]).mapError(e => new RuntimeException(s"parse error: $e"))
    )

  val cursorConnector = HttpExtract[State]
    .request(state => Request.get(URL.decode(s"http://test.invalid/items?page=${state.page}").toOption.get))
    .parse(parsePage)
    .records(page => Chunk.fromIterable(page.records))
    .nextRequest((state, page) =>
      page.next.map(n => Request.get(URL.decode(s"http://test.invalid/items?page=$n").toOption.get))
    )
    .advance((state, page) => state.copy(page = page.next.getOrElse(state.page)))
    .toConnector("test-cursor", "0.1.0")

  def spec = suite("HttpExtract")(
    test("walks all pages via nextRequest, accumulating records, advances state") {
      for
        dest <- Destination.InMemory.make[State]
        _    <- TestClient.addRoutes(testRoutes)
        _ <- Exlo
               .run(cursorConnector, State(1), SinkConfig.testing)
               .provideSome[Client](ZLayer.succeed[Destination[State]](dest))
        all  <- dest.allRecords
        snap <- dest.readState
      yield assertTrue(
        all == Chunk("r-1-1", "r-1-2", "r-2-1", "r-2-2", "r-3-1", "r-3-2"),
        snap == Some(State(page = 3))
      )
    }.provide(TestClient.layer),
    test("bearer: Authorization header is sent on every request") {
      val authedRoute = Routes(
        Method.GET / "secure" -> handler { (req: Request) =>
          val auth = req.headers.get(Header.Authorization).map(_.renderedValue)
          if auth.contains("Bearer secret-xyz") then
            Response.json("""{"records":["ok"],"next":null}""")
          else Response.status(Status.Unauthorized)
        }
      )
      val authed = HttpExtract[State]
        .request(_ => Request.get(URL.decode("http://test.invalid/secure").toOption.get))
        .parse(parsePage)
        .records(p => Chunk.fromIterable(p.records))
        .advance((s, _) => s)
        .bearer("secret-xyz")
        .toConnector("authed", "0.1.0")
      for
        dest <- Destination.InMemory.make[State]
        _    <- TestClient.addRoutes(authedRoute)
        _ <- Exlo
               .run(authed, State(1), SinkConfig.testing)
               .provideSome[Client](ZLayer.succeed[Destination[State]](dest))
        all <- dest.allRecords
      yield assertTrue(all == Chunk("ok"))
    }.provide(TestClient.layer),
    test("retryOnStatus + retry: transient 503 responses are retried until success") {
      val resilient = HttpExtract[State]
        .request(_ => Request.get(URL.decode("http://test.invalid/flaky").toOption.get))
        .parse(parsePage)
        .records(p => Chunk.fromIterable(p.records))
        .advance((s, _) => s)
        .retryOnStatus(s => s.code == 503)
        .retry(Schedule.recurs(5))
        .toConnector("resilient", "0.1.0")

      for
        dest    <- Destination.InMemory.make[State]
        counter <- Ref.make(0)
        flakyRoute = Routes(
          Method.GET / "flaky" -> handler { (_: Request) =>
            counter.updateAndGet(_ + 1).map { n =>
              if n < 3 then Response.status(Status.ServiceUnavailable)
              else Response.json("""{"records":["finally"],"next":null}""")
            }
          }
        )
        _ <- TestClient.addRoutes(flakyRoute)
        _ <- Exlo
               .run(resilient, State(1), SinkConfig.testing)
               .provideSome[Client](ZLayer.succeed[Destination[State]](dest))
        all <- dest.allRecords
        n   <- counter.get
      yield assertTrue(all == Chunk("finally"), n == 3)
    }.provide(TestClient.layer),
    test("fullPull: stateless single-request connector for reference-data endpoints") {
      val staticRoute = Routes(
        Method.GET / "countries" -> handler { (_: Request) =>
          Response.json("""{"records":["US","CA","MX","UK","FR"],"next":null}""")
        }
      )
      val countries = HttpExtract.fullPull
        .request(Request.get(URL.decode("http://test.invalid/countries").toOption.get))
        .parse(parsePage)
        .records(page => Chunk.fromIterable(page.records))
        .toConnector("countries", "0.1.0")
      for
        dest <- Destination.InMemory.make[Unit]
        _    <- TestClient.addRoutes(staticRoute)
        _ <- Exlo
               .run(countries, (), SinkConfig.testing)
               .provideSome[Client](ZLayer.succeed[Destination[Unit]](dest))
        all <- dest.allRecords
      yield assertTrue(all == Chunk("US", "CA", "MX", "UK", "FR"))
    }.provide(TestClient.layer)
  )
