package exlo.http

import exlo.Exlo
import exlo.domain.RecoveryAction
import exlo.runtime.{Destination, ExloState, SinkConfig, Telemetry}
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
    .toConnector("test_cursor", "0.1.0")

  def spec = suite("HttpExtract")(
    test("walks all pages via nextRequest, accumulating records, advances state") {
      for
        dest <- Destination.InMemory.make[State]
        _    <- TestClient.addRoutes(testRoutes)
        _ <- Exlo
               .run(cursorConnector, State(1), SinkConfig.testing)
               .provideSome[Client](ZLayer.succeed[Destination[State]](dest) ++ Telemetry.noop)
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
               .provideSome[Client](ZLayer.succeed[Destination[State]](dest) ++ Telemetry.noop)
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
               .provideSome[Client](ZLayer.succeed[Destination[State]](dest) ++ Telemetry.noop)
        all <- dest.allRecords
        n   <- counter.get
      yield assertTrue(all == Chunk("finally"), n == 3)
    }.provide(TestClient.layer),
    test("retryOnStatus with the DEFAULT schedule retries 429 with backoff") {
      // Regression: previously, the default schedule's `recurWhile(isTransient)` gate
      // excluded `RetryableResponse`, so .retryOnStatus(_ == 429) silently became a
      // no-op — the framework returned the 429 to the parser, which then crashed.
      val rateLimited = HttpExtract[State]
        .request(_ => Request.get(URL.decode("http://test.invalid/rate").toOption.get))
        .parse(parsePage)
        .records(p => Chunk.fromIterable(p.records))
        .advance((s, _) => s)
        .retryOnStatus(_ == Status.TooManyRequests)
        // No .retry() override — relies on HttpExec.defaultRetrySchedule (1s/2s/4s).
        .toConnector("rate_limited", "0.1.0")

      for
        dest    <- Destination.InMemory.make[State]
        counter <- Ref.make(0)
        flakyRoute = Routes(
          Method.GET / "rate" -> handler { (_: Request) =>
            counter.updateAndGet(_ + 1).map { n =>
              if n < 3 then Response.status(Status.TooManyRequests)
              else Response.json("""{"records":["finally"],"next":null}""")
            }
          }
        )
        _        <- TestClient.addRoutes(flakyRoute)
        runFiber <- Exlo
                      .run(rateLimited, State(1), SinkConfig.testing)
                      .provideSome[Client](ZLayer.succeed[Destination[State]](dest) ++ Telemetry.noop)
                      .fork
        // Default schedule waits ~1s, then ~2s. Adjust generously to cover jitter.
        _   <- TestClock.adjust(20.seconds)
        _   <- runFiber.join
        all <- dest.allRecords
        n   <- counter.get
      yield assertTrue(all == Chunk("finally"), n == 3)
    }.provide(TestClient.layer),
    test("recover: matched error mutates state and re-issues request from new state") {
      // Models the poison-pill case: the first request to ?page=1 fails with a
      // PoisonError. The recover handler narrows state (page=2). On re-entry, the next
      // request fetches page=2, which succeeds and walks normally to page=3.
      final case class PoisonError(msg: String) extends Throwable(msg)

      val flakyRoute = Routes(
        Method.GET / "items" -> handler { (req: Request) =>
          val page = req.url.queryParams.queryParam("page").flatMap(_.toIntOption).getOrElse(1)
          if page == 1 then
            // Server error simulating a poisoned range. Surfaced as a Throwable via parse.
            Response.json("""{"poisoned":true}""")
          else
            val resp = Page(
              records = List(s"r-$page-1"),
              next    = if page < 3 then Some(page + 1) else None
            )
            Response.json(resp.toJson)
        }
      )
      // Parse fails with PoisonError on the poisoned response; otherwise decodes normally.
      val parseOrPoison: Response => ZIO[Any, Throwable, Page] = resp =>
        resp.body.asString.flatMap { s =>
          if s.contains("poisoned") then ZIO.fail(PoisonError(s))
          else ZIO.fromEither(s.fromJson[Page])
            .mapError(e => new RuntimeException(s"parse error: $e"))
        }

      val withRecover = HttpExtract[State]
        .request(state => Request.get(URL.decode(s"http://test.invalid/items?page=${state.page}").toOption.get))
        .parse(parseOrPoison)
        .records(p => Chunk.fromIterable(p.records))
        .nextRequest((_, p) => p.next.map(n => Request.get(URL.decode(s"http://test.invalid/items?page=$n").toOption.get)))
        .advance((s, p) => s.copy(page = p.next.getOrElse(s.page)))
        .recover { case (s, _: PoisonError) => RecoveryAction.Continue(s.copy(page = s.page + 1)) }
        .toConnector("with_recover", "0.1.0")

      for
        dest <- Destination.InMemory.make[State]
        _    <- TestClient.addRoutes(flakyRoute)
        _ <- Exlo
               .run(withRecover, State(1), SinkConfig.testing)
               .provideSome[Client](ZLayer.succeed[Destination[State]](dest) ++ Telemetry.noop)
        all  <- dest.allRecords
        snap <- dest.readState
      yield assertTrue(
        all == Chunk("r-2-1", "r-3-1"),
        snap == Some(State(page = 3))
      )
    }.provide(TestClient.layer),
    test("recover: unmatched error propagates and fails the stream") {
      final case class OtherError(msg: String) extends Throwable(msg)

      val brokenParse: Response => ZIO[Any, Throwable, Page] = _ =>
        ZIO.fail(OtherError("not handled by recover"))

      val withRecover = HttpExtract[State]
        .request(_ => Request.get(URL.decode("http://test.invalid/items?page=1").toOption.get))
        .parse(brokenParse)
        .records(p => Chunk.fromIterable(p.records))
        .advance((s, _) => s)
        .recover { case (s, e) if e.getMessage == "specific" => RecoveryAction.Continue(s) }
        .toConnector("unmatched", "0.1.0")

      val anyRoute = Routes(
        Method.GET / "items" -> handler { (_: Request) => Response.json("{}") }
      )

      for
        dest   <- Destination.InMemory.make[State]
        _      <- TestClient.addRoutes(anyRoute)
        result <- Exlo
                    .run(withRecover, State(1), SinkConfig.testing)
                    .provideSome[Client](ZLayer.succeed[Destination[State]](dest) ++ Telemetry.noop)
                    .either
      yield assertTrue(result.isLeft)
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
               .provideSome[Client](ZLayer.succeed[Destination[Unit]](dest) ++ Telemetry.noop)
        all <- dest.allRecords
      yield assertTrue(all == Chunk("US", "CA", "MX", "UK", "FR"))
    }.provide(TestClient.layer)
  )
