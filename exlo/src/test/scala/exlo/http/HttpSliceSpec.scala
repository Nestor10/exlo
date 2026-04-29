package exlo.http

import exlo.Exlo
import exlo.domain.RecoveryAction
import exlo.runtime.{Destination, ExloState, SinkConfig, Telemetry}
import zio.*
import zio.http.*
import zio.json.*
import zio.stream.ZStream
import zio.test.*
import zio.test.TestAspect.*

object HttpSliceSpec extends ZIOSpecDefault:

  final case class State(done: Set[Int])
  final case class Page(records: List[String], next: Option[Int])
  given JsonDecoder[Page] = DeriveJsonDecoder.gen[Page]
  given JsonEncoder[Page] = DeriveJsonEncoder.gen[Page]

  val testRoutes: Routes[Any, Response] = Routes(
    Method.GET / "items" -> handler { (req: Request) =>
      val slice = req.url.queryParams.queryParam("slice").flatMap(_.toIntOption).getOrElse(0)
      val page  = req.url.queryParams.queryParam("page").flatMap(_.toIntOption).getOrElse(1)
      val resp = Page(
        records = List(s"s$slice-p$page-r1", s"s$slice-p$page-r2"),
        next    = if page < 2 then Some(page + 1) else None
      )
      Response.json(resp.toJson)
    }
  )

  val parsePage: Response => ZIO[Any, Throwable, Page] = resp =>
    resp.body.asString.flatMap(s =>
      ZIO.fromEither(s.fromJson[Page]).mapError(e => new RuntimeException(s"parse error: $e"))
    )

  val backfill = HttpSlice[Int, State]
    .slices(state => ZStream.fromIterable(1 to 4).filterNot(state.done.contains))
    .request((slice, _) =>
      Request.get(URL.decode(s"http://test.invalid/items?slice=$slice&page=1").toOption.get)
    )
    .parse(parsePage)
    .records(page => Chunk.fromIterable(page.records))
    .nextRequest((slice, _, page) =>
      page.next.map(p =>
        Request.get(URL.decode(s"http://test.invalid/items?slice=$slice&page=$p").toOption.get)
      )
    )
    .advance((state, slice, page) =>
      if page.next.isEmpty then state.copy(done = state.done + slice) else state
    )
    .parallelism(4)
    .toSlicedConnector("backfill", "0.1.0")

  def spec = suite("HttpSlice")(
    test("parallel slices, intra-slice pagination: every record lands and every slice marks done") {
      for
        dest <- Destination.InMemory.make[State]
        _    <- TestClient.addRoutes(testRoutes)
        _ <- Exlo
               .run(backfill.toConnector, State(Set.empty), SinkConfig.testing)
               .provideSome[Client](ZLayer.succeed[Destination[State]](dest) ++ Telemetry.noop)
        all  <- dest.allRecords
        snap <- dest.readState
      yield assertTrue(
        all.length == 4 * 2 * 2,
        all.toSet.size == 16,
        snap.exists(_.done == Set(1, 2, 3, 4))
      )
    }.provide(TestClient.layer) @@ nonFlaky(5),
    test("resume: pre-seeded done set makes those slices skipped") {
      for
        dest <- Destination.InMemory.seeded[State](
                  Chunk(Destination.InMemory.Snapshot(Chunk.empty, State(done = Set(1, 2))))
                )
        _ <- TestClient.addRoutes(testRoutes)
        _ <- Exlo
               .run(backfill.toConnector, State(Set.empty), SinkConfig.testing)
               .provideSome[Client](ZLayer.succeed[Destination[State]](dest) ++ Telemetry.noop)
        all  <- dest.allRecords
        snap <- dest.readState
      yield assertTrue(
        all.length == 2 * 2 * 2,
        all.forall(r => r.startsWith("s3-") || r.startsWith("s4-")),
        snap.exists(_.done == Set(1, 2, 3, 4))
      )
    }.provide(TestClient.layer),
    test("recover: matched error mutates state, slice re-issues request from new state") {
      // Models the bisection / poison-skip pattern: slice 2 fails on its first attempt
      // ("the window is too wide"). Recover narrows state — the request fn reads
      // state.attempts to decide which URL to hit. Second attempt succeeds.
      final case class PoisonError(msg: String) extends Throwable(msg)
      final case class State2(done: Set[Int], attempts: Int)

      val routes: Routes[Any, Response] = Routes(
        Method.GET / "items" -> handler { (req: Request) =>
          val slice    = req.url.queryParams.queryParam("slice").flatMap(_.toIntOption).getOrElse(0)
          val attempts = req.url.queryParams.queryParam("attempt").flatMap(_.toIntOption).getOrElse(0)
          if slice == 2 && attempts == 0 then Response.json("""{"poisoned":true}""")
          else Response.json(s"""{"records":["s$slice-a$attempts"],"next":null}""")
        }
      )
      val parseOrPoison: Response => ZIO[Any, Throwable, Page] = resp =>
        resp.body.asString.flatMap { s =>
          if s.contains("poisoned") then ZIO.fail(PoisonError(s))
          else ZIO.fromEither(s.fromJson[Page])
            .mapError(e => new RuntimeException(s"parse error: $e"))
        }

      val resilient = HttpSlice[Int, State2]
        .slices(state => ZStream.fromIterable(1 to 2).filterNot(state.done.contains))
        .request((slice, state) =>
          Request.get(URL.decode(s"http://test.invalid/items?slice=$slice&attempt=${state.attempts}").toOption.get)
        )
        .parse(parseOrPoison)
        .records(page => Chunk.fromIterable(page.records))
        .advance((state, slice, _) => state.copy(done = state.done + slice, attempts = 0))
        .recover {
          case (_, state, _: PoisonError) =>
            RecoveryAction.Continue(state.copy(attempts = state.attempts + 1))
        }
        .parallelism(1) // serial so recover-state doesn't race
        .toSlicedConnector("resilient_slice", "0.1.0")

      for
        dest <- Destination.InMemory.make[State2]
        _    <- TestClient.addRoutes(routes)
        _ <- Exlo
               .run(resilient.toConnector, State2(Set.empty, 0), SinkConfig.testing)
               .provideSome[Client](ZLayer.succeed[Destination[State2]](dest) ++ Telemetry.noop)
        all  <- dest.allRecords
        snap <- dest.readState
      yield assertTrue(
        all.toSet == Set("s1-a0", "s2-a1"),
        snap.exists(_.done == Set(1, 2))
      )
    }.provide(TestClient.layer)
  )
