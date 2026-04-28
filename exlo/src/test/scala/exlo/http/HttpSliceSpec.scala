package exlo.http

import exlo.Exlo
import exlo.runtime.{Destination, ExloState, SinkConfig}
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
               .provideSome[Client](ZLayer.succeed[Destination[State]](dest))
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
               .provideSome[Client](ZLayer.succeed[Destination[State]](dest))
        all  <- dest.allRecords
        snap <- dest.readState
      yield assertTrue(
        all.length == 2 * 2 * 2,
        all.forall(r => r.startsWith("s3-") || r.startsWith("s4-")),
        snap.exists(_.done == Set(1, 2, 3, 4))
      )
    }.provide(TestClient.layer)
  )
