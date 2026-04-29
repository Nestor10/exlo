package examples.glance

import exlo.Exlo
import exlo.runtime.{Destination, SinkConfig, Telemetry}
import zio.*
import zio.http.*
import zio.test.*
import zio.test.TestAspect.*

object GlanceConnectorSpec extends ZIOSpecDefault:

  val testRoutes: Routes[Any, Response] = Routes(
    Method.GET / "broadcasts" -> handler { (req: Request) =>
      val auth = req.headers.get(Header.Authorization).map(_.renderedValue)
      if !auth.contains("Bearer test-token") then Response.status(Status.Unauthorized)
      else
        val region = req.url.queryParams.queryParam("region").getOrElse("0")
        val page   = req.url.queryParams.queryParam("page").flatMap(_.toIntOption).getOrElse(1)
        val rec1   = s"""{"id":"r${region}-p${page}-1"}"""
        val rec2   = s"""{"id":"r${region}-p${page}-2"}"""
        val nextField = if page < 2 then s""","nextPage":${page + 1}""" else ""
        Response.json(s"""{"broadcasts":[$rec1,$rec2]$nextField}""")
    }
  )

  def spec = suite("GlanceConnector")(
    test("4 parallel slices × 2 pages × 2 records all land; bearer auth enforced") {
      val connector = GlanceConnector.make(
        baseUrl = "http://test.invalid",
        token   = "test-token"
      )
      for
        dest <- Destination.InMemory.make[GlanceConnector.State]
        _    <- TestClient.addRoutes(testRoutes)
        _ <- Exlo
               .run(connector.toConnector, GlanceConnector.State(Set.empty), SinkConfig.testing)
               .provideSome[Client](ZLayer.succeed[Destination[GlanceConnector.State]](dest) ++ Telemetry.noop)
        all  <- dest.allRecords
        snap <- dest.readState
      yield assertTrue(
        all.length == 4 * 2 * 2,
        all.toSet.size == 16,
        snap.exists(_.done == Set("italy", "france", "germany", "united_kingdom"))
      )
    }.provide(TestClient.layer) @@ nonFlaky(5)
  )
