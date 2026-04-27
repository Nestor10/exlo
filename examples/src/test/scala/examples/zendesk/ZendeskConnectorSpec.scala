package examples.zendesk

import exlo.Exlo
import exlo.runtime.{Destination, SinkConfig}
import zio.*
import zio.http.*
import zio.test.*

object ZendeskConnectorSpec extends ZIOSpecDefault:

  val testRoutes: Routes[Any, Response] = Routes(
    Method.GET / "api" / "v2" / "ticket_metrics" -> handler { (req: Request) =>
      val authOk =
        req.headers.get(Header.Authorization).map(_.renderedValue).contains("Basic dXNlcjpzZWNyZXQ=")
      if !authOk then Response.status(Status.Unauthorized)
      else
        val page = req.url.queryParams.queryParam("page").flatMap(_.toIntOption).getOrElse(1)
        val nextUrl =
          if page < 3 then s""""next":"http://test.invalid/api/v2/ticket_metrics?page=${page + 1}""""
          else """"next":null"""
        val day = 14 - page
        val r1  = s"""{"id":${page * 10},"updated_at":"2026-04-${"%02d".format(day)}T20:00:00Z"}"""
        val r2  = s"""{"id":${page * 10 + 1},"updated_at":"2026-04-${"%02d".format(day)}T10:00:00Z"}"""
        Response.json(s"""{"ticket_metrics":[$r1,$r2],"links":{$nextUrl}}""")
    }
  )

  def spec = suite("ZendeskConnector")(
    test("walks all pages via links.next, accumulates records, advances cursor to newest seen") {
      val connector = ZendeskConnector.make(
        subdomain = "test",
        username  = "user",
        password  = "secret"
      )
      for
        dest <- Destination.InMemory.make[ZendeskConnector.State]
        _    <- TestClient.addRoutes(testRoutes)
        _ <- Exlo
               .run(connector, ZendeskConnector.State(""), SinkConfig.testing)
               .provideSome[Client](ZLayer.succeed[Destination[ZendeskConnector.State]](dest))
        all  <- dest.allRecords
        snap <- dest.readState
      yield assertTrue(
        all.length == 6,
        snap.exists(_.cursor == "2026-04-13T20:00:00Z")
      )
    }.provide(TestClient.layer)
  )
