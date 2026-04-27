package examples.zendesk

import exlo.Exlo
import exlo.runtime.{Destination, SinkConfig, StreamRegistry}
import zio.*
import zio.http.*
import zio.test.*

object ZendeskConnectorSpec extends ZIOSpecDefault:

  /**
   * Fake Zendesk: same routes for `/tickets` and `/ticket_metrics`. Records keyed by the
   * endpoint name. Three pages of two records each, descending updated_at. Basic auth
   * required (`user`:`secret` → `Basic dXNlcjpzZWNyZXQ=`).
   */
  val testRoutes: Routes[Any, Response] = Routes(
    Method.GET / "api" / "v2" / "tickets" -> handler { (req: Request) =>
      pageResponse(req, recordsKey = "tickets")
    },
    Method.GET / "api" / "v2" / "ticket_metrics" -> handler { (req: Request) =>
      pageResponse(req, recordsKey = "ticket_metrics")
    }
  )

  private def pageResponse(req: Request, recordsKey: String): Response =
    val authOk = req.headers
      .get(Header.Authorization)
      .map(_.renderedValue)
      .contains("Basic dXNlcjpzZWNyZXQ=")
    if !authOk then Response.status(Status.Unauthorized)
    else
      val page = req.url.queryParams.queryParam("page").flatMap(_.toIntOption).getOrElse(1)
      val nextUrl =
        if page < 3 then s""""next":"http://test.invalid/api/v2/$recordsKey?page=${page + 1}""""
        else """"next":null"""
      val day = 14 - page
      val r1  = s"""{"id":${page * 10},"updated_at":"2026-04-${"%02d".format(day)}T20:00:00Z"}"""
      val r2  = s"""{"id":${page * 10 + 1},"updated_at":"2026-04-${"%02d".format(day)}T10:00:00Z"}"""
      Response.json(s"""{"$recordsKey":[$r1,$r2],"links":{$nextUrl}}""")

  private val creds = Zendesk.Creds(subdomain = "test", username = "user", password = "secret")

  def spec = suite("Zendesk")(
    test("ticket_metrics: walks pages, accumulates records, advances cursor") {
      val connector = Zendesk.ticketMetrics(creds)
      for
        dest <- Destination.InMemory.make[Zendesk.State]
        _    <- TestClient.addRoutes(testRoutes)
        _ <- Exlo
               .run(connector, Zendesk.State(""), SinkConfig.testing)
               .provideSome[Client](ZLayer.succeed[Destination[Zendesk.State]](dest))
        all  <- dest.allRecords
        snap <- dest.readState
      yield assertTrue(
        all.length == 6,
        snap.exists(_.cursor == "2026-04-13T20:00:00Z")
      )
    }.provide(TestClient.layer),
    test("tickets: same shape against the /tickets endpoint with shared auth + parser") {
      val connector = Zendesk.tickets(creds)
      for
        dest <- Destination.InMemory.make[Zendesk.State]
        _    <- TestClient.addRoutes(testRoutes)
        _ <- Exlo
               .run(connector, Zendesk.State(""), SinkConfig.testing)
               .provideSome[Client](ZLayer.succeed[Destination[Zendesk.State]](dest))
        all  <- dest.allRecords
        snap <- dest.readState
      yield assertTrue(
        all.length == 6,
        snap.exists(_.cursor == "2026-04-13T20:00:00Z")
      )
    }.provide(TestClient.layer),
    test("ZendeskStreams.make exposes both 'tickets' and 'ticket_metrics'") {
      // Registry-level dispatch is tested at the framework level (StreamRegistrySpec).
      // Here we just confirm Zendesk's registry lists the expected streams.
      val registry = ZendeskStreams.make(creds)
      assertTrue(registry.streams.keySet == Set("tickets", "ticket_metrics"))
    }
  )
