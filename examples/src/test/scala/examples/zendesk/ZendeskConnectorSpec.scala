package examples.zendesk

import exlo.Exlo
import exlo.runtime.{Destination, SinkConfig, Telemetry}
import zio.*
import zio.http.*
import zio.test.*

import java.time.Instant

object ZendeskConnectorSpec extends ZIOSpecDefault:

  /**
   * Fake Zendesk routes:
   *   - `/api/v2/incremental/tickets?start_time=X&end_time=Y` — returns 2 records inside
   *     the window with `next_page` chaining 2 pages, then null.
   *   - `/api/v2/ticket_metrics?sort=-updated_at` — returns 3 pages descending, with
   *     `links.next` chaining.
   * Both require `Basic dXNlcjpzZWNyZXQ=` (user:secret).
   */
  val testRoutes: Routes[Any, Response] = Routes(
    Method.GET / "api" / "v2" / "incremental" / "tickets" -> handler { (req: Request) =>
      val authOk = req.headers
        .get(Header.Authorization)
        .map(_.renderedValue)
        .contains("Basic dXNlcjpzZWNyZXQ=")
      if !authOk then Response.status(Status.Unauthorized)
      else
        val startTime = req.url.queryParams.queryParam("start_time").flatMap(_.toLongOption).getOrElse(0L)
        val page      = req.url.queryParams.queryParam("page").flatMap(_.toIntOption).getOrElse(1)
        val nextPage =
          if page < 2 then
            s"http://test.invalid/api/v2/incremental/tickets?start_time=$startTime&page=${page + 1}"
          else
            null

        val r1 = s"""{"id":${page * 100 + startTime},"updated_at":"2026-04-${"%02d".format(10 + page)}T05:00:00Z"}"""
        val r2 = s"""{"id":${page * 100 + startTime + 1},"updated_at":"2026-04-${"%02d".format(10 + page)}T15:00:00Z"}"""
        val nextField =
          if nextPage != null then s""""next_page":"$nextPage""""
          else """"next_page":null"""
        Response.json(s"""{"tickets":[$r1,$r2],$nextField,"end_of_stream":${nextPage == null}}""")
    },
    Method.GET / "api" / "v2" / "ticket_metrics" -> handler { (req: Request) =>
      val authOk = req.headers
        .get(Header.Authorization)
        .map(_.renderedValue)
        .contains("Basic dXNlcjpzZWNyZXQ=")
      if !authOk then Response.status(Status.Unauthorized)
      else
        val page = req.url.queryParams.queryParam("page").flatMap(_.toIntOption).getOrElse(1)
        val nextUrl =
          if page < 3 then s""""next":"http://test.invalid/api/v2/ticket_metrics?page=${page + 1}""""
          else """"next":null"""
        // sort=-updated_at: page 1 newest, page 3 oldest. Day decreases with page.
        val day = 14 - page
        val r1  = s"""{"id":${page * 10},"updated_at":"2026-04-${"%02d".format(day)}T20:00:00Z"}"""
        val r2  = s"""{"id":${page * 10 + 1},"updated_at":"2026-04-${"%02d".format(day)}T10:00:00Z"}"""
        Response.json(s"""{"ticket_metrics":[$r1,$r2],"links":{$nextUrl}}""")
    }
  )

  private val creds = Zendesk.Creds(subdomain = "test", username = "user", password = "secret")

  /** A small `from` so dailyWindows generates a tractable number for tests. */
  private val testFrom: Long = Instant.parse("2026-04-25T00:00:00Z").getEpochSecond

  def spec = suite("Zendesk")(
    test("incrementalConnector: walks per-window pages, marks each window done") {
      val connector = Zendesk.incrementalConnector(
        creds, "zendesk_tickets", "0.1.0", "tickets",
        from = testFrom, parallelism = 4
      )
      for
        dest <- Destination.InMemory.make[Zendesk.State]
        _    <- TestClient.addRoutes(testRoutes)
        _ <- Exlo
               .run(connector.toConnector, Zendesk.State.zero, SinkConfig.testing)
               .provideSome[Client](ZLayer.succeed[Destination[Zendesk.State]](dest) ++ Telemetry.noop)
        all  <- dest.allRecords
        snap <- dest.readState
      yield assertTrue(
        // Each daily slice produces 2 pages × 2 records = 4 records. Number of slices
        // depends on test execution time; we just check that records arrived AND state
        // tracked at least one completed window.
        all.nonEmpty,
        all.length % 4 == 0,                               // each slice contributes a multiple of 4
        snap.exists(_.done.nonEmpty)                       // at least one window marked done
      )
    }.provide(TestClient.layer),
    test("slicedCursorConnector: descending walk, stop when oldest predates slice.start") {
      val connector = Zendesk.slicedCursorConnector(
        creds, "zendesk_ticket_metrics", "0.1.0", "ticket_metrics",
        from = testFrom
      )
      for
        dest <- Destination.InMemory.make[Zendesk.State]
        _    <- TestClient.addRoutes(testRoutes)
        _ <- Exlo
               .run(connector.toConnector, Zendesk.State.zero, SinkConfig.testing)
               .provideSome[Client](ZLayer.succeed[Destination[Zendesk.State]](dest) ++ Telemetry.noop)
        all  <- dest.allRecords
        snap <- dest.readState
      yield assertTrue(
        // testFrom = 2026-04-25 in unix seconds; slice = (testFrom, now). All test records
        // are in 2026-04-(11..13) — older than slice.start. The first page's oldest
        // record is older than slice.start, so we stop after page 1: 2 records emitted.
        all.length == 2,
        // The slice was completed (terminal stop counts as done).
        snap.exists(_.done.size == 1)
      )
    }.provide(TestClient.layer),
    test("ZendeskStreams.make exposes both 'tickets' and 'ticket_metrics'") {
      val registry = ZendeskStreams.make(creds)
      assertTrue(registry.streams.keySet == Set("tickets", "ticket_metrics"))
    },
    test("daily window helpers: non-overlapping, tail can be partial") {
      val from = 0L
      val to   = 86400L * 2 + 3600L      // 2 days + 1 hour
      val ws   = Zendesk.dailyWindows(from, to)
      assertTrue(
        ws.length == 3,
        ws(0) == Zendesk.Window(0, 86400),
        ws(1) == Zendesk.Window(86400, 172800),
        ws(2) == Zendesk.Window(172800, 176400) // partial 1-hour tail
      )
    }
  )
