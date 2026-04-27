package examples.zendesk

import exlo.domain.Connector
import exlo.http.HttpExtract
import zio.*
import zio.http.*
import zio.json.*
import zio.json.ast.Json

/**
 * Zendesk ticket_metrics — cursor pagination via the `links.next` URL pattern, basic auth.
 *
 * Mirrors `context/connectors/zendesk/zendesk.py` minus the lookback-window stop condition
 * (which would require capturing the run-start cursor; left as a TODO for a real port).
 *
 * State is just `updated_at` cursor — the high-water mark of records seen.
 */
object ZendeskConnector:

  final case class State(cursor: String)

  /** Parsed Zendesk response. We keep records as raw JSON strings so the framework stays opaque. */
  final case class Page(
      records: List[Json],
      oldestUpdated: Option[String],
      newestUpdated: Option[String],
      nextUrl: Option[String]
  )

  private val parsePage: Response => ZIO[Any, Throwable, Page] = resp =>
    resp.body.asString.flatMap { body =>
      ZIO
        .fromEither(body.fromJson[Json])
        .mapError(e => new RuntimeException(s"json parse: $e"))
        .flatMap {
          case Json.Obj(fields) =>
            val tickets =
              fields.find(_._1 == "ticket_metrics").map(_._2) match
                case Some(Json.Arr(items)) => items.toList
                case _                     => Nil
            val updatedAts = tickets.flatMap {
              case Json.Obj(rec) =>
                rec.find(_._1 == "updated_at").map(_._2) match
                  case Some(Json.Str(s)) => Some(s)
                  case _                 => None
              case _ => None
            }
            val nextUrl =
              fields.find(_._1 == "links").map(_._2) match
                case Some(Json.Obj(links)) =>
                  links.find(_._1 == "next").map(_._2) match
                    case Some(Json.Str(s)) => Some(s)
                    case _                 => None
                case _ => None
            ZIO.succeed(
              Page(
                records       = tickets,
                oldestUpdated = if updatedAts.isEmpty then None else Some(updatedAts.min),
                newestUpdated = if updatedAts.isEmpty then None else Some(updatedAts.max),
                nextUrl       = nextUrl
              )
            )
          case _ => ZIO.fail(new RuntimeException("expected JSON object"))
        }
    }

  /**
   * Build the connector for a given subdomain and basic-auth credentials.
   */
  def make(
      subdomain: String,
      username: String,
      password: String
  ): Connector[State, Client, Throwable] =
    val baseUrl = s"https://$subdomain.zendesk.com/api/v2/ticket_metrics"

    HttpExtract[State]
      .request { _ =>
        Request.get(URL.decode(s"$baseUrl?page%5Bsize%5D=100&sort=-updated_at").toOption.get)
      }
      .parse(parsePage)
      .records(p => Chunk.fromIterable(p.records.map(_.toJson)))
      .nextRequest { (_, page) =>
        page.nextUrl.flatMap(url => URL.decode(url).toOption.map(Request.get(_)))
      }
      .advance { (state, page) =>
        val newest = page.newestUpdated.getOrElse(state.cursor)
        if newest > state.cursor then state.copy(cursor = newest) else state
      }
      .basicAuth(username, password)
      .header(Header.ContentType(MediaType.application.json))
      .toConnector("zendesk-ticket-metrics", "0.1.0")
