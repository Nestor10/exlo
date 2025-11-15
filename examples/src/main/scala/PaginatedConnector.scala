package examples

import exlo.*
import exlo.domain.Connector
import exlo.domain.StreamElement
import zio.*
import zio.json.*
import zio.stream.*

/**
 * Demonstrates pagination pattern for APIs.
 *
 * Shows:
 * - State parsing and resumability
 * - ZStream.unfoldZIO for pagination
 * - Checkpoint after each page
 *
 * Run with: sbt "examples/runMain examples.PaginatedConnector"
 */
object PaginatedConnector extends ExloApp:

  override def connector: ZIO[Any, Throwable, Connector] =
    ZIO.succeed(
      new Connector:
        def id      = "paginated-connector"
        def version = "1.0.0"
        type Env = Any

        def extract(state: String): ZStream[Any, Throwable, StreamElement] =
          val startPage = if state.isEmpty then 1 else parsePageFromState(state)

          ZStream
            .unfoldZIO(startPage) { page =>
              fetchPage(page).map {
                case Some(data) =>
                  val elements =
                    data.records.map(r => StreamElement.Data(r.toJson)) :+
                      StreamElement.Checkpoint(s"""{"page": ${page + 1}}""")
                  Some((elements, page + 1))

                case None =>
                  None // No more pages
              }
            }
            .flatMap(ZStream.fromIterable)

        def environment = ZLayer.empty
    )

  // Simulated API call
  def fetchPage(page: Int): Task[Option[PageData]] =
    ZIO.succeed {
      if page > 3 then None // Only 3 pages
      else
        Some(
          PageData(
            records = List(
              Record(page * 10 + 1, s"User ${page * 10 + 1}"),
              Record(page * 10 + 2, s"User ${page * 10 + 2}")
            )
          )
        )
    }

  def parsePageFromState(state: String): Int =
    import zio.json.*
    state.fromJson[Map[String, Int]].toOption.flatMap(_.get("page")).getOrElse(1)

  case class PageData(records: List[Record])

  case class Record(id: Int, name: String):
    def toJson: String = s"""{"id": $id, "name": "$name"}"""
