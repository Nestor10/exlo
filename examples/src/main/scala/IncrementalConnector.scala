package examples

import exlo.*
import exlo.domain.Connector
import exlo.domain.StreamElement
import zio.*
import zio.json.*
import zio.stream.*

import java.time.Instant

/**
 * Demonstrates incremental extraction pattern.
 *
 * Shows:
 * - Timestamp-based incremental sync
 * - Fetching only new data since last checkpoint
 * - Structured state with timestamps
 *
 * Run with: sbt "examples/runMain examples.IncrementalConnector"
 */
object IncrementalConnector extends ExloApp:

  override def connector: ZIO[Any, Throwable, Connector] =
    ZIO.succeed(
      new Connector:
        def id      = "incremental-connector"
        def version = "1.0.0"
        type Env = Any

        def extract(state: String): ZStream[Any, Throwable, StreamElement] =
          val lastTimestamp =
            if state.isEmpty then Instant.parse("2024-01-01T00:00:00Z")
            else parseTimestampFromState(state)

          for
            _ <- ZStream.fromZIO(ZIO.logInfo(s"Fetching records since $lastTimestamp"))

            // Simulate fetching records modified after timestamp
            records <- ZStream.fromZIO(fetchRecordsSince(lastTimestamp))

            _ <- ZStream.fromIterable(records).map(r => StreamElement.Data(r.toJson))

            // Save latest timestamp as checkpoint
            latestTimestamp = records.map(_.modifiedAt).maxOption.getOrElse(lastTimestamp)
          yield StreamElement.Checkpoint(s"""{"lastTimestamp": "$latestTimestamp"}""")

        def environment = ZLayer.empty
    )

  def fetchRecordsSince(timestamp: Instant): Task[List[Record]] =
    ZIO.succeed(
      List(
        Record(1, "Alice", timestamp.plusSeconds(100)),
        Record(2, "Bob", timestamp.plusSeconds(200)),
        Record(3, "Charlie", timestamp.plusSeconds(300))
      )
    )

  def parseTimestampFromState(state: String): Instant =
    import zio.json.*
    state
      .fromJson[Map[String, String]]
      .toOption
      .flatMap(_.get("lastTimestamp"))
      .map(Instant.parse)
      .getOrElse(Instant.parse("2024-01-01T00:00:00Z"))

  case class Record(id: Int, name: String, modifiedAt: Instant):
    def toJson: String = s"""{"id": $id, "name": "$name", "modified_at": "$modifiedAt"}"""
