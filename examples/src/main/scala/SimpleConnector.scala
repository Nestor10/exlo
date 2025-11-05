package examples

import zio.*
import zio.stream.*

/**
 * Simplest possible EXLO connector with hardcoded data.
 *
 * Demonstrates:
 * - Basic connector pattern
 * - Emitting Data and Checkpoint elements
 * - No external dependencies
 *
 * Run with: sbt "examples/runMain examples.SimpleConnector"
 */
object SimpleConnector extends ZIOAppDefault:

  def run =
    ZIO.logInfo("Simple connector - emitting hardcoded data") *>
      extract("")
        .tap(elem => ZIO.logInfo(s"Emitted: $elem"))
        .runDrain

  def extract(state: String): ZStream[Any, Throwable, StreamElement] =
    ZStream(
      StreamElement.Data("""{"id": 1, "name": "Alice"}"""),
      StreamElement.Data("""{"id": 2, "name": "Bob"}"""),
      StreamElement.Checkpoint("""{"last_id": 2}""")
    )
