package examples

import exlo.*
import exlo.domain.StreamElement
import zio.*
import zio.stream.*

/**
 * Simplest possible EXLO connector using ExloApp trait.
 *
 * Demonstrates:
 * - Extending ExloApp trait
 * - Implementing required methods
 * - Emitting Data and Checkpoint elements
 * - No external dependencies
 *
 * Run with: sbt "examples/runMain examples.SimpleExloConnector"
 */
object SimpleExloConnector extends ExloApp:

  override def connectorId: String = "simple-connector"

  override def connectorVersion: String = "1.0.0"

  type Env = Any

  override def extract(state: String): ZStream[Any, Throwable, StreamElement] =
    ZStream(
      StreamElement.Data("""{"id": 1, "name": "Alice"}"""),
      StreamElement.Data("""{"id": 2, "name": "Bob"}"""),
      StreamElement.Checkpoint("""{"last_id": 2}""")
    )

  override def environment = ZLayer.empty
