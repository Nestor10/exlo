package examples

import exlo.*
import exlo.domain.Connector
import exlo.domain.StreamElement
import zio.*
import zio.stream.*

/**
 * Simplest possible EXLO connector using ExloApp trait.
 *
 * Demonstrates:
 * - Extending ExloApp trait
 * - Implementing connector method
 * - Emitting Data and Checkpoint elements
 * - No external dependencies
 *
 * Run with: sbt "examples/runMain examples.SimpleExloConnector"
 */
object SimpleExloConnector extends ExloApp:

  override def connector: ZIO[Any, Throwable, Connector] =
    ZIO.succeed(
      new Connector:
        def id      = "simple-connector"
        def version = "1.0.0"
        type Env = Any

        def extract(state: String): ZStream[Any, Throwable, StreamElement] =
          ZStream(
            StreamElement.Data("""{"id": 1, "name": "Alice"}"""),
            StreamElement.Data("""{"id": 2, "name": "Bob"}"""),
            StreamElement.Checkpoint("""{"last_id": 2}""")
          )

        def environment = ZLayer.empty
    )
