package examples

import exlo.*
import exlo.domain.Connector
import exlo.domain.StreamElement
import zio.*
import zio.http.*
import zio.stream.*

/**
 * Demonstrates using external dependencies with ExloApp.
 *
 * Shows:
 * - Declaring environment requirements (Client & Scope)
 * - Providing dependencies via environment
 * - Making real HTTP calls
 *
 * Run with: sbt "examples/runMain examples.HttpExloConnector"
 */
object HttpExloConnector extends ExloApp:

  override def connector: ZIO[Any, Throwable, Connector] =
    ZIO.succeed(
      new Connector:
        def id      = "http-connector"
        def version = "1.0.0"
        type Env = Client & Scope

        /** Extract function requires HTTP Client and Scope from environment */
        def extract(state: String): ZStream[Client & Scope, Throwable, StreamElement] =
          for
            client <- ZStream.service[Client]

            // Example: Fetch from a public API
            response <- ZStream.fromZIO(
              client
                .request(Request.get("https://jsonplaceholder.typicode.com/users"))
                .flatMap(_.body.asString)
            )

            _ <- ZStream.fromZIO(ZIO.logInfo(s"Fetched ${response.length} bytes"))
          yield StreamElement.Checkpoint("""{"fetched": true}""")

        /** Provide HTTP client and Scope */
        def environment = ZLayer.make[Env](
          Client.default,
          Scope.default
        )
    )
