package examples

import exlo.*
import exlo.domain.StreamElement
import zio.*
import zio.http.*
import zio.stream.*

/**
 * Demonstrates using external dependencies with ExloApp.
 *
 * Shows:
 * - Declaring environment requirements (Client & Scope)
 * - Overriding environment to provide dependencies
 * - Making real HTTP calls
 *
 * Run with: sbt "examples/runMain examples.HttpExloConnector"
 */
object HttpExloConnector extends ExloApp:

  override def connectorId: String = "http-connector"

  override def connectorVersion: String = "1.0.0"

  type Env = Client & Scope

  /** Extract function requires HTTP Client and Scope from environment */
  override def extract(state: String): ZStream[Client & Scope, Throwable, StreamElement] =
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
  override def environment = ZLayer.make[Env](
    Client.default,
    Scope.default
  )
