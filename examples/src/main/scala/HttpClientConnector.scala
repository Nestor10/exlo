package examples

import zio.*
import zio.http.*
import zio.stream.*

/**
 * Demonstrates using external dependencies (HTTP client).
 *
 * Shows:
 * - Declaring environment requirements (Client)
 * - Providing dependencies via layers
 * - Making real HTTP calls
 *
 * Run with: sbt "examples/runMain examples.HttpClientConnector"
 */
object HttpClientConnector extends ZIOAppDefault:

  def run =
    extract("")
      .tap(elem => ZIO.logInfo(s"Emitted: $elem"))
      .runDrain
      .provide(Client.default, Scope.default)

  /** Extract function requires HTTP Client from environment */
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
