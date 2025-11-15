package exlo.yaml.integration

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import exlo.config.StreamConfig
import exlo.domain.StreamElement
import exlo.infra.IcebergCatalog
import exlo.yaml.YamlConnector
import exlo.yaml.infra.HttpClient
import exlo.yaml.interpreter.YamlInterpreter
import exlo.yaml.service.*
import zio.*
import zio.http.Client
import zio.stream.*
import zio.test.*

/**
 * End-to-end integration test for YAML connectors with real API.
 *
 * Tests the full pipeline:
 * 1. Load YAML connector spec (NewsAPI with ApiKeyAuthenticator)
 * 2. Read API key from environment (NEWSAPI_API_KEY - required)
 * 3. Execute HTTP requests against real NewsAPI
 * 4. Parse JSON responses
 * 5. Verify connector works end-to-end
 *
 * REQUIRED: NEWSAPI_API_KEY environment variable must be set.
 * Get a free key at: https://newsapi.org/register
 *
 * To run this test:
 *   cp .env.example .env
 *   # Edit .env and add your API key
 *   sbt "exloYaml/testOnly exlo.yaml.integration.EndToEndIntegrationSpec"
 *
 * The sbt-dotenv plugin automatically loads .env file.
 *
 * This validates that all the pieces work together: YAML parsing,
 * ApiKeyAuthenticator, HTTP execution, and JSON parsing.
 */
object EndToEndIntegrationSpec extends ZIOSpecDefault:

  private val jsonMapper: ObjectMapper =
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    mapper

  def spec = suite("End-to-End Integration")(
    test("runs NewsAPI connector with ApiKeyAuthenticator") {
      // Read API key from sys.env (loaded by sbt-dotenv)
      val apiKey       = sys.env.getOrElse(
        "NEWSAPI_API_KEY",
        throw new RuntimeException(
          "NEWSAPI_API_KEY environment variable not set in .env file. " +
            "Copy .env.example to .env and add your API key."
        )
      )
      val configString = s"""{"api_key": "$apiKey", "country": "us", "category": "general"}"""

      (for
        // Load NewsAPI YAML spec from test resources
        specPath <- ZIO.succeed(getClass.getResource("/newsapi-connector.yaml").getPath)
        spec     <- YamlSpecLoader.loadSpec(specPath)

        // Get the top_headlines stream
        streamSpec    <- ZIO
          .fromOption(spec.streams.find(_.name == "top_headlines"))
          .orElseFail(new RuntimeException("Stream 'top_headlines' not found"))

        // Execute the stream
        elements      <- YamlInterpreter
          .interpretStream(streamSpec)
          .take(10) // Take first 10 articles
          .runCollect
          .timeout(30.seconds)
          .someOrFailException

        // Parse records to verify structure
        parsedRecords <- ZIO.foreach(elements)(json => ZIO.attempt(jsonMapper.readTree(json.toString)))
      yield assertTrue(
        parsedRecords.nonEmpty,
        parsedRecords.size <= 10,
        parsedRecords.forall(_.has("title")), // Articles have title
        parsedRecords.forall(_.has("url"))    // Articles have URL
      )).provide(
        Client.default,
        YamlSpecLoader.Live.layer,
        HttpClient.Live.layer,
        TemplateEngine.Live.layer,
        ResponseParser.Live.layer,
        Authenticator.Live.layer,
        ErrorHandlerService.live,
        RateLimiter.Live.layer,
        RequestExecutor.live,
        ZLayer.fromZIO(
          ZIO.attempt {
            val configJson = jsonMapper.readTree(configString)
            exlo.yaml.domain.ConnectorConfig(configJson)
          }
        ),
        RuntimeContext.Live.layer("{}")
      )
    },
    test("handles JSONPlaceholder API without authentication") {
      // Simple test using public API (no credentials needed)
      val yaml =
        """
          |version: 1.0.0
          |streams:
          |  - name: users
          |    requester:
          |      url: https://jsonplaceholder.typicode.com/users
          |      method: GET
          |      auth:
          |        type: NoAuth
          |      params:
          |        _limit: "5"
          |    recordSelector:
          |      extractor:
          |        type: DpathExtractor
          |        field_path: []
          |    paginator:
          |      type: NoPagination
          |""".stripMargin

      for
        tempFile <- ZIO.attempt(java.nio.file.Files.createTempFile("jsonplaceholder-test", ".yaml"))
        _        <- ZIO.attempt(java.nio.file.Files.writeString(tempFile, yaml))

        _ <- TestSystem.putEnv("EXLO_CONNECTOR_CONFIG", "{}")

        spec <- YamlSpecLoader.loadSpec(tempFile.toString)
        _    <- ZIO.attempt(java.nio.file.Files.delete(tempFile))

        streamSpec <- ZIO
          .fromOption(spec.streams.find(_.name == "users"))
          .orElseFail(new RuntimeException("Stream not found"))

        elements <- YamlInterpreter
          .interpretStream(streamSpec)
          .take(5)
          .runCollect
          .timeout(30.seconds)
          .someOrFailException

        parsedRecords <- ZIO.foreach(elements)(json => ZIO.attempt(jsonMapper.readTree(json.toString)))
      yield assertTrue(
        parsedRecords.nonEmpty,
        parsedRecords.size == 5,
        parsedRecords.forall(_.has("email")), // Users have email
        parsedRecords.forall(_.has("name"))   // Users have name
      )
    },
    test("emits checkpoint records with state") {
      val apiKey       = sys.env.getOrElse(
        "NEWSAPI_API_KEY",
        throw new RuntimeException(
          "NEWSAPI_API_KEY environment variable not set in .env file. " +
            "Copy .env.example to .env and add your API key."
        )
      )
      val configString = s"""{"api_key": "$apiKey", "country": "us", "category": "general"}"""

      (for
        // Set environment for YamlConnector (reads from config)
        _ <- TestSystem.putEnv("EXLO_CONNECTOR_CONFIG", configString)
        _ <- TestSystem.putEnv("EXLO_STREAM_NAMESPACE", "test")
        _ <- TestSystem.putEnv("EXLO_STREAM_STREAMNAME", "top_headlines")
        _ <- TestSystem.putEnv("EXLO_STREAM_TABLENAME", "top_headlines")
        _ <- TestSystem.putEnv(
          "EXLO_CONNECTOR_SPEC_FILE_PATH",
          getClass.getResource("/newsapi-connector.yaml").getPath
        )

        // Get connector from YamlConnector
        yamlConnector <- YamlConnector.connector

        // Extract stream elements using YamlConnector (Data + Checkpoints)
        elements <- yamlConnector
          .extract("")
          .take(2500) // Take enough to trigger checkpoint emission (batched every 1000 records)
          .runCollect
          .timeout(30.seconds)
          .someOrFailException
          .provide(yamlConnector.environment)

        dataElements       = elements.collect { case d: StreamElement.Data => d }
        checkpointElements = elements.collect { case c: StreamElement.Checkpoint => c }

        // Parse checkpoint state
        checkpointStates <- ZIO.foreach(checkpointElements) { checkpoint =>
          ZIO.attempt(jsonMapper.readTree(checkpoint.state))
        }
      yield assertTrue(
        dataElements.nonEmpty,              // Has data records
        checkpointElements.nonEmpty,        // Has checkpoint records
        checkpointElements.size >= 1,       // At least one checkpoint (after 1000 records)
        checkpointStates.forall(_.isObject) // Checkpoints are valid JSON objects
      ))
    }
  ).provide(
    Client.default,
    YamlSpecLoader.Live.layer,
    HttpClient.Live.layer,
    TemplateEngine.Live.layer,
    ResponseParser.Live.layer,
    Authenticator.Live.layer,
    ErrorHandlerService.live,
    RateLimiter.Live.layer,
    RequestExecutor.live,
    RuntimeContext.Stub.layer
  ) @@ TestAspect.timeout(60.seconds) @@ TestAspect.sequential
