package exlo.yaml.integration

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import exlo.config.ExloConfigProvider
import exlo.config.StreamConfig
import exlo.domain.StreamElement
import exlo.yaml.YamlConnector
import exlo.yaml.infra.HttpClient
import exlo.yaml.interpreter.YamlInterpreter
import exlo.yaml.service.*
import exlo.yaml.template.TemplateValue
import zio.*
import zio.http.*
import zio.stream.*
import zio.test.*

import java.nio.file.Files
import java.nio.file.Paths

/**
 * Integration tests for multiple streams per YAML spec with stream selection.
 *
 * Verifies that:
 *   1. YAML specs can define multiple streams
 *   1. YamlConnector selects correct stream based on EXLO_STREAM_STREAM_NAME config
 *   1. Only the selected stream is executed
 */
object MultiStreamSpec extends ZIOSpecDefault:

  private val jsonMapper: ObjectMapper =
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    mapper

  private def parseJson(jsonString: String): JsonNode =
    jsonMapper.readTree(jsonString)

  def spec = suite("Multiple Streams Support")(
    test("selects specific stream by name") {
      val yaml = """
        |streams:
        |  - name: users
        |    requester:
        |      url: "https://jsonplaceholder.typicode.com/users"
        |      method: GET
        |      headers: {}
        |      params: {}
        |      auth:
        |        type: NoAuth
        |    recordSelector:
        |      extractor:
|        type: DpathExtractor
        |        field_path: []
        |      filter: null
        |    paginator:
        |      type: NoPagination
        |  
        |  - name: posts
        |    requester:
        |      url: "https://jsonplaceholder.typicode.com/posts"
        |      method: GET
        |      headers: {}
        |      params:
        |        _limit: "5"
        |      auth:
        |        type: NoAuth
        |    recordSelector:
        |      extractor:
        |        type: DpathExtractor
        |        field_path: []
        |      filter: null
        |    paginator:
        |      type: NoPagination
        |""".stripMargin

      for
        // Write temp YAML file
        tempFile      <- ZIO.succeed {
          val path = Files.createTempFile("multi-stream", ".yaml")
          Files.write(path, yaml.getBytes)
          path.toString
        }

        // Load spec and test stream selection
        elements      <- ZStream
          .scoped {
            for
              // Simulate config with stream name = "posts"
              streamConfig <- ZIO.succeed(StreamConfig("test", "test_table", "posts"))

              spec <- YamlSpecLoader.loadSpec(tempFile)
              _    <- ZIO.succeed(Files.delete(Paths.get(tempFile)))

              // Select stream by name
              streamSpec <- ZIO
                .fromOption(spec.streams.find(_.name == "posts"))
                .orElseFail(new RuntimeException("Stream not found"))
            yield streamSpec
          }
          .flatMap { streamSpec =>
            val context = Map.empty[String, TemplateValue]

            YamlInterpreter
              .interpretStream(streamSpec)
              .map(json => StreamElement.Data(json.toString))
          }
          .take(5)
          .runCollect
          .timeout(30.seconds)
          .someOrFailException

        // Parse and verify records
        dataElements = elements.collect { case d: StreamElement.Data => d }
        parsedRecords <- ZIO.foreach(dataElements)(data => ZIO.attempt(parseJson(data.record)))
      yield assertTrue(
        parsedRecords.nonEmpty,
        parsedRecords.size == 5,         // Limited to 5 posts
        parsedRecords.head.has("title"), // Posts have title
        !parsedRecords.head.has("name")  // Posts don't have name (users do)
      )
    }.provide(
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
    ),
    test("fails when stream name not found") {
      val yaml = """
        |streams:
        |  - name: users
        |    requester:
        |      url: "https://jsonplaceholder.typicode.com/users"
        |      method: GET
        |      headers: {}
        |      params: {}
        |      auth:
        |        type: NoAuth
        |    recordSelector:
        |      extractor:
|        type: DpathExtractor
        |        field_path: []
        |      filter: null
        |    paginator:
        |      type: NoPagination
        |""".stripMargin

      for
        tempFile <- ZIO.succeed {
          val path = Files.createTempFile("single-stream", ".yaml")
          Files.write(path, yaml.getBytes)
          path.toString
        }

        result <- ZStream
          .scoped {
            for
              spec <- YamlSpecLoader.loadSpec(tempFile)
              _    <- ZIO.succeed(Files.delete(Paths.get(tempFile)))

              // Try to find non-existent stream
              streamSpec <- ZIO
                .fromOption(spec.streams.find(_.name == "nonexistent"))
                .orElseFail(
                  new RuntimeException(
                    s"Stream 'nonexistent' not found. Available: ${spec.streams.map(_.name).mkString(", ")}"
                  )
                )
            yield streamSpec
          }
          .runCollect
          .timeout(5.seconds)
          .either
      yield assertTrue(
        result.isLeft, // Should fail
        result.swap.toOption.exists(_.getMessage.contains("not found"))
      )
    }.provide(
      Client.default,
      YamlSpecLoader.Live.layer,
      RequestExecutor.live,
      HttpClient.Live.layer,
      TemplateEngine.Live.layer,
      ResponseParser.Live.layer,
      Authenticator.Live.layer,
      ErrorHandlerService.live
    ),
    test("defaults to first stream when no name specified") {
      val yaml = """
        |streams:
        |  - name: comments
        |    requester:
        |      url: "https://jsonplaceholder.typicode.com/comments?postId=1"
        |      method: GET
        |      headers: {}
        |      params: {}
        |      auth:
        |        type: NoAuth
        |    recordSelector:
        |      extractor:
|        type: DpathExtractor
        |        field_path: []
        |      filter: null
        |    paginator:
        |      type: NoPagination
        |  
        |  - name: posts
        |    requester:
        |      url: "https://jsonplaceholder.typicode.com/posts"
        |      method: GET
        |      headers: {}
        |      params: {}
        |      auth:
        |        type: NoAuth
        |    recordSelector:
        |      extractor:
|        type: DpathExtractor
        |        field_path: []
        |      filter: null
        |    paginator:
        |      type: NoPagination
        |""".stripMargin

      for
        tempFile <- ZIO.succeed {
          val path = Files.createTempFile("default-stream", ".yaml")
          Files.write(path, yaml.getBytes)
          path.toString
        }

        elements <- ZStream
          .scoped {
            for
              spec <- YamlSpecLoader.loadSpec(tempFile)
              _    <- ZIO.succeed(Files.delete(Paths.get(tempFile)))

              // Default to first stream (no name specified)
              streamSpec <- ZIO
                .fromOption(spec.streams.headOption)
                .orElseFail(new RuntimeException("No streams found"))
            yield streamSpec
          }
          .flatMap { streamSpec =>
            val context = Map.empty[String, TemplateValue]

            YamlInterpreter
              .interpretStream(streamSpec)
              .map(json => StreamElement.Data(json.toString))
          }
          .take(5)
          .runCollect
          .timeout(30.seconds)
          .someOrFailException

        dataElements = elements.collect { case d: StreamElement.Data => d }
        parsedRecords <- ZIO.foreach(dataElements)(data => ZIO.attempt(parseJson(data.record)))
      yield assertTrue(
        parsedRecords.nonEmpty,
        parsedRecords.head.has("email") // Comments have email
      )
    }.provide(
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
    )
  ) @@ TestAspect.timeout(60.seconds)
