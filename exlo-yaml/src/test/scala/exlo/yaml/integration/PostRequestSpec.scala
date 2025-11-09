package exlo.yaml.integration

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import exlo.yaml.infra.HttpClient
import exlo.yaml.interpreter.YamlInterpreter
import exlo.yaml.service.*
import exlo.yaml.spec.*
import exlo.yaml.template.TemplateValue
import zio.*
import zio.http.*
import zio.test.*

/**
 * Integration tests for POST requests with body templates.
 *
 * Uses JSONPlaceholder POST endpoints for testing.
 */
object PostRequestSpec extends ZIOSpecDefault:

  private val jsonMapper: ObjectMapper =
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    mapper

  def spec = suite("POST Request Support")(
    test("sends POST request with JSON body") {
      val spec = StreamSpec(
        name = "create-post",
        requester = Requester(
          url = "https://jsonplaceholder.typicode.com/posts",
          method = HttpMethod.POST,
          headers = Map("Content-Type" -> "application/json"),
          params = Map.empty,
          auth = Auth.NoAuth,
          body = Some("""{
            "title": "Test Post",
            "body": "This is a test post",
            "userId": 1
          }""")
        ),
        recordSelector = RecordSelector(
          extractor = Extractor.DPath(List()),
          filter = None
        ),
        paginator = PaginationStrategy.NoPagination
      )

      val context = Map.empty[String, TemplateValue]

      for records <- YamlInterpreter
          .interpretStream(spec, context)
          .runCollect
          .timeout(30.seconds)
          .someOrFailException
      yield assertTrue(
        records.nonEmpty,
        records.head.has("id"),
        records.head.get("title").asText().contains("Test Post"),
        records.head.get("body").asText().contains("This is a test post"),
        records.head.get("userId").asInt() == 1
      )
    },
    test("renders body template with variables") {
      val spec = StreamSpec(
        name = "create-post-templated",
        requester = Requester(
          url = "https://jsonplaceholder.typicode.com/posts",
          method = HttpMethod.POST,
          headers = Map("Content-Type" -> "application/json"),
          params = Map.empty,
          auth = Auth.NoAuth,
          body = Some("""{
            "title": "{{ post_title }}",
            "body": "{{ post_body }}",
            "userId": {{ user_id }}
          }""")
        ),
        recordSelector = RecordSelector(
          extractor = Extractor.DPath(List()),
          filter = None
        ),
        paginator = PaginationStrategy.NoPagination
      )

      val context = Map[String, TemplateValue](
        "post_title" -> TemplateValue.Str("Templated Title"),
        "post_body"  -> TemplateValue.Str("Templated body content"),
        "user_id"    -> TemplateValue.Num(42)
      )

      for records <- YamlInterpreter
          .interpretStream(spec, context)
          .runCollect
          .timeout(30.seconds)
          .someOrFailException
      yield assertTrue(
        records.nonEmpty,
        records.head.get("title").asText().contains("Templated Title"),
        records.head.get("body").asText().contains("Templated body content"),
        records.head.get("userId").asInt() == 42
      )
    },
    test("sends POST with nested JSON structure") {
      val spec = StreamSpec(
        name = "create-nested",
        requester = Requester(
          url = "https://jsonplaceholder.typicode.com/posts",
          method = HttpMethod.POST,
          headers = Map("Content-Type" -> "application/json"),
          params = Map.empty,
          auth = Auth.NoAuth,
          body = Some("""{
            "title": "Nested Structure",
            "body": "Testing nested data",
            "userId": 1,
            "metadata": {
              "source": "{{ source }}",
              "timestamp": "{{ timestamp }}"
            }
          }""")
        ),
        recordSelector = RecordSelector(
          extractor = Extractor.DPath(List()),
          filter = None
        ),
        paginator = PaginationStrategy.NoPagination
      )

      val context = Map[String, TemplateValue](
        "source"    -> TemplateValue.Str("integration-test"),
        "timestamp" -> TemplateValue.Str("2025-11-06T22:00:00Z")
      )

      for records <- YamlInterpreter
          .interpretStream(spec, context)
          .runCollect
          .timeout(30.seconds)
          .someOrFailException
      yield assertTrue(
        records.nonEmpty,
        records.head.get("title").asText().contains("Nested Structure")
      )
    },
    test("GET request still works without body") {
      val spec = StreamSpec(
        name = "get-users",
        requester = Requester(
          url = "https://jsonplaceholder.typicode.com/users/1",
          method = HttpMethod.GET,
          headers = Map.empty,
          params = Map.empty,
          auth = Auth.NoAuth,
          body = None
        ),
        recordSelector = RecordSelector(
          extractor = Extractor.DPath(List()),
          filter = None
        ),
        paginator = PaginationStrategy.NoPagination
      )

      val context = Map.empty[String, TemplateValue]

      for records <- YamlInterpreter
          .interpretStream(spec, context)
          .runCollect
          .timeout(30.seconds)
          .someOrFailException
      yield assertTrue(
        records.nonEmpty,
        records.head.get("id").asInt() == 1,
        records.head.has("name")
      )
    }
  ).provide(
    Client.default,
    HttpClient.Live.layer,
    TemplateEngine.Live.layer,
    ResponseParser.Live.layer,
    Authenticator.Live.layer,
    ErrorHandlerService.live,
    RequestExecutor.live
  ) @@ TestAspect.timeout(60.seconds)
