package exlo.yaml.integration

import exlo.yaml.interpreter.YamlInterpreter
import exlo.yaml.service.*
import exlo.yaml.spec.*
import io.circe.Json
import zio.*
import zio.http.*
import zio.test.*

/** Integration tests for POST requests with body templates.
  *
  * Uses JSONPlaceholder POST endpoints for testing.
  */
object PostRequestSpec extends ZIOSpecDefault:

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

      val context = Map.empty[String, Any]

      for
        records <- YamlInterpreter
          .interpretStream(spec, context)
          .runCollect
          .timeout(30.seconds)
          .someOrFailException
      yield assertTrue(
        records.nonEmpty,
        records.head.hcursor.downField("id").as[Int].isRight,
        records.head.hcursor.downField("title").as[String].contains("Test Post"),
        records.head.hcursor.downField("body").as[String].contains("This is a test post"),
        records.head.hcursor.downField("userId").as[Int].contains(1)
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

      val context = Map(
        "post_title" -> "Templated Title",
        "post_body" -> "Templated body content",
        "user_id" -> 42
      )

      for
        records <- YamlInterpreter
          .interpretStream(spec, context)
          .runCollect
          .timeout(30.seconds)
          .someOrFailException
      yield assertTrue(
        records.nonEmpty,
        records.head.hcursor.downField("title").as[String].contains("Templated Title"),
        records.head.hcursor.downField("body").as[String].contains("Templated body content"),
        records.head.hcursor.downField("userId").as[Int].contains(42)
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

      val context = Map(
        "source" -> "integration-test",
        "timestamp" -> "2025-11-06T22:00:00Z"
      )

      for
        records <- YamlInterpreter
          .interpretStream(spec, context)
          .runCollect
          .timeout(30.seconds)
          .someOrFailException
      yield assertTrue(
        records.nonEmpty,
        records.head.hcursor.downField("title").as[String].contains("Nested Structure")
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

      val context = Map.empty[String, Any]

      for
        records <- YamlInterpreter
          .interpretStream(spec, context)
          .runCollect
          .timeout(30.seconds)
          .someOrFailException
      yield assertTrue(
        records.nonEmpty,
        records.head.hcursor.downField("id").as[Int].contains(1),
        records.head.hcursor.downField("name").as[String].isRight
      )
    }
  ).provide(
    Client.default,
    HttpClient.Live.layer,
    TemplateEngine.Live.layer,
    ResponseParser.Live.layer,
    Authenticator.Live.layer
  ) @@ TestAspect.timeout(60.seconds)
