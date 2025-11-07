package exlo.yaml.integration

import exlo.yaml.interpreter.YamlInterpreter
import exlo.yaml.service.*
import exlo.yaml.spec.*
import zio.*
import zio.http.Client
import zio.test.*

/**
 * Integration tests for YAML interpreter with real HTTP calls.
 *
 * Uses JSONPlaceholder (https://jsonplaceholder.typicode.com/), a free fake
 * REST API for testing.
 */
object YamlInterpreterIntegrationSpec extends ZIOSpecDefault:

  def spec = suite("YamlInterpreter Integration")(
    test("extracts users from JSONPlaceholder API") {
      // Build a simple spec for fetching users
      val spec = StreamSpec(
        name = "users",
        requester = Requester(
          url = "https://jsonplaceholder.typicode.com/users",
          method = HttpMethod.GET
        ),
        recordSelector = RecordSelector(
          extractor = Extractor.DPath(List()) // Root is already an array
        ),
        paginator = PaginationStrategy.NoPagination
      )

      for
        // Interpret the stream
        records <- YamlInterpreter
          .interpretStream(spec, Map.empty)
          .runCollect

        // Assertions
        firstUser = records.head
      yield assertTrue(
        records.nonEmpty,
        records.length == 10, // JSONPlaceholder has 10 users
        records.head.asObject.isDefined,
        firstUser.asObject.flatMap(_("id")).flatMap(_.asNumber).isDefined,
        firstUser.asObject.flatMap(_("name")).flatMap(_.asString).isDefined,
        firstUser.asObject.flatMap(_("email")).flatMap(_.asString).isDefined
      )
    },
    test("extracts nested posts data") {
      // JSONPlaceholder posts are at root level, but let's test extraction
      val spec = StreamSpec(
        name = "posts",
        requester = Requester(
          url = "https://jsonplaceholder.typicode.com/posts",
          method = HttpMethod.GET
        ),
        recordSelector = RecordSelector(
          extractor = Extractor.DPath(List())
        ),
        paginator = PaginationStrategy.NoPagination
      )

      for records <- YamlInterpreter
          .interpretStream(spec, Map.empty)
          .runCollect
      yield assertTrue(
        records.length == 100, // JSONPlaceholder has 100 posts
        records.head.asObject.flatMap(_("userId")).flatMap(_.asNumber).isDefined,
        records.head.asObject.flatMap(_("title")).flatMap(_.asString).isDefined
      )
    },
    test("applies filter to records") {
      // Filter posts to only userId = 1
      val spec = StreamSpec(
        name = "posts",
        requester = Requester(
          url = "https://jsonplaceholder.typicode.com/posts",
          method = HttpMethod.GET
        ),
        recordSelector = RecordSelector(
          extractor = Extractor.DPath(List()),
          filter = Some("record.userId == 1")
        ),
        paginator = PaginationStrategy.NoPagination
      )

      for
        records <- YamlInterpreter
          .interpretStream(spec, Map.empty)
          .take(20) // Take first 20 to verify filtering works
          .runCollect

        // All records should have userId = 1
        userIds = records.flatMap(
          _.asObject.flatMap(_("userId")).flatMap(_.asNumber).flatMap(_.toInt)
        )
      yield assertTrue(
        userIds.forall(_ == 1),
        records.nonEmpty
      )
    },
    test("renders URL template with variables") {
      // Fetch comments for a specific post
      val spec = StreamSpec(
        name = "comments",
        requester = Requester(
          url = "https://jsonplaceholder.typicode.com/posts/{{ postId }}/comments",
          method = HttpMethod.GET
        ),
        recordSelector = RecordSelector(
          extractor = Extractor.DPath(List())
        ),
        paginator = PaginationStrategy.NoPagination
      )

      for records <- YamlInterpreter
          .interpretStream(spec, Map("postId" -> 1))
          .runCollect
      yield assertTrue(
        records.nonEmpty,
        records.head.asObject.flatMap(_("postId")).flatMap(_.asNumber).flatMap(_.toInt) == Some(1)
      )
    }
  ).provide(
    // All service layers
    Client.default,
    HttpClient.Live.layer,
    TemplateEngine.Live.layer,
    ResponseParser.Live.layer,
    Authenticator.Live.layer
  ) @@ TestAspect.timeout(30.seconds) // Network calls can be slow
