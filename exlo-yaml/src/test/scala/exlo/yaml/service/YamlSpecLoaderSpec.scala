package exlo.yaml.service

import exlo.yaml.spec.*
import exlo.yaml.template.TemplateValue
import zio.*
import zio.test.*

import java.nio.file.Files
import java.nio.file.Paths
import scala.io.Source

/**
 * Tests for YamlSpecLoader service.
 *
 * Verifies that YAML files can be loaded and parsed into ConnectorSpec ADTs.
 */
object YamlSpecLoaderSpec extends ZIOSpecDefault:

  def spec = suite("YamlSpecLoader")(
    test("parses simple YAML spec with no pagination") {
      val yaml = """
        |streams:
        |  - name: users
        |    requester:
        |      url: "https://api.example.com/users"
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
        // Write to temp file
        tempFile <- ZIO.succeed {
          val path = Files.createTempFile("test-spec", ".yaml")
          Files.write(path, yaml.getBytes)
          path.toString
        }

        // Load and parse
        spec     <- YamlSpecLoader.loadSpec(tempFile)

        // Cleanup
        _ <- ZIO.succeed(Files.delete(Paths.get(tempFile)))
      yield assertTrue(
        spec.streams.length == 1,
        spec.streams.head.name == "users",
        spec.streams.head.requester.url == "https://api.example.com/users",
        spec.streams.head.requester.method == HttpMethod.GET,
        spec.streams.head.requester.auth == Auth.NoAuth,
        spec.streams.head.paginator == PaginationStrategy.NoPagination
      )
    },
    test("parses YAML spec with ApiKey auth") {
      val yaml = """
        |streams:
        |  - name: orders
        |    requester:
        |      url: "https://api.example.com/orders"
        |      method: GET
        |      headers:
        |        Accept: "application/json"
        |      params:
        |        limit: "100"
        |      auth:
        |        type: ApiKey
        |        header: "X-API-Key"
        |        token: "secret-key"
        |    recordSelector:
        |      extractor:
|        type: DpathExtractor
        |        field_path: ["data", "orders"]
        |      filter: null
        |    paginator:
        |      type: NoPagination
        |""".stripMargin

      for
        tempFile <- ZIO.succeed {
          val path = Files.createTempFile("test-spec", ".yaml")
          Files.write(path, yaml.getBytes)
          path.toString
        }

        spec <- YamlSpecLoader.loadSpec(tempFile)
        _    <- ZIO.succeed(Files.delete(Paths.get(tempFile)))
      yield assertTrue(
        spec.streams.head.requester.auth == Auth.ApiKey("X-API-Key", "secret-key"),
        spec.streams.head.requester.headers == Map("Accept" -> "application/json"),
        spec.streams.head.requester.params == Map("limit" -> "100"),
        spec.streams.head.recordSelector.extractor == Extractor.DPath(List("data", "orders"))
      )
    },
    test("parses YAML spec with Bearer auth") {
      val yaml = """
        |streams:
        |  - name: products
        |    requester:
        |      url: "https://api.example.com/products"
        |      method: GET
        |      headers: {}
        |      params: {}
        |      auth:
        |        type: Bearer
        |        token: "bearer-token-123"
        |    recordSelector:
        |      extractor:
|        type: DpathExtractor
        |        field_path: ["items"]
        |      filter: null
        |    paginator:
        |      type: NoPagination
        |""".stripMargin

      for
        tempFile <- ZIO.succeed {
          val path = Files.createTempFile("test-spec", ".yaml")
          Files.write(path, yaml.getBytes)
          path.toString
        }

        spec <- YamlSpecLoader.loadSpec(tempFile)
        _    <- ZIO.succeed(Files.delete(Paths.get(tempFile)))
      yield assertTrue(
        spec.streams.head.requester.auth == Auth.Bearer("bearer-token-123")
      )
    },
    test("parses YAML spec with OAuth authentication") {
      val yaml = """
        |streams:
        |  - name: oauth_api
        |    requester:
        |      url: "https://api.example.com/data"
        |      method: GET
        |      headers: {}
        |      params: {}
        |      auth:
        |        type: OAuth
        |        tokenUrl: "https://auth.example.com/token"
        |        clientId: "{{ config.client_id }}"
        |        clientSecret: "{{ config.client_secret }}"
        |        scopes: "read write"
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
          val path = Files.createTempFile("test-spec", ".yaml")
          Files.write(path, yaml.getBytes)
          path.toString
        }

        spec <- YamlSpecLoader.loadSpec(tempFile)
        _    <- ZIO.succeed(Files.delete(Paths.get(tempFile)))
      yield assertTrue(
        spec.streams.head.requester.auth match
          case Auth.OAuth(tokenUrl, clientId, clientSecret, scopes) =>
            tokenUrl == "https://auth.example.com/token" &&
            clientId == "{{ config.client_id }}" &&
            clientSecret == "{{ config.client_secret }}" &&
            scopes == Some("read write")
          case _                                                    => false
      )
    },
    test("parses YAML spec with PageIncrement pagination") {
      val yaml = """
        |streams:
        |  - name: posts
        |    requester:
        |      url: "https://api.example.com/posts?page={{ page }}"
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
        |      type: PageIncrement
        |      pageSize: 50
        |      startFrom: 1
        |""".stripMargin

      for
        tempFile <- ZIO.succeed {
          val path = Files.createTempFile("test-spec", ".yaml")
          Files.write(path, yaml.getBytes)
          path.toString
        }

        spec <- YamlSpecLoader.loadSpec(tempFile)
        _    <- ZIO.succeed(Files.delete(Paths.get(tempFile)))
      yield assertTrue(
        spec.streams.head.paginator == PaginationStrategy.PageIncrement(50, 1),
        spec.streams.head.requester.url.contains("{{ page }}")
      )
    },
    test("parses YAML spec with OffsetIncrement pagination") {
      val yaml = """
        |streams:
        |  - name: comments
        |    requester:
        |      url: "https://api.example.com/comments?offset={{ offset }}&limit={{ limit }}"
        |      method: GET
        |      headers: {}
        |      params: {}
        |      auth:
        |        type: NoAuth
        |    recordSelector:
        |      extractor:
|        type: DpathExtractor
        |        field_path: ["data"]
        |      filter: null
        |    paginator:
        |      type: OffsetIncrement
        |      pageSize: 100
        |""".stripMargin

      for
        tempFile <- ZIO.succeed {
          val path = Files.createTempFile("test-spec", ".yaml")
          Files.write(path, yaml.getBytes)
          path.toString
        }

        spec <- YamlSpecLoader.loadSpec(tempFile)
        _    <- ZIO.succeed(Files.delete(Paths.get(tempFile)))
      yield assertTrue(
        spec.streams.head.paginator == PaginationStrategy.OffsetIncrement(100)
      )
    },
    test("parses YAML spec with CursorPagination") {
      val yaml = """
        |streams:
        |  - name: events
        |    requester:
        |      url: "https://api.example.com/events?cursor={{ next_page_token }}"
        |      method: GET
        |      headers: {}
        |      params: {}
        |      auth:
        |        type: NoAuth
        |    recordSelector:
        |      extractor:
|        type: DpathExtractor
        |        field_path: ["data", "events"]
        |      filter: null
        |    paginator:
        |      type: CursorPagination
        |      cursorValue: "{{ response.pagination.next_cursor }}"
        |      stopCondition: "{{ not response.pagination.has_more }}"
        |""".stripMargin

      for
        tempFile <- ZIO.succeed {
          val path = Files.createTempFile("test-spec", ".yaml")
          Files.write(path, yaml.getBytes)
          path.toString
        }

        spec <- YamlSpecLoader.loadSpec(tempFile)
        _    <- ZIO.succeed(Files.delete(Paths.get(tempFile)))
      yield assertTrue(
        spec.streams.head.paginator match
          case PaginationStrategy.CursorPagination(cursor, stop) =>
            cursor == "{{ response.pagination.next_cursor }}" &&
            stop == "{{ not response.pagination.has_more }}"
          case _                                                 => false
      )
    },
    test("parses YAML spec with filter") {
      val yaml = """
        |streams:
        |  - name: active_users
        |    requester:
        |      url: "https://api.example.com/users"
        |      method: GET
        |      headers: {}
        |      params: {}
        |      auth:
        |        type: NoAuth
        |    recordSelector:
        |      extractor:
|        type: DpathExtractor
        |        field_path: []
        |      filter: "{{ record.status == 'active' }}"
        |    paginator:
        |      type: NoPagination
        |""".stripMargin

      for
        tempFile <- ZIO.succeed {
          val path = Files.createTempFile("test-spec", ".yaml")
          Files.write(path, yaml.getBytes)
          path.toString
        }

        spec <- YamlSpecLoader.loadSpec(tempFile)
        _    <- ZIO.succeed(Files.delete(Paths.get(tempFile)))
      yield assertTrue(
        spec.streams.head.recordSelector.filter == Some("{{ record.status == 'active' }}")
      )
    },
    test("parses YAML spec with multiple streams") {
      val yaml = """
        |streams:
        |  - name: users
        |    requester:
        |      url: "https://api.example.com/users"
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
        |  - name: orders
        |    requester:
        |      url: "https://api.example.com/orders"
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
          val path = Files.createTempFile("test-spec", ".yaml")
          Files.write(path, yaml.getBytes)
          path.toString
        }

        spec <- YamlSpecLoader.loadSpec(tempFile)
        _    <- ZIO.succeed(Files.delete(Paths.get(tempFile)))
      yield assertTrue(
        spec.streams.length == 2,
        spec.streams(0).name == "users",
        spec.streams(1).name == "orders"
      )
    },
    test("parses YAML spec with POST request and body") {
      val yaml = """
        |streams:
        |  - name: create_order
        |    requester:
        |      url: "https://api.example.com/orders"
        |      method: POST
        |      headers:
        |        Content-Type: "application/json"
        |      params: {}
        |      auth:
        |        type: NoAuth
        |      body: |
        |        {
        |          "orderId": "{{ order_id }}",
        |          "amount": {{ amount }}
        |        }
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
          val path = Files.createTempFile("test-spec", ".yaml")
          Files.write(path, yaml.getBytes)
          path.toString
        }

        spec <- YamlSpecLoader.loadSpec(tempFile)
        _    <- ZIO.succeed(Files.delete(Paths.get(tempFile)))
      yield assertTrue(
        spec.streams.head.requester.method == HttpMethod.POST,
        spec.streams.head.requester.body.isDefined,
        spec.streams.head.requester.body.get.contains("order_id"),
        spec.streams.head.requester.body.get.contains("amount")
      )
    },
    test("loads the example connector.yaml file") {
      for spec <- YamlSpecLoader.loadSpec("exlo-yaml/connector.yaml")
      yield assertTrue(
        spec.streams.nonEmpty,
        spec.streams.head.name == "users",
        spec.streams.head.requester.url == "https://jsonplaceholder.typicode.com/users"
      )
    },
    test("parses error_handler with DefaultErrorHandler") {
      val yaml = """
        |streams:
        |  - name: test
        |    requester:
        |      url: "https://api.example.com/data"
        |      method: GET
        |      headers: {}
        |      params: {}
        |      auth:
        |        type: NoAuth
        |      error_handler:
        |        type: DefaultErrorHandler
        |        max_retries: 3
        |        backoff_strategies:
        |          - type: ExponentialBackoffStrategy
        |            factor: 2
        |        response_filters:
        |          - http_codes: [429, 500, 502, 503]
        |            action: RETRY
        |    recordSelector:
        |      extractor:
        |        type: DpathExtractor
        |        field_path: []
        |    paginator:
        |      type: NoPagination
        |""".stripMargin

      for
        tempFile <- ZIO.succeed {
          val path = Files.createTempFile("test-error-handler", ".yaml")
          Files.write(path, yaml.getBytes)
          path.toString
        }

        spec <- YamlSpecLoader.loadSpec(tempFile)
        _    <- ZIO.succeed(Files.delete(Paths.get(tempFile)))

        errorHandler = spec.streams.head.requester.errorHandler
      yield assertTrue(
        errorHandler.isDefined,
        errorHandler.get.isInstanceOf[ErrorHandler.DefaultErrorHandler],
        errorHandler.get.asInstanceOf[ErrorHandler.DefaultErrorHandler].maxRetries == 3,
        errorHandler.get.asInstanceOf[ErrorHandler.DefaultErrorHandler].backoffStrategies.length == 1,
        errorHandler.get
          .asInstanceOf[ErrorHandler.DefaultErrorHandler]
          .backoffStrategies
          .head
          .isInstanceOf[BackoffStrategy.ExponentialBackoff],
        errorHandler.get
          .asInstanceOf[ErrorHandler.DefaultErrorHandler]
          .backoffStrategies
          .head
          .asInstanceOf[BackoffStrategy.ExponentialBackoff]
          .factor == 2,
        errorHandler.get.asInstanceOf[ErrorHandler.DefaultErrorHandler].responseFilters.length == 1,
        errorHandler.get.asInstanceOf[ErrorHandler.DefaultErrorHandler].responseFilters.head.httpCodes == List(
          429,
          500,
          502,
          503
        ),
        errorHandler.get
          .asInstanceOf[ErrorHandler.DefaultErrorHandler]
          .responseFilters
          .head
          .action == ResponseAction.RETRY
      )
    },
    test("parses error_handler with CompositeErrorHandler") {
      val yaml = """
        |streams:
        |  - name: test
        |    requester:
        |      url: "https://api.example.com/data"
        |      method: GET
        |      headers: {}
        |      params: {}
        |      auth:
        |        type: NoAuth
        |      error_handler:
        |        type: CompositeErrorHandler
        |        error_handlers:
        |          - type: DefaultErrorHandler
        |            max_retries: 5
        |            backoff_strategies:
        |              - type: ConstantBackoffStrategy
        |                backoff_time_in_seconds: 10
        |            response_filters:
        |              - http_codes: [403]
        |                action: IGNORE
        |          - type: DefaultErrorHandler
        |            max_retries: 3
        |            backoff_strategies:
        |              - type: ExponentialBackoffStrategy
        |                factor: 5
        |            response_filters:
        |              - http_codes: [429, 500]
        |                action: RETRY
        |    recordSelector:
        |      extractor:
        |        type: DpathExtractor
        |        field_path: []
        |    paginator:
        |      type: NoPagination
        |""".stripMargin

      for
        tempFile <- ZIO.succeed {
          val path = Files.createTempFile("test-composite-error-handler", ".yaml")
          Files.write(path, yaml.getBytes)
          path.toString
        }

        spec <- YamlSpecLoader.loadSpec(tempFile)
        _    <- ZIO.succeed(Files.delete(Paths.get(tempFile)))

        errorHandler = spec.streams.head.requester.errorHandler
      yield assertTrue(
        errorHandler.isDefined,
        errorHandler.get.isInstanceOf[ErrorHandler.CompositeErrorHandler],
        errorHandler.get.asInstanceOf[ErrorHandler.CompositeErrorHandler].errorHandlers.length == 2,
        errorHandler.get.asInstanceOf[ErrorHandler.CompositeErrorHandler].errorHandlers.head.maxRetries == 5,
        errorHandler.get
          .asInstanceOf[ErrorHandler.CompositeErrorHandler]
          .errorHandlers
          .head
          .backoffStrategies
          .head
          .isInstanceOf[BackoffStrategy.ConstantBackoff],
        errorHandler.get
          .asInstanceOf[ErrorHandler.CompositeErrorHandler]
          .errorHandlers
          .head
          .backoffStrategies
          .head
          .asInstanceOf[BackoffStrategy.ConstantBackoff]
          .backoffTimeInSeconds == 10.0
      )
    },
    test("parses error_handler with error_message_contains filter") {
      val yaml = """
        |streams:
        |  - name: test
        |    requester:
        |      url: "https://api.example.com/data"
        |      method: GET
        |      headers: {}
        |      params: {}
        |      auth:
        |        type: NoAuth
        |      error_handler:
        |        type: DefaultErrorHandler
        |        max_retries: 5
        |        response_filters:
        |          - error_message_contains: "rate limit"
        |            action: RETRY
        |          - error_message_contains: "permission denied"
        |            action: IGNORE
        |    recordSelector:
        |      extractor:
        |        type: DpathExtractor
        |        field_path: []
        |    paginator:
        |      type: NoPagination
        |""".stripMargin

      for
        tempFile <- ZIO.succeed {
          val path = Files.createTempFile("test-error-message-filter", ".yaml")
          Files.write(path, yaml.getBytes)
          path.toString
        }

        spec <- YamlSpecLoader.loadSpec(tempFile)
        _    <- ZIO.succeed(Files.delete(Paths.get(tempFile)))

        errorHandler = spec.streams.head.requester.errorHandler
      yield assertTrue(
        errorHandler.isDefined,
        errorHandler.get.asInstanceOf[ErrorHandler.DefaultErrorHandler].responseFilters.length == 2,
        errorHandler.get
          .asInstanceOf[ErrorHandler.DefaultErrorHandler]
          .responseFilters
          .head
          .errorMessageContains
          .contains("rate limit"),
        errorHandler.get
          .asInstanceOf[ErrorHandler.DefaultErrorHandler]
          .responseFilters
          .head
          .action == ResponseAction.RETRY,
        errorHandler.get
          .asInstanceOf[ErrorHandler.DefaultErrorHandler]
          .responseFilters(1)
          .errorMessageContains
          .contains("permission denied"),
        errorHandler.get
          .asInstanceOf[ErrorHandler.DefaultErrorHandler]
          .responseFilters(1)
          .action == ResponseAction.IGNORE
      )
    }
  ).provide(YamlSpecLoader.Live.layer)
