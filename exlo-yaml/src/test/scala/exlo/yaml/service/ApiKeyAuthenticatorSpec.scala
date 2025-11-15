package exlo.yaml.service

import exlo.yaml.spec.Auth
import exlo.yaml.template.TemplateValue
import zio.*
import zio.test.*

/**
 * Unit tests for ApiKeyAuthenticator parsing and Jinja template features.
 *
 * Tests:
 * - ApiKeyAuthenticator with inject_into configuration
 * - Jinja conditional expressions
 * - Jinja array join filter
 */
object ApiKeyAuthenticatorSpec extends ZIOSpecDefault:

  def spec = suite("ApiKeyAuthenticator")(
    test("parses ApiKeyAuthenticator with inject_into header") {
      val yaml =
        """
          |version: 1.0.0
          |streams:
          |  - name: everything
          |    requester:
          |      url: https://newsapi.org/v2/everything
          |      method: GET
          |      auth:
          |        type: ApiKeyAuthenticator
          |        api_token: "{{ config['api_key'] }}"
          |        inject_into:
          |          type: RequestOption
          |          field_name: X-Api-Key
          |          inject_into: header
          |    recordSelector:
          |      extractor:
          |        type: DpathExtractor
          |        field_path:
          |          - articles
          |""".stripMargin

      ZIO.scoped {
        for
          tempFile <- ZIO.attempt(java.nio.file.Files.createTempFile("newsapi-test", ".yaml"))
          _        <- ZIO.attempt(java.nio.file.Files.writeString(tempFile, yaml))
          spec     <- YamlSpecLoader.loadSpec(tempFile.toString)
        yield assertTrue(
          spec.streams.size == 1,
          spec.streams.head.name == "everything",
          spec.streams.head.requester.url == "https://newsapi.org/v2/everything",
          spec.streams.head.requester.auth match
            case Auth.ApiKeyAuthenticator(apiToken, injectInto) =>
              apiToken == "{{ config['api_key'] }}" &&
              injectInto.fieldName == "X-Api-Key" &&
              injectInto.injectInto == "header"
            case _                                              => false
        )
      }
    }.provide(YamlSpecLoader.Live.layer),
    test("renders Jinja conditionals") {
      val template = "{{ config['country'] if config['country'] is defined else 'default' }}"

      for
        // Config with country
        _      <- RuntimeContext.setPaginationVar(
          "config",
          TemplateValue.Obj(
            Map(
              "country" -> Some(TemplateValue.Str("us"))
            )
          )
        )
        result <- TemplateEngine.render(template)
      yield assertTrue(
        result == "us"
      )
    }.provide(
      RuntimeContext.Stub.layer,
      TemplateEngine.Live.layer
    ),
    test("renders Jinja array join filter") {
      val template = "{{ config['search_in']|join(',') }}"

      for
        _      <- RuntimeContext.setPaginationVar(
          "config",
          TemplateValue.Obj(
            Map(
              "search_in" -> Some(
                TemplateValue.Arr(
                  List(
                    Some(TemplateValue.Str("title")),
                    Some(TemplateValue.Str("description")),
                    Some(TemplateValue.Str("content"))
                  )
                )
              )
            )
          )
        )
        result <- TemplateEngine.render(template)
      yield assertTrue(
        result == "title,description,content"
      )
    }.provide(
      RuntimeContext.Stub.layer,
      TemplateEngine.Live.layer
    ),
    test("parses ApiKeyAuthenticator with request params") {
      val yaml =
        """
          |version: 4.3.0
          |streams:
          |  - name: everything
          |    requester:
          |      url: https://newsapi.org/v2/everything
          |      method: GET
          |      auth:
          |        type: ApiKeyAuthenticator
          |        api_token: "{{ config['api_key'] }}"
          |        inject_into:
          |          type: RequestOption
          |          field_name: X-Api-Key
          |          inject_into: header
          |      params:
          |        q: "{{ config['search_query'] }}"
          |        searchIn: "{{ ','.join(config.get('search_in', [])) }}"
          |        from: "{{ config['start_date'] }}"
          |        language: "{{ config['language'] }}"
          |      error_handler:
          |        type: CompositeErrorHandler
          |        error_handlers:
          |          - type: DefaultErrorHandler
          |            response_filters:
          |              - type: HttpResponseFilter
          |                action: IGNORE
          |                http_codes:
          |                  - 426
          |                error_message_contains: requested too many results
          |    recordSelector:
          |      extractor:
          |        type: DpathExtractor
          |        field_path:
          |          - articles
          |    paginator:
          |      type: PageIncrement
          |      pageSize: 100
          |      startFrom: 1
          |""".stripMargin

      ZIO.scoped {
        for
          tempFile <- ZIO.attempt(java.nio.file.Files.createTempFile("newsapi-full", ".yaml"))
          _        <- ZIO.attempt(java.nio.file.Files.writeString(tempFile, yaml))
          spec     <- YamlSpecLoader.loadSpec(tempFile.toString)
          stream = spec.streams.head
        yield assertTrue(
          stream.name == "everything",
          stream.requester.params.contains("q"),
          stream.requester.params("q") == "{{ config['search_query'] }}",
          stream.requester.params.contains("searchIn"),
          stream.requester.params("searchIn") == "{{ ','.join(config.get('search_in', [])) }}"
        )
      }
    }.provide(YamlSpecLoader.Live.layer)
  )
