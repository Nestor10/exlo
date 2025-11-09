package exlo.yaml.integration

import exlo.yaml.*
import exlo.yaml.infra.ConfigLoader
import exlo.yaml.infra.HttpClient
import exlo.yaml.service.*
import exlo.yaml.spec.*
import exlo.yaml.template.TemplateValue
import exlo.yaml.util.JsonUtils
import zio.*
import zio.http.Client
import zio.test.*

object ConfigInterpolationSpec extends ZIOSpecDefault:

  def spec = suite("Config Interpolation")(
    test("interpolates config values in URL") {
      val yaml = """
        |streams:
        |  - name: test
        |    requester:
        |      url: "https://{{ config.shop }}.myshopify.com/admin/api/{{ config.api_version }}/products.json"
        |      method: GET
        |      headers:
        |        X-Shopify-Access-Token: "{{ config.api_token }}"
        |      params:
        |        limit: "50"
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
        // Set config in environment
        _        <- TestSystem.putEnv(
          "EXLO_CONNECTOR_CONFIG",
          """{"shop": "my-store", "api_version": "2024-01", "api_token": "shpat_secret"}"""
        )

        // Load spec
        tempFile <- ZIO.succeed {
          val path = java.nio.file.Files.createTempFile("shopify-test", ".yaml")
          java.nio.file.Files.write(path, yaml.getBytes)
          path.toString
        }
        spec     <- YamlSpecLoader.loadSpec(tempFile)
        _        <- ZIO.succeed(java.nio.file.Files.delete(java.nio.file.Paths.get(tempFile)))

        // Verify spec loaded
        _ <- ZIO.succeed(assertTrue(spec.streams.size == 1))

        // Load config from environment
        config <- ConfigLoader.loadUnvalidated
        context = Map(
          "state"  -> TemplateValue.Obj(Map.empty),
          "config" -> TemplateValue.fromJsonNode(config.node)
        )

        // Render URL template
        templateEngine <- ZIO.service[TemplateEngine]
        renderedUrl <- templateEngine.render(
          spec.streams.head.requester.url,
          context
        )

        // Render header template
        accessToken = spec.streams.head.requester.headers("X-Shopify-Access-Token")
        renderedToken <- templateEngine.render(accessToken, context)
      yield assertTrue(
        renderedUrl == "https://my-store.myshopify.com/admin/api/2024-01/products.json",
        renderedToken == "shpat_secret"
      )
    }.provide(
      TemplateEngine.Live.layer,
      YamlSpecLoader.Live.layer,
      Client.default
    ),
    test("interpolates config in authentication") {
      val yaml = """
        |streams:
        |  - name: test
        |    requester:
        |      url: "https://api.example.com/users"
        |      method: GET
        |      headers: {}
        |      params: {}
        |      auth:
        |        type: ApiKey
        |        header: "X-API-Key"
        |        token: "{{ config.api_key }}"
        |    recordSelector:
        |      extractor:
|        type: DpathExtractor
        |        field_path: []
        |      filter: null
        |    paginator:
        |      type: NoPagination
        |""".stripMargin

      for
        // Set config
        _ <- TestSystem.putEnv("EXLO_CONNECTOR_CONFIG", """{"api_key": "secret123"}""")

        // Load spec
        tempFile <- ZIO.succeed {
          val path = java.nio.file.Files.createTempFile("apikey-test", ".yaml")
          java.nio.file.Files.write(path, yaml.getBytes)
          path.toString
        }
        spec     <- YamlSpecLoader.loadSpec(tempFile)
        _        <- ZIO.succeed(java.nio.file.Files.delete(java.nio.file.Paths.get(tempFile)))

        // Load config from environment
        config <- ConfigLoader.loadUnvalidated
        context    = Map(
          "state"  -> TemplateValue.Obj(Map.empty),
          "config" -> TemplateValue.fromJsonNode(config.node)
        )

        // Render the token template manually to verify interpolation
        templateEngine <- ZIO.service[TemplateEngine]
        authConfig = spec.streams.head.requester.auth match
          case auth: Auth.ApiKey => auth
          case _                 => throw new Exception("Expected ApiKey auth")

        renderedToken <- templateEngine.render(authConfig.token, context)
      yield assertTrue(
        authConfig.header == "X-API-Key",
        renderedToken == "secret123"
      )
    }.provide(
      Authenticator.Live.layer,
      TemplateEngine.Live.layer,
      YamlSpecLoader.Live.layer,
      HttpClient.Live.layer,
      Client.default
    ),
    test("interpolates config in OAuth credentials") {
      val yaml = """
        |streams:
        |  - name: test
        |    requester:
        |      url: "https://api.example.com/users"
        |      method: GET
        |      headers: {}
        |      params: {}
        |      auth:
        |        type: OAuth
        |        tokenUrl: "https://oauth.example.com/token"
        |        clientId: "{{ config.client_id }}"
        |        clientSecret: "{{ config.client_secret }}"
        |        scopes: "read:users write:users"
        |    recordSelector:
        |      extractor:
|        type: DpathExtractor
        |        field_path: []
        |      filter: null
        |    paginator:
        |      type: NoPagination
        |""".stripMargin

      for
        // Set config
        _ <- TestSystem.putEnv("EXLO_CONNECTOR_CONFIG", """{"client_id": "app123", "client_secret": "secret456"}""")

        // Load spec
        tempFile <- ZIO.succeed {
          val path = java.nio.file.Files.createTempFile("oauth-test", ".yaml")
          java.nio.file.Files.write(path, yaml.getBytes)
          path.toString
        }
        spec     <- YamlSpecLoader.loadSpec(tempFile)
        _        <- ZIO.succeed(java.nio.file.Files.delete(java.nio.file.Paths.get(tempFile)))

        // Verify OAuth config loaded with templates
        authConfig = spec.streams.head.requester.auth match
          case auth: Auth.OAuth => auth
          case _                => throw new Exception("Expected OAuth auth")

        // Load config from environment
        config <- ConfigLoader.loadUnvalidated
        context    = Map(
          "state"  -> TemplateValue.Obj(Map.empty),
          "config" -> TemplateValue.fromJsonNode(config.node)
        )

        templateEngine       <- ZIO.service[TemplateEngine]
        renderedClientId     <- templateEngine.render(authConfig.clientId, context)
        renderedClientSecret <- templateEngine.render(authConfig.clientSecret, context)
      yield assertTrue(
        renderedClientId == "app123",
        renderedClientSecret == "secret456"
      )
    }.provide(
      TemplateEngine.Live.layer,
      YamlSpecLoader.Live.layer,
      Client.default
    )
  )
