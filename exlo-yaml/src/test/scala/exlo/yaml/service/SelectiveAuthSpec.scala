package exlo.yaml.service

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.databind.node.TextNode
import exlo.yaml.domain.YamlRuntimeError
import exlo.yaml.spec.Auth
import exlo.yaml.template.TemplateValue
import zio.*
import zio.http.Client
import zio.test.*
import zio.test.Assertion.*

/**
 * Test SelectiveAuth authenticator selection based on config paths.
 */
object SelectiveAuthSpec extends ZIOSpecDefault:

  def spec = suite("SelectiveAuth")(
    test("selects BearerAuthenticator based on config path") {
      val mapper     = ObjectMapper()
      val configNode = mapper.createObjectNode()
      val credsNode  = mapper.createObjectNode()
      credsNode.put("option_title", "API Token Credentials")
      credsNode.put("api_token", "xoxb-secret-token")
      configNode.set[ObjectNode]("credentials", credsNode)

      val selectiveAuth = Auth.SelectiveAuth(
        selectionPath = List("credentials", "option_title"),
        authenticators = Map(
          "API Token Credentials" -> Auth.BearerAuthenticator("{{ config.credentials.api_token }}"),
          "OAuth2.0"              -> Auth.Bearer("different-token")
        )
      )

      for
        // Set config in RuntimeContext
        _      <- RuntimeContext.setPaginationVar("config", TemplateValue.fromJsonNode(configNode))
        // Authenticate
        result <- Authenticator.authenticate(selectiveAuth, Map.empty)
      yield assertTrue(
        result.contains("Authorization"),
        result("Authorization") == "Bearer {{ config.credentials.api_token }}"
      )
    },
    test("fails when config path not found") {
      val selectiveAuth = Auth.SelectiveAuth(
        selectionPath = List("missing", "path"),
        authenticators = Map(
          "API Token" -> Auth.Bearer("token")
        )
      )

      for
        // Empty config
        _      <- RuntimeContext.setPaginationVar("config", TemplateValue.Obj(Map.empty))
        result <- Authenticator.authenticate(selectiveAuth, Map.empty).exit
      yield assert(result)(fails(isSubtype[YamlRuntimeError.AuthError](anything)))
    },
    test("fails when selector value not found in authenticators") {
      val mapper     = ObjectMapper()
      val configNode = mapper.createObjectNode()
      val credsNode  = mapper.createObjectNode()
      credsNode.put("option_title", "Unknown Option")
      configNode.set[ObjectNode]("credentials", credsNode)

      val selectiveAuth = Auth.SelectiveAuth(
        selectionPath = List("credentials", "option_title"),
        authenticators = Map(
          "API Token" -> Auth.Bearer("token1"),
          "OAuth"     -> Auth.Bearer("token2")
        )
      )

      for
        _      <- RuntimeContext.setPaginationVar("config", TemplateValue.fromJsonNode(configNode))
        result <- Authenticator.authenticate(selectiveAuth, Map.empty).exit
      yield assert(result)(fails(isSubtype[YamlRuntimeError.AuthError](anything)))
    },
    test("BearerAuthenticator adds Authorization header") {
      val auth = Auth.BearerAuthenticator("my-api-token")
      for result <- Authenticator.authenticate(auth, Map.empty)
      yield assertTrue(
        result == Map("Authorization" -> "Bearer my-api-token")
      )
    }
  ).provide(
    Client.default,
    Authenticator.Live.layer,
    RuntimeContext.Stub.layer,
    TemplateEngine.Live.layer
  )
