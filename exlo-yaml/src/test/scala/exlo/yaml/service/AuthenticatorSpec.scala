package exlo.yaml.service

import exlo.yaml.spec.Auth
import exlo.yaml.template.TemplateValue
import zio.*
import zio.http.*
import zio.test.*

/**
 * Tests for Authenticator service.
 *
 * Verifies that authentication configurations are correctly transformed into
 * HTTP headers.
 */
object AuthenticatorSpec extends ZIOSpecDefault:

  def spec = suite("Authenticator")(
    test("NoAuth leaves headers unchanged") {
      for result <- Authenticator.authenticate(
          Auth.NoAuth,
          Map("Content-Type" -> "application/json")
        )
      yield assertTrue(
        result == Map("Content-Type" -> "application/json")
      )
    },
    test("ApiKey adds custom header") {
      for result <- Authenticator.authenticate(
          Auth.ApiKey("X-API-Key", "secret-key-123"),
          Map("Content-Type" -> "application/json")
        )
      yield assertTrue(
        result == Map(
          "Content-Type" -> "application/json",
          "X-API-Key"    -> "secret-key-123"
        )
      )
    },
    test("Bearer adds Authorization header") {
      for result <- Authenticator.authenticate(
          Auth.Bearer("token-abc-xyz"),
          Map("User-Agent" -> "exlo")
        )
      yield assertTrue(
        result == Map(
          "User-Agent"    -> "exlo",
          "Authorization" -> "Bearer token-abc-xyz"
        )
      )
    },
    test("ApiKey can override existing header") {
      for result <- Authenticator.authenticate(
          Auth.ApiKey("X-API-Key", "new-key"),
          Map("X-API-Key" -> "old-key")
        )
      yield assertTrue(
        result == Map("X-API-Key" -> "new-key")
      )
    },
    test("works with empty headers") {
      for result <- Authenticator.authenticate(
          Auth.Bearer("token"),
          Map.empty
        )
      yield assertTrue(
        result == Map("Authorization" -> "Bearer token")
      )
    }
  ).provide(
    Client.default,
    Authenticator.Live.layer,
    RuntimeContext.Stub.layer,
    TemplateEngine.Live.layer
  )
