package exlo.yaml

import zio.*
import zio.test.*
import zio.test.Assertion.*

object ConnectorConfigSpec extends ZIOSpecDefault:

  def spec = suite("ConnectorConfig")(
    test("loads empty config when EXLO_CONFIG not set") {
      for
        _      <- TestSystem.putEnv("EXLO_CONFIG", "")
        config <- ConnectorConfig.fromEnvironment
      yield assertTrue(
        config.values.isEmpty
      )
    },
    test("parses valid JSON config") {
      val configJson = """{"api_key": "secret123", "shop": "my-store", "region": "us-east-1"}"""

      for
        _      <- TestSystem.putEnv("EXLO_CONFIG", configJson)
        config <- ConnectorConfig.fromEnvironment
      yield assertTrue(
        config.values.size == 3,
        config.values("api_key") == "secret123",
        config.values("shop") == "my-store",
        config.values("region") == "us-east-1"
      )
    },
    test("handles empty JSON object") {
      for
        _      <- TestSystem.putEnv("EXLO_CONFIG", "{}")
        config <- ConnectorConfig.fromEnvironment
      yield assertTrue(
        config.values.isEmpty
      )
    },
    test("handles empty string as empty config") {
      for
        _      <- TestSystem.putEnv("EXLO_CONFIG", "")
        config <- ConnectorConfig.fromEnvironment
      yield assertTrue(
        config.values.isEmpty
      )
    },
    test("fails on invalid JSON") {
      for
        _      <- TestSystem.putEnv("EXLO_CONFIG", "{invalid json")
        result <- ConnectorConfig.fromEnvironment.either
      yield assertTrue(
        result.isLeft,
        result.swap.toOption.get.getMessage.contains("Failed to parse EXLO_CONFIG")
      )
    },
    test("getRequired succeeds when key exists") {
      val config = ConnectorConfig(Map("api_key" -> "secret"))

      for value <- config.getRequired("api_key")
      yield assertTrue(value == "secret")
    },
    test("getRequired fails when key missing") {
      val config = ConnectorConfig(Map("api_key" -> "secret"))

      for result <- config.getRequired("other_key").either
      yield assertTrue(
        result.isLeft,
        result.swap.toOption.get.contains("Missing required config key: 'other_key'")
      )
    },
    test("getOptional returns Some when key exists") {
      val config = ConnectorConfig(Map("api_key" -> "secret"))

      for maybeValue <- config.getOptional("api_key")
      yield assertTrue(maybeValue == Some("secret"))
    },
    test("getOptional returns None when key missing") {
      val config = ConnectorConfig(Map("api_key" -> "secret"))

      for maybeValue <- config.getOptional("other_key")
      yield assertTrue(maybeValue.isEmpty)
    },
    test("toJavaMap converts to Java Map for Jinja2") {
      val config  = ConnectorConfig(Map("api_key" -> "secret", "shop" -> "store"))
      val javaMap = config.toJavaMap

      assertTrue(
        javaMap.get("api_key") == "secret",
        javaMap.get("shop") == "store",
        javaMap.size() == 2
      )
    }
  )
