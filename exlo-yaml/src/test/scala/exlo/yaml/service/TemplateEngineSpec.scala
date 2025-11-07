package exlo.yaml.service

import zio.*
import zio.test.*

/**
 * Tests for TemplateEngine service.
 *
 * Verifies Jinja2 template rendering and condition evaluation using jinjava.
 */
object TemplateEngineSpec extends ZIOSpecDefault:

  def spec = suite("TemplateEngine")(
    suite("render")(
      test("renders simple variable") {
        for result <- TemplateEngine.render(
            "Hello {{ name }}!",
            Map("name" -> "World")
          )
        yield assertTrue(result == "Hello World!")
      },
      test("renders multiple variables") {
        for result <- TemplateEngine.render(
            "{{ greeting }} {{ name }}!",
            Map("greeting" -> "Hi", "name" -> "Alice")
          )
        yield assertTrue(result == "Hi Alice!")
      },
      test("renders nested object access") {
        import scala.jdk.CollectionConverters.*
        for result <- TemplateEngine.render(
            "{{ config.api_key }}",
            Map("config" -> Map("api_key" -> "secret").asJava.asInstanceOf[Any])
          )
        yield assertTrue(result == "secret")
      },
      test("renders URL with variables") {
        for result <- TemplateEngine.render(
            "https://{{ shop }}.myshopify.com/api?limit={{ limit }}",
            Map("shop" -> "test-store", "limit" -> 50)
          )
        yield assertTrue(result == "https://test-store.myshopify.com/api?limit=50")
      },
      test("renders template with missing variable as empty") {
        for result <- TemplateEngine.render(
            "Value: {{ missing }}",
            Map.empty
          )
        yield assertTrue(result == "Value: ")
      }
    ),
    suite("evaluateCondition")(
      test("evaluates true condition") {
        for result <- TemplateEngine.evaluateCondition(
            "value > 10",
            Map("value" -> 15)
          )
        yield assertTrue(result == true)
      },
      test("evaluates false condition") {
        for result <- TemplateEngine.evaluateCondition(
            "value < 10",
            Map("value" -> 15)
          )
        yield assertTrue(result == false)
      },
      test("evaluates equality") {
        for result <- TemplateEngine.evaluateCondition(
            "status == 'active'",
            Map("status" -> "active")
          )
        yield assertTrue(result == true)
      },
      test("evaluates boolean variable") {
        for result <- TemplateEngine.evaluateCondition(
            "is_valid",
            Map("is_valid" -> true)
          )
        yield assertTrue(result == true)
      },
      test("evaluates logical AND") {
        for result <- TemplateEngine.evaluateCondition(
            "x > 5 and y < 10",
            Map("x" -> 7, "y" -> 8)
          )
        yield assertTrue(result == true)
      },
      test("treats non-empty string as truthy") {
        for result <- TemplateEngine.evaluateCondition(
            "name",
            Map("name" -> "Alice")
          )
        yield assertTrue(result == true)
      },
      test("treats empty string as falsy") {
        for result <- TemplateEngine.evaluateCondition(
            "name",
            Map("name" -> "")
          )
        yield assertTrue(result == false)
      }
    )
  ).provide(TemplateEngine.Live.layer)
