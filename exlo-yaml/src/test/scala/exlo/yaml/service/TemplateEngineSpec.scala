package exlo.yaml.service

import exlo.yaml.template.TemplateValue
import exlo.yaml.template.TemplateValue
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
            Map("name" -> TemplateValue.Str("World"))
          )
        yield assertTrue(result == "Hello World!")
      },
      test("renders multiple variables") {
        for result <- TemplateEngine.render(
            "{{ greeting }} {{ name }}!",
            Map("greeting" -> TemplateValue.Str("Hi"), "name" -> TemplateValue.Str("Alice"))
          )
        yield assertTrue(result == "Hi Alice!")
      },
      test("renders nested object access") {
        for result <- TemplateEngine.render(
            "{{ config.api_key }}",
            Map("config" -> TemplateValue.Obj(Map("api_key" -> Some(TemplateValue.Str("secret")))))
          )
        yield assertTrue(result == "secret")
      },
      test("renders URL with variables") {
        for result <- TemplateEngine.render(
            "https://{{ shop }}.myshopify.com/api?limit={{ limit }}",
            Map("shop" -> TemplateValue.Str("test-store"), "limit" -> TemplateValue.Num(50))
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
            Map[String, TemplateValue]("value" -> TemplateValue.Num(15))
          )
        yield assertTrue(result == true)
      },
      test("evaluates false condition") {
        for result <- TemplateEngine.evaluateCondition(
            "value < 10",
            Map[String, TemplateValue]("value" -> TemplateValue.Num(15))
          )
        yield assertTrue(result == false)
      },
      test("evaluates equality") {
        for result <- TemplateEngine.evaluateCondition(
            "status == 'active'",
            Map("status" -> TemplateValue.Str("active"))
          )
        yield assertTrue(result == true)
      },
      test("evaluates boolean variable") {
        for result <- TemplateEngine.evaluateCondition(
            "is_valid",
            Map("is_valid" -> TemplateValue.Bool(true))
          )
        yield assertTrue(result == true)
      },
      test("evaluates logical AND") {
        for result <- TemplateEngine.evaluateCondition(
            "x > 5 and y < 10",
            Map("x" -> TemplateValue.Num(7), "y" -> TemplateValue.Num(8))
          )
        yield assertTrue(result == true)
      },
      test("treats non-empty string as truthy") {
        for result <- TemplateEngine.evaluateCondition(
            "name",
            Map("name" -> TemplateValue.Str("Alice"))
          )
        yield assertTrue(result == true)
      },
      test("treats empty string as falsy") {
        for result <- TemplateEngine.evaluateCondition(
            "name",
            Map("name" -> TemplateValue.Str(""))
          )
        yield assertTrue(result == false)
      }
    )
  ).provide(TemplateEngine.Live.layer)
