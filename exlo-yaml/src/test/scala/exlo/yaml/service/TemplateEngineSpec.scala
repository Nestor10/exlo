package exlo.yaml.service

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
        for
          _      <- RuntimeContext.setPaginationVar("name", TemplateValue.Str("World"))
          result <- TemplateEngine.render("Hello {{ name }}!")
        yield assertTrue(result == "Hello World!")
      },
      test("renders multiple variables") {
        for
          _      <- RuntimeContext.setPaginationVar("greeting", TemplateValue.Str("Hi"))
          _      <- RuntimeContext.setPaginationVar("name", TemplateValue.Str("Alice"))
          result <- TemplateEngine.render("{{ greeting }} {{ name }}!")
        yield assertTrue(result == "Hi Alice!")
      },
      test("renders nested object access") {
        for
          _      <- RuntimeContext.setPaginationVar(
            "config",
            TemplateValue.Obj(Map("api_key" -> Some(TemplateValue.Str("secret"))))
          )
          result <- TemplateEngine.render("{{ config.api_key }}")
        yield assertTrue(result == "secret")
      },
      test("renders URL with variables") {
        for
          _      <- RuntimeContext.setPaginationVar("shop", TemplateValue.Str("test-store"))
          _      <- RuntimeContext.setPaginationVar("limit", TemplateValue.Num(50))
          result <- TemplateEngine.render("https://{{ shop }}.myshopify.com/api?limit={{ limit }}")
        yield assertTrue(result == "https://test-store.myshopify.com/api?limit=50")
      },
      test("renders template with missing variable as empty") {
        for result <- TemplateEngine.render("Value: {{ missing }}")
        yield assertTrue(result == "Value: ")
      }
    ),
    suite("evaluateCondition")(
      test("evaluates true condition") {
        for
          _      <- RuntimeContext.setPaginationVar("value", TemplateValue.Num(15))
          result <- TemplateEngine.evaluateCondition("value > 10")
        yield assertTrue(result == true)
      },
      test("evaluates false condition") {
        for
          _      <- RuntimeContext.setPaginationVar("value", TemplateValue.Num(15))
          result <- TemplateEngine.evaluateCondition("value < 10")
        yield assertTrue(result == false)
      },
      test("evaluates equality") {
        for
          _      <- RuntimeContext.setPaginationVar("status", TemplateValue.Str("active"))
          result <- TemplateEngine.evaluateCondition("status == 'active'")
        yield assertTrue(result == true)
      },
      test("evaluates boolean variable") {
        for
          _      <- RuntimeContext.setPaginationVar("is_valid", TemplateValue.Bool(true))
          result <- TemplateEngine.evaluateCondition("is_valid")
        yield assertTrue(result == true)
      },
      test("evaluates logical AND") {
        for
          _      <- RuntimeContext.setPaginationVar("x", TemplateValue.Num(7))
          _      <- RuntimeContext.setPaginationVar("y", TemplateValue.Num(8))
          result <- TemplateEngine.evaluateCondition("x > 5 and y < 10")
        yield assertTrue(result == true)
      },
      test("treats non-empty string as truthy") {
        for
          _      <- RuntimeContext.setPaginationVar("name", TemplateValue.Str("Alice"))
          result <- TemplateEngine.evaluateCondition("name")
        yield assertTrue(result == true)
      },
      test("treats empty string as falsy") {
        for
          _      <- RuntimeContext.setPaginationVar("name", TemplateValue.Str(""))
          result <- TemplateEngine.evaluateCondition("name")
        yield assertTrue(result == false)
      }
    )
  ).provide(TemplateEngine.Live.layer, RuntimeContext.Stub.layer)
