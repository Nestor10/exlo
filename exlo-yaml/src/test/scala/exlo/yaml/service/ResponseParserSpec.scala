package exlo.yaml.service

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import exlo.yaml.spec.Extractor
import exlo.yaml.template.TemplateValue
import zio.*
import zio.test.*

/**
 * Tests for ResponseParser service.
 *
 * Verifies JSON extraction using json-path library with full JSONPath support.
 */
object ResponseParserSpec extends ZIOSpecDefault:

  private val jsonMapper: ObjectMapper =
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    mapper

  private def parseJson(jsonString: String): JsonNode =
    jsonMapper.readTree(jsonString)

  def spec = suite("ResponseParser")(
    test("extracts array from root") {
      val json = parseJson("""[{"id":1},{"id":2}]""")
      for result <- ResponseParser.extract(
          json,
          Extractor.DPath(List())
        )
      yield assertTrue(
        result.length == 2,
        result.head.get("id").asInt() == 1
      )
    },
    test("extracts nested field") {
      val json = parseJson("""{"data":{"users":[{"name":"Alice"},{"name":"Bob"}]}}""")
      for result <- ResponseParser.extract(
          json,
          Extractor.DPath(List("data", "users"))
        )
      yield assertTrue(
        result.length == 2,
        result.head.get("name").asText() == "Alice"
      )
    },
    test("extracts single-level nested field") {
      val json = parseJson("""{"items":[{"id":1},{"id":2},{"id":3}]}""")
      for result <- ResponseParser.extract(
          json,
          Extractor.DPath(List("items"))
        )
      yield assertTrue(
        result.length == 3
      )
    },
    test("wraps non-array result in list") {
      val json = parseJson("""{"user":{"id":1,"name":"Alice"}}""")
      for result <- ResponseParser.extract(
          json,
          Extractor.DPath(List("user"))
        )
      yield assertTrue(
        result.length == 1,
        result.head.get("name").asText() == "Alice"
      )
    },
    test("returns empty list for missing path") {
      val json = parseJson("""{"data":[]}""")
      for result <- ResponseParser.extract(
          json,
          Extractor.DPath(List("missing", "path"))
        )
      yield assertTrue(result.isEmpty)
    },
    test("extracts deeply nested field") {
      val json = parseJson("""{"response":{"body":{"data":{"records":[{"x":1}]}}}}""")
      for result <- ResponseParser.extract(
          json,
          Extractor.DPath(List("response", "body", "data", "records"))
        )
      yield assertTrue(
        result.length == 1,
        result.head.get("x").asInt() == 1
      )
    },
    test("handles empty array") {
      val json = parseJson("""{"items":[]}""")
      for result <- ResponseParser.extract(
          json,
          Extractor.DPath(List("items"))
        )
      yield assertTrue(result.isEmpty)
    }
  ).provide(ResponseParser.Live.layer)
