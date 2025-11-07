package exlo.yaml.service

import exlo.yaml.spec.Extractor
import io.circe.Json
import io.circe.parser.*
import zio.*
import zio.test.*

/** Tests for ResponseParser service.
  *
  * Verifies JSON extraction using circe-optics with field paths.
  */
object ResponseParserSpec extends ZIOSpecDefault:

  def spec = suite("ResponseParser")(
    test("extracts array from root") {
      val json = parse("""[{"id":1},{"id":2}]""").getOrElse(Json.Null)
      for
        result <- ResponseParser.extract(
          json,
          Extractor.DPath(List())
        )
      yield assertTrue(
        result.length == 2,
        result.head.asObject.flatMap(_("id")).flatMap(_.asNumber).flatMap(_.toInt) == Some(1)
      )
    },
    
    test("extracts nested field") {
      val json = parse("""{"data":{"users":[{"name":"Alice"},{"name":"Bob"}]}}""").getOrElse(Json.Null)
      for
        result <- ResponseParser.extract(
          json,
          Extractor.DPath(List("data", "users"))
        )
      yield assertTrue(
        result.length == 2,
        result.head.asObject.flatMap(_("name")).flatMap(_.asString) == Some("Alice")
      )
    },
    
    test("extracts single-level nested field") {
      val json = parse("""{"items":[{"id":1},{"id":2},{"id":3}]}""").getOrElse(Json.Null)
      for
        result <- ResponseParser.extract(
          json,
          Extractor.DPath(List("items"))
        )
      yield assertTrue(
        result.length == 3
      )
    },
    
    test("wraps non-array result in list") {
      val json = parse("""{"user":{"id":1,"name":"Alice"}}""").getOrElse(Json.Null)
      for
        result <- ResponseParser.extract(
          json,
          Extractor.DPath(List("user"))
        )
      yield assertTrue(
        result.length == 1,
        result.head.asObject.flatMap(_("name")).flatMap(_.asString) == Some("Alice")
      )
    },
    
    test("returns empty list for missing path") {
      val json = parse("""{"data":[]}""").getOrElse(Json.Null)
      for
        result <- ResponseParser.extract(
          json,
          Extractor.DPath(List("missing", "path"))
        )
      yield assertTrue(result.isEmpty)
    },
    
    test("extracts deeply nested field") {
      val json = parse("""{"response":{"body":{"data":{"records":[{"x":1}]}}}}""").getOrElse(Json.Null)
      for
        result <- ResponseParser.extract(
          json,
          Extractor.DPath(List("response", "body", "data", "records"))
        )
      yield assertTrue(
        result.length == 1,
        result.head.asObject.flatMap(_("x")).flatMap(_.asNumber).flatMap(_.toInt) == Some(1)
      )
    },
    
    test("handles empty array") {
      val json = parse("""{"items":[]}""").getOrElse(Json.Null)
      for
        result <- ResponseParser.extract(
          json,
          Extractor.DPath(List("items"))
        )
      yield assertTrue(result.isEmpty)
    }
  ).provide(ResponseParser.Live.layer)
