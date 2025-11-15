package exlo.yaml.service

import com.fasterxml.jackson.databind.ObjectMapper
import exlo.yaml.domain.ConnectorConfig
import exlo.yaml.template.TemplateValue
import zio.*
import zio.test.*

import java.time.Instant

/**
 * Tests for RuntimeContext service.
 *
 * Verifies:
 *   - Atomic state updates via Ref
 *   - Concurrent access safety
 *   - Template context composition
 *   - Cursor tracking (max value logic)
 */
object RuntimeContextSpec extends ZIOSpecDefault:

  def spec = suite("RuntimeContextSpec")(
    test("updateHeaders stores headers atomically") {
      for
        ctx     <- ZIO.service[RuntimeContext]
        _       <- ctx.updateHeaders(Map("Retry-After" -> "120", "X-RateLimit-Remaining" -> "50"))
        headers <- ctx.getHeaders
      yield assertTrue(
        headers("Retry-After") == "120",
        headers("X-RateLimit-Remaining") == "50"
      )
    },
    test("updateCursor tracks maximum cursor value") {
      for
        ctx <- ZIO.service[RuntimeContext]

        // Update with ascending values
        _ <- ctx.updateCursor("2024-01-10")
        _ <- ctx.updateCursor("2024-01-15")
        _ <- ctx.updateCursor("2024-01-12") // Out of order - should be ignored

        cursor <- ctx.getCursor
      yield assertTrue(cursor == Some("2024-01-15"))
    },
    test("updateCursor with numeric values tracks max") {
      for
        ctx <- ZIO.service[RuntimeContext]

        _ <- ctx.updateCursor("100")
        _ <- ctx.updateCursor("200")
        _ <- ctx.updateCursor("150") // Lower - should be ignored

        cursor <- ctx.getCursor
      yield assertTrue(cursor == Some("200"))
    },
    test("setPaginationVar updates pagination variables") {
      for
        ctx <- ZIO.service[RuntimeContext]

        _ <- ctx.setPaginationVar("page", TemplateValue.Num(1))
        _ <- ctx.setPaginationVar("offset", TemplateValue.Num(0))
        _ <- ctx.setPaginationVar("limit", TemplateValue.Num(50))

        vars <- ctx.getPaginationVars
      yield assertTrue(
        vars("page") == TemplateValue.Num(1),
        vars("offset") == TemplateValue.Num(0),
        vars("limit") == TemplateValue.Num(50)
      )
    },
    test("resetPaginationVars clears pagination variables") {
      for
        ctx <- ZIO.service[RuntimeContext]

        _ <- ctx.setPaginationVar("page", TemplateValue.Num(5))
        _ <- ctx.resetPaginationVars

        vars <- ctx.getPaginationVars
      yield assertTrue(vars.isEmpty)
    },
    test("updateRateLimitInfo stores rate limit state") {
      for
        ctx <- ZIO.service[RuntimeContext]

        now <- Clock.instant
        resetTime = now.plusSeconds(3600)

        _ <- ctx.updateRateLimitInfo(50, resetTime)

        info <- ctx.getRateLimitInfo
      yield assertTrue(
        info.isDefined,
        info.get.remaining == 50,
        info.get.resetAt == resetTime
      )
    },
    test("getTemplateContext combines config, state, and pagination vars") {
      // Create test config
      val mapper     = new ObjectMapper()
      val configNode = mapper.createObjectNode()
      configNode.put("api_key", "test123")
      configNode.put("region", "us")

      val config = ConnectorConfig(configNode)

      // Create test initial state
      val stateNode = mapper.createObjectNode()
      stateNode.put("cursor", "2024-01-01")

      for
        // Create context with test data
        stateRef <- Ref.make(RuntimeContext.ExecutionState(cursor = Some("2024-01-15")))
        ctx = RuntimeContext.Live(config, stateNode, stateRef)

        // Add pagination var
        _ <- ctx.setPaginationVar("page", TemplateValue.Num(3))

        // Get template context
        templateContext <- ctx.getTemplateContext
      yield
        val configObj = templateContext("config").asInstanceOf[TemplateValue.Obj]
        val stateObj  = templateContext("state").asInstanceOf[TemplateValue.Obj]
        val pageVar   = templateContext("page")

        assertTrue(
          // Config is present
          configObj.value.contains("api_key"),
          // State has cursor
          stateObj.value.contains("cursor"),
          // Pagination var at top level
          pageVar == TemplateValue.Num(3)
        )
    },
    test("getStateJson serializes cursor") {
      for
        ctx <- ZIO.service[RuntimeContext]

        _ <- ctx.updateCursor("2024-01-15T10:30:00Z")

        stateJson <- ctx.getStateJson
      yield assertTrue(stateJson == """{"cursor": "2024-01-15T10:30:00Z"}""")
    },
    test("getStateJson returns empty object when no cursor") {
      for
        ctx <- ZIO.service[RuntimeContext]

        stateJson <- ctx.getStateJson
      yield assertTrue(stateJson == "{}")
    },
    test("concurrent updates to different fields are safe") {
      for
        ctx <- ZIO.service[RuntimeContext]

        // Concurrent updates to different state fields
        fiber1 <- ctx.updateHeaders(Map("header1" -> "value1")).fork
        fiber2 <- ctx.updateCursor("cursor1").fork
        fiber3 <- ctx.setPaginationVar("page", TemplateValue.Num(1)).fork

        _ <- fiber1.join
        _ <- fiber2.join
        _ <- fiber3.join

        headers <- ctx.getHeaders
        cursor  <- ctx.getCursor
        vars    <- ctx.getPaginationVars
      yield assertTrue(
        headers("header1") == "value1",
        cursor == Some("cursor1"),
        vars("page") == TemplateValue.Num(1)
      )
    },
    test("concurrent cursor updates preserve max value") {
      for
        ctx <- ZIO.service[RuntimeContext]

        // Concurrent cursor updates
        fibers <- ZIO.foreach(List("100", "200", "150", "300", "250"))(cursor => ctx.updateCursor(cursor).fork)

        _ <- ZIO.foreach(fibers)(_.join)

        finalCursor <- ctx.getCursor
      yield assertTrue(finalCursor == Some("300"))
    },
    test("layer creates RuntimeContext with initial state") {
      val initialState = """{"cursor": "initial-cursor"}"""

      val testLayer = for {
        mapper     <- ZIO.succeed(new ObjectMapper())
        configNode <- ZIO.succeed {
          val node = mapper.createObjectNode()
          node.put("test_key", "test_value")
          node
        }
        config = ConnectorConfig(configNode)
      } yield config

      (for
        ctx    <- ZIO.service[RuntimeContext]
        cursor <- ctx.getCursor
      yield assertTrue(cursor == Some("initial-cursor")))
        .provide(
          ZLayer.fromZIO(testLayer),
          RuntimeContext.Live.layer(initialState)
        )
    },
    test("empty layer creates RuntimeContext with no state") {
      (for
        ctx     <- ZIO.service[RuntimeContext]
        cursor  <- ctx.getCursor
        headers <- ctx.getHeaders
        vars    <- ctx.getPaginationVars
      yield assertTrue(
        cursor.isEmpty,
        headers.isEmpty,
        vars.isEmpty
      )).provide(RuntimeContext.Stub.layer)
    }
  ).provide(RuntimeContext.Stub.layer)
