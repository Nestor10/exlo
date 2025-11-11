package exlo.yaml.integration

import com.fasterxml.jackson.databind.ObjectMapper
import exlo.yaml.infra.HttpClient
import exlo.yaml.service.*
import exlo.yaml.template.TemplateValue
import zio.*
import zio.test.*

/**
 * Tests for incremental sync with state management.
 *
 * Verifies:
 *   - State JSON is parsed and available in templates
 *   - Templates can access {{ state.cursor }}, {{ state.lastTimestamp }}
 *   - Empty state is handled as empty object
 */
object IncrementalSyncSpec extends ZIOSpecDefault:

  def spec = suite("Incremental Sync")(
    test("parses state JSON and makes it available in templates") {
      val mapper = new ObjectMapper()

      // Simulate state from previous run
      val stateJson = """{"lastTimestamp": "2024-01-15T10:30:00Z", "lastId": 100}"""

      // Parse state into TemplateValue
      val jsonNode   = mapper.readTree(stateJson)
      val stateValue = TemplateValue.fromJsonNode(jsonNode)

      // Set state in RuntimeContext and render templates
      for
        _ <- RuntimeContext.setPaginationVar("state", stateValue)
        _ <- RuntimeContext.setPaginationVar(
          "config",
          TemplateValue.Obj(Map("api_key" -> Some(TemplateValue.Str("test-key"))))
        )

        url <- TemplateEngine.render("/api/users?since={{ state.lastTimestamp }}")

        idFilter <- TemplateEngine.render("id > {{ state.lastId }}")
      yield assertTrue(
        url == "/api/users?since=2024-01-15T10:30:00Z",
        idFilter == "id > 100"
      )
    }.provide(TemplateEngine.Live.layer, RuntimeContext.Stub.layer),
    test("handles empty state as empty object") {
      val emptyState = ""

      // Empty state should parse to empty object
      val stateValue =
        if emptyState.isEmpty then TemplateValue.Obj(Map.empty)
        else
          val mapper = new ObjectMapper()
          TemplateValue.fromJsonNode(mapper.readTree(emptyState))

      // Template with default value when state field is missing
      for
        _ <- RuntimeContext.setPaginationVar("state", stateValue)
        _ <- RuntimeContext.setPaginationVar("config", TemplateValue.Obj(Map.empty))

        // Jinjava should handle missing fields gracefully
        url <- TemplateEngine.render("/api/users?since={{ state.lastTimestamp | default('') }}")
      yield assertTrue(
        url == "/api/users?since=" // Empty default
      )
    }.provide(TemplateEngine.Live.layer, RuntimeContext.Stub.layer),
    test("parses nested state with multiple fields") {
      val mapper = new ObjectMapper()

      // Complex state with nested structure
      val stateJson = """{
        "lastTimestamp": "2024-01-15T10:30:00Z",
        "lastId": 100,
        "cursors": {
          "users": "user_cursor_123",
          "orders": "order_cursor_456"
        }
      }"""

      val jsonNode   = mapper.readTree(stateJson)
      val stateValue = TemplateValue.fromJsonNode(jsonNode)

      // Render templates using nested state values
      for
        _ <- RuntimeContext.setPaginationVar("state", stateValue)

        userCursor <- TemplateEngine.render("{{ state.cursors.users }}")

        orderCursor <- TemplateEngine.render("{{ state.cursors.orders }}")
      yield assertTrue(
        userCursor == "user_cursor_123",
        orderCursor == "order_cursor_456"
      )
    }.provide(TemplateEngine.Live.layer, RuntimeContext.Stub.layer),
    test("RuntimeContext updates cursor value from records") {
      val mapper = new ObjectMapper()

      // Create some test records with timestamps
      val record1 = mapper.readTree("""{"id": 1, "updated_at": "2024-01-15T10:00:00Z"}""")
      val record2 = mapper.readTree("""{"id": 2, "updated_at": "2024-01-15T12:00:00Z"}""")
      val record3 = mapper.readTree("""{"id": 3, "updated_at": "2024-01-15T11:00:00Z"}""")

      for
        // Update RuntimeContext with cursor values in non-sorted order
        _ <- RuntimeContext.updateCursor(record1.get("updated_at").asText())
        _ <- RuntimeContext.updateCursor(record2.get("updated_at").asText())
        _ <- RuntimeContext.updateCursor(record3.get("updated_at").asText())

        // Get state - should have the MAX timestamp
        stateJson <- RuntimeContext.getStateJson
      yield
        val state = mapper.readTree(stateJson)
        assertTrue(
          state.get("cursor").asText() == "2024-01-15T12:00:00Z" // Maximum value
        )
    }.provide(RuntimeContext.Stub.layer),
    test("RuntimeContext handles missing cursor field gracefully") {
      val mapper = new ObjectMapper()

      // Record without the cursor field
      val record = mapper.readTree("""{"id": 1, "name": "Test"}""")

      for
        // Don't update cursor if field is missing
        cursor    <- ZIO.succeed(Option(record.get("updated_at")))
        _         <- cursor match {
          case Some(node) => RuntimeContext.updateCursor(node.asText())
          case None       => ZIO.unit
        }
        stateJson <- RuntimeContext.getStateJson
      yield
      // State should be empty since no cursor was updated
      assertTrue(stateJson == "{}")
    }.provide(RuntimeContext.Stub.layer),
    test("RuntimeContext initializes with state from layer") {
      val mapper = new ObjectMapper()

      // Create environment with initial cursor
      val envWithState = ZLayer.make[RuntimeContext](
        ZLayer.fromZIO(
          ZIO.succeed(
            exlo.yaml.domain.ConnectorConfig(mapper.createObjectNode())
          )
        ),
        RuntimeContext.Live.layer("""{"cursor": "2024-01-01T00:00:00Z"}""")
      )

      for stateJson <- RuntimeContext.getStateJson
      yield assertTrue(stateJson.contains("2024-01-01T00:00:00Z"))
    }.provide(
      ZLayer.fromZIO(
        ZIO.succeed(
          exlo.yaml.domain.ConnectorConfig(new ObjectMapper().createObjectNode())
        )
      ) >>> RuntimeContext.Live.layer("""{"cursor": "2024-01-01T00:00:00Z"}""")
    )
  )
