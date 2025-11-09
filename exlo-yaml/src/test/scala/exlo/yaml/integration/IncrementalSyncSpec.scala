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

      // Build context with state
      val context = Map[String, TemplateValue](
        "state"  -> stateValue,
        "config" -> TemplateValue.Obj(Map("api_key" -> Some(TemplateValue.Str("test-key"))))
      )

      // Render template using state
      for
        url <- TemplateEngine.render(
          "/api/users?since={{ state.lastTimestamp }}",
          context
        )

        idFilter <- TemplateEngine.render(
          "id > {{ state.lastId }}",
          context
        )
      yield assertTrue(
        url == "/api/users?since=2024-01-15T10:30:00Z",
        idFilter == "id > 100"
      )
    }.provide(TemplateEngine.Live.layer),
    test("handles empty state as empty object") {
      val emptyState = ""

      // Empty state should parse to empty object
      val stateValue =
        if emptyState.isEmpty then TemplateValue.Obj(Map.empty)
        else
          val mapper = new ObjectMapper()
          TemplateValue.fromJsonNode(mapper.readTree(emptyState))

      val context = Map[String, TemplateValue](
        "state"  -> stateValue,
        "config" -> TemplateValue.Obj(Map.empty)
      )

      // Template with default value when state field is missing
      for
        // Jinjava should handle missing fields gracefully
        url <- TemplateEngine.render(
          "/api/users?since={{ state.lastTimestamp | default('') }}",
          context
        )
      yield assertTrue(
        url == "/api/users?since=" // Empty default
      )
    }.provide(TemplateEngine.Live.layer),
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

      val context = Map[String, TemplateValue](
        "state" -> stateValue
      )

      // Render templates using nested state values
      for
        userCursor <- TemplateEngine.render(
          "{{ state.cursors.users }}",
          context
        )

        orderCursor <- TemplateEngine.render(
          "{{ state.cursors.orders }}",
          context
        )
      yield assertTrue(
        userCursor == "user_cursor_123",
        orderCursor == "order_cursor_456"
      )
    }.provide(TemplateEngine.Live.layer),
    test("StateTracker updates cursor value from records") {
      val mapper = new ObjectMapper()

      // Create some test records with timestamps
      val record1 = mapper.readTree("""{"id": 1, "updated_at": "2024-01-15T10:00:00Z"}""")
      val record2 = mapper.readTree("""{"id": 2, "updated_at": "2024-01-15T12:00:00Z"}""")
      val record3 = mapper.readTree("""{"id": 3, "updated_at": "2024-01-15T11:00:00Z"}""")

      for
        // Update tracker with records in non-sorted order
        _ <- StateTracker.updateFromRecord(record1, Some("updated_at"))
        _ <- StateTracker.updateFromRecord(record2, Some("updated_at"))
        _ <- StateTracker.updateFromRecord(record3, Some("updated_at"))

        // Get state - should have the MAX timestamp
        stateJson <- StateTracker.getStateJson
      yield
        val state = mapper.readTree(stateJson)
        assertTrue(
          state.get("cursor").asText() == "2024-01-15T12:00:00Z" // Maximum value
        )
    }.provide(StateTracker.Live.layer),
    test("StateTracker handles records without cursor field") {
      val mapper = new ObjectMapper()

      // Record without the cursor field
      val record = mapper.readTree("""{"id": 1, "name": "Test"}""")

      for
        _         <- StateTracker.updateFromRecord(record, Some("updated_at"))
        stateJson <- StateTracker.getStateJson
      yield
      // State should be empty since no cursor field was found
      assertTrue(stateJson == "{}")
    }.provide(StateTracker.Live.layer),
    test("StateTracker resets to initial state") {
      val mapper = new ObjectMapper()

      // Start with some state
      val record = mapper.readTree("""{"id": 1, "updated_at": "2024-01-15T10:00:00Z"}""")

      for
        _      <- StateTracker.updateFromRecord(record, Some("updated_at"))
        state1 <- StateTracker.getStateJson

        // Reset to new state
        _      <- StateTracker.reset("""{"cursor": "2024-01-01T00:00:00Z"}""")
        state2 <- StateTracker.getStateJson
      yield assertTrue(
        state1.contains("2024-01-15T10:00:00Z"),
        state2.contains("2024-01-01T00:00:00Z")
      )
    }.provide(StateTracker.Live.layer)
  )
