package exlo.yaml.service

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import zio.*

/**
 * Service for tracking incremental sync state during stream processing.
 *
 * Following Zionomicon Chapter 9 (Ref - Shared State):
 *   - Uses Ref[StreamState] for fiber-safe mutable state
 *   - All state updates are atomic (cursor, timestamps, etc.)
 *   - Single Ref contains all related state (consistency guarantee)
 *
 * This service sits at the app level (in YamlConnector) and is updated
 * as records flow through the stream. At checkpoint time, the current
 * state is serialized to JSON.
 */
trait StateTracker:

  /**
   * Update state based on a record.
   *
   * If cursorField is configured, extracts the cursor value from the record
   * and updates state with the maximum value seen.
   *
   * @param record
   *   JsonNode record from API
   * @param cursorField
   *   Optional field name to extract cursor from (e.g., "updated_at", "id")
   * @return
   *   Effect that updates the state
   */
  def updateFromRecord(record: JsonNode, cursorField: Option[String]): UIO[Unit]

  /**
   * Get current state as JSON string for checkpointing.
   *
   * @return
   *   JSON string like {"cursor": "2024-01-15T10:30:00Z"}
   */
  def getStateJson: UIO[String]

  /**
   * Reset state (used between batches/streams).
   *
   * @param initialState
   *   JSON string to parse and set as initial state
   * @return
   *   Effect that resets the state
   */
  def reset(initialState: String): Task[Unit]

object StateTracker:

  /**
   * Incremental sync state.
   *
   * All fields that need to be consistent with each other are in a single
   * case class (Zionomicon best practice).
   *
   * @param cursor
   *   Current cursor value (max seen so far)
   * @param customFields
   *   Additional state fields for complex scenarios
   */
  case class StreamState(
    cursor: Option[String] = None,
    customFields: Map[String, String] = Map.empty
  )

  /**
   * Accessor for updateFromRecord.
   */
  def updateFromRecord(
    record: JsonNode,
    cursorField: Option[String]
  ): ZIO[StateTracker, Nothing, Unit] =
    ZIO.serviceWithZIO[StateTracker](_.updateFromRecord(record, cursorField))

  /**
   * Accessor for getStateJson.
   */
  def getStateJson: ZIO[StateTracker, Nothing, String] =
    ZIO.serviceWithZIO[StateTracker](_.getStateJson)

  /**
   * Accessor for reset.
   */
  def reset(initialState: String): ZIO[StateTracker, Throwable, Unit] =
    ZIO.serviceWithZIO[StateTracker](_.reset(initialState))

  /**
   * Live implementation using Ref.
   */
  case class Live(stateRef: Ref[StreamState]) extends StateTracker:

    override def updateFromRecord(
      record: JsonNode,
      cursorField: Option[String]
    ): UIO[Unit] =
      cursorField match
        case Some(fieldName) =>
          // Extract cursor value from record
          val newCursor = Option(record.get(fieldName)).map(_.asText())

          newCursor match
            case Some(cursorValue) =>
              // Atomically update state with max cursor value
              stateRef.update { state =>
                state.cursor match
                  case Some(current) if current > cursorValue =>
                    state // Keep current (it's newer)
                  case _ =>
                    state.copy(cursor = Some(cursorValue)) // Update to new max
              }
            case None =>
              ZIO.unit // No cursor in record, don't update

        case None =>
          ZIO.unit // No cursor field configured, don't track

    override def getStateJson: UIO[String] =
      for
        state <- stateRef.get
        json = state.cursor match
          case Some(cursor) =>
            s"""{"cursor": "$cursor"}"""
          case None =>
            "{}"
      yield json

    override def reset(initialState: String): Task[Unit] =
      ZIO.attempt {
        if initialState.isEmpty then
          StreamState()
        else
          // Parse JSON state
          val mapper = new ObjectMapper()
          val jsonNode = mapper.readTree(initialState)
          
          val cursor = Option(jsonNode.get("cursor")).map(_.asText())
          StreamState(cursor = cursor)
      }.flatMap(newState => stateRef.set(newState))

  object Live:
    /**
     * ZLayer for StateTracker.
     *
     * Creates a Ref with empty initial state.
     */
    val layer: ULayer[StateTracker] =
      ZLayer {
        for ref <- Ref.make(StreamState())
        yield Live(ref)
      }

    /**
     * ZLayer that initializes with given state.
     *
     * @param initialState
     *   JSON string to parse as initial state
     */
    def layerWithState(initialState: String): TaskLayer[StateTracker] =
      ZLayer {
        for
          initialStreamState <- ZIO.attempt {
            if initialState.isEmpty then
              StreamState()
            else
              val mapper = new ObjectMapper()
              val jsonNode = mapper.readTree(initialState)
              val cursor = Option(jsonNode.get("cursor")).map(_.asText())
              StreamState(cursor = cursor)
          }
          ref <- Ref.make(initialStreamState)
        yield Live(ref)
      }
