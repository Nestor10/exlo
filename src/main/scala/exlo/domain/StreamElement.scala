package exlo.domain

/**
 * User-facing union type for emitting data and state checkpoints.
 *
 * Users emit two types of elements in their streams:
 * - Data: User's record as a string (any format: JSON, CSV, hex, etc.)
 * - Checkpoint: User's state as a JSON string
 *
 * Framework handles these differently:
 * - Data → wrapped in ExloRecord → written to Iceberg table
 * - Checkpoint → written to Iceberg snapshot summary properties
 */
sealed trait StreamElement

object StreamElement:

  /**
   * Data record containing user's payload string.
   *
   * @param record
   *   User's data as string - no format requirements. Framework never
   *   deserializes this.
   */
  case class Data(record: String) extends StreamElement

  /**
   * State checkpoint containing user's state as JSON.
   *
   * @param state
   *   User's state as JSON string. User chooses their JSON library.
   */
  case class Checkpoint(state: String) extends StreamElement
