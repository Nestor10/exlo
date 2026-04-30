package exlo.runtime

/**
 * A record stamped with a monotonic seq# as it leaves the connector's stream.
 * The seq# is what the data sink reports back as its durable watermark and
 * what the [[WatermarkTracker]] uses to release pending state marks.
 */
final case class Sequenced(seq: Long, value: String)

/**
 * A state proposal awaiting durability. Captured when the connector emits an
 * `Emission.Mark`; the seq# is the seq# of the most-recently-emitted Record
 * at that moment. The runner releases this for state-store commit only once
 * the data sink's durable watermark has reached `seq`.
 */
final case class Pending[+S](seq: Long, state: S)
