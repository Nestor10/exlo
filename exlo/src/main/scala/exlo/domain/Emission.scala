package exlo.domain

/**
 * What a [[Stage]] yields on its output stream.
 *
 *   - [[Emission.Record]] is one record of type `O`. For inter-stage flow,
 *     records are typed however the stage chooses; for the leaf stage that
 *     writes to a `DataSink`, records are `String` (opaque text — the
 *     framework is plumbing, content is an upstream/downstream concern).
 *   - [[Emission.Mark]] is a resume-point proposal: "if every Record I
 *     emitted before this is durable, then `state` is a safe resume point."
 *     The runner stamps each Mark with the seq# of the most-recently-emitted
 *     Record and only writes the underlying state to the StateStore once
 *     that seq# crosses the data sink's durable watermark.
 *
 * A stage that emits only Records is fully stateless from the framework's
 * point of view (nothing to commit). A stage that emits Marks gets
 * watermark-gated, at-least-once state advancement for free.
 *
 * Variance: `O` and `S` are both covariant — emissions can be widened freely.
 */
enum Emission[+O, +S]:
  case Record(value: O) extends Emission[O, Nothing]
  case Mark(state: S)   extends Emission[Nothing, S]
