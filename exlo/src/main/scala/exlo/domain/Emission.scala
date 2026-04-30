package exlo.domain

/**
 * What a connector yields on its `dataStream`.
 *
 *   - [[Emission.Record]] is opaque text. The framework is plumbing: record
 *     content is an upstream/downstream concern, never a framework concern.
 *     (Validation, schema enforcement, etc. are explicitly out of scope.)
 *   - [[Emission.Mark]] is a resume-point proposal: "if every Record I emitted
 *     before this is durable, then `state` is a safe resume point." It is not
 *     a commit instruction. The runner stamps each Mark with the seq# of the
 *     most-recently-emitted Record and only writes the underlying state to the
 *     StateStore once that seq# crosses the data sink's durable watermark.
 *
 * A connector that emits only Records is fully stateless from the framework's
 * point of view (nothing to commit). A connector that emits Marks gets
 * watermark-gated, at-least-once state advancement for free.
 */
enum Emission[+S]:
  case Record(value: String)
  case Mark(state: S)
