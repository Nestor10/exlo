package exlo.domain

/**
 * Phantom marker for a connector's output.
 *
 * Tags exist purely at the type level. They let multiple `Source[T <: Tag]`
 * services coexist in a child connector's environment when it consumes from
 * several parents simultaneously — e.g. `OrdersConnector` requires
 * `Source[Users] & Source[Projects] & Source[Tenants]`. Without per-parent
 * phantom tags, all three would collapse to one `Source[String]` and the
 * env couldn't disambiguate.
 *
 * No record content lives at the tag layer; records on the wire are always
 * `String`. Concrete tags are typically empty marker traits in the connector's
 * package: `trait Users extends Tag`, etc.
 */
trait Tag
