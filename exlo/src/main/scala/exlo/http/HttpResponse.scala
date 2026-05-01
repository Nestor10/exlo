package exlo.http

import zio.Chunk
import zio.json.ast.Json

/**
 * The response object passed to user-defined methods (`records`, `nextCtx`,
 * `nextState`). [[exlo.http.HttpExec]] reads the body and parses it as JSON
 * before invoking user code, so user methods are pure (no `IO` around JSON
 * access). Non-JSON bodies surface as `ExloError.ConnectorFailure` from
 * `HttpExec.run` before user code is reached.
 */
final case class HttpResponse(
    status:  Int,
    headers: Map[String, String],
    body:    String,
    json:    Json
)

object HttpResponse:

  /** Convenience extensions for navigating a `Json` AST without ceremony.
   *  zio-json already provides `asString`, `asArray`, `asNumber`, `asObject`,
   *  `asBoolean`, `asNull` — so we only add what's missing: field lookup. */
  extension (j: Json)
    /** Look up a field by name on a `Json.Obj`. None if `j` isn't an object
     *  or the field isn't present. */
    def field(name: String): Option[Json] = j match
      case Json.Obj(fields) => fields.collectFirst { case (n, v) if n == name => v }
      case _                => None
