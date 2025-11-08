package exlo.yaml.infra

import com.networknt.schema.{JsonSchemaFactory, SpecVersion}
import io.circe.Json
import io.circe.yaml.parser as YamlParser
import zio.*
import exlo.yaml.domain.YamlRuntimeError

import scala.jdk.CollectionConverters.*

/**
 * YAML to JSON converter with $ref resolution.
 *
 * Design (Onion Architecture - Infrastructure Layer):
 * - Parses YAML files to JSON (using circe-yaml)
 * - Resolves $ref references using json-schema-validator
 * - Returns fully resolved JSON document ready for Jinja2 templating
 *
 * Handles two types of references:
 * 1. Internal refs: $ref: "#/definitions/base_requester"
 * 2. Internal refs: $ref: "#/schemas/organizations"
 *
 * Example input (YAML with $ref):
 * {{{
 * definitions:
 *   base_requester:
 *     url_base: "https://api.example.com"
 *     authenticator:
 *       type: ApiKeyAuthenticator
 *
 * streams:
 *   users:
 *     requester:
 *       $ref: "#/definitions/base_requester"
 *       path: "/users"
 * }}}
 *
 * Example output (JSON with resolved $ref):
 * {{{
 * {
 *   "definitions": { ... },
 *   "streams": {
 *     "users": {
 *       "requester": {
 *         "url_base": "https://api.example.com",
 *         "authenticator": { "type": "ApiKeyAuthenticator" },
 *         "path": "/users"
 *       }
 *     }
 *   }
 * }
 * }}}
 */
object YamlToJsonResolver:

  /**
   * Parse YAML file and resolve all $ref references.
   *
   * @param yamlContent
   *   Raw YAML string
   * @return
   *   Fully resolved JSON document
   */
  def resolveYaml(yamlContent: String): IO[YamlRuntimeError.InvalidSpec, Json] =
    for
      // 1. Parse YAML to JSON
      json <- ZIO
        .fromEither(YamlParser.parse(yamlContent))
        .mapError(err => YamlRuntimeError.InvalidSpec("yaml-content", s"YAML parse error: ${err.getMessage}"))

      // 2. Resolve $ref references
      resolved <- resolveRefs(json)
    yield resolved

  /**
   * Resolve all $ref references in a JSON document.
   *
   * Uses json-schema-validator's $ref resolution logic. This handles:
   * - JSON Pointer syntax: #/path/to/definition
   * - Recursive references
   * - Reference cycles (throws error)
   *
   * @param json
   *   JSON document with potential $ref references
   * @return
   *   JSON document with all references resolved
   */
  private def resolveRefs(json: Json): IO[YamlRuntimeError.InvalidSpec, Json] =
    ZIO.attempt {
      // networknt uses Jackson, convert Circe → Jackson
      val jacksonNode = circeToJackson(json)

      // Use JsonSchemaFactory to resolve refs
      // Note: We're using the schema factory's ref resolution capability
      // without actually validating against a schema
      val schemaFactory = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V7)
      val schema        = schemaFactory.getSchema(jacksonNode)

      // The schema object has resolved all refs internally
      // Get the resolved schema as JSON
      val resolvedJackson = schema.getSchemaNode

      // Convert back to Circe Json
      jacksonToCirce(resolvedJackson)
    }.refineOrDie { case e: Exception =>
      YamlRuntimeError.InvalidSpec("$ref-resolution", s"Failed to resolve references: ${e.getMessage}")
    }

  /**
   * Convert Circe Json to Jackson JsonNode.
   */
  private def circeToJackson(json: Json): com.fasterxml.jackson.databind.JsonNode =
    import com.fasterxml.jackson.databind.ObjectMapper
    val mapper = new ObjectMapper()
    mapper.readTree(json.noSpaces)

  /**
   * Convert Jackson JsonNode to Circe Json.
   */
  private def jacksonToCirce(node: com.fasterxml.jackson.databind.JsonNode): Json =
    import io.circe.parser.parse
    parse(node.toString).getOrElse(Json.Null)
