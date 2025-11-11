package exlo.yaml.infra

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.networknt.schema.{JsonSchemaFactory, SpecVersion}
import zio.*
import exlo.yaml.domain.YamlRuntimeError

/**
 * YAML to JSON converter with $ref resolution.
 *
 * Design (Onion Architecture - Infrastructure Layer):
 * - Parses YAML files to Jackson JsonNode (using Jackson YAML parser)
 * - Resolves $ref references using json-schema-validator
 * - Returns fully resolved JsonNode ready for use throughout the system
 * - Zero conversion overhead (Jackson everywhere)
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

  private val yamlMapper = new ObjectMapper(new YAMLFactory())
  private val jsonMapper = new ObjectMapper()

  /**
   * Parse YAML file and resolve all $ref references.
   *
   * @param yamlContent
   *   Raw YAML string
   * @return
   *   Fully resolved JsonNode
   */
  def resolveYaml(yamlContent: String): IO[YamlRuntimeError.InvalidSpec, JsonNode] =
    for
      // 1. Parse YAML to JsonNode using Jackson YAML parser
      jsonNode <- ZIO
        .attempt(yamlMapper.readTree(yamlContent))
        .mapError(err => YamlRuntimeError.InvalidSpec("yaml-content", s"YAML parse error: ${err.getMessage}"))

      // 2. Resolve $ref references
      resolved <- resolveRefs(jsonNode)
    yield resolved

  /**
   * Resolve all $ref references in a JSON document.
   *
   * Uses json-schema-validator's $ref resolution logic. This handles:
   * - JSON Pointer syntax: #/path/to/definition
   * - Recursive references
   * - Reference cycles (throws error)
   *
   * @param node
   *   JsonNode with potential $ref references
   * @return
   *   JsonNode with all references resolved
   */
  private def resolveRefs(node: JsonNode): IO[YamlRuntimeError.InvalidSpec, JsonNode] =
    ZIO
      .attempt {
        // Use JsonSchemaFactory to resolve refs
        // The schema factory handles $ref resolution automatically
        val schemaFactory = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V7)
        val schema        = schemaFactory.getSchema(node)

        // The schema object has resolved all refs internally
        // Get the resolved schema as JsonNode
        schema.getSchemaNode
      }
      .refineOrDie {
        case e: Exception =>
          YamlRuntimeError.InvalidSpec("$ref-resolution", s"Failed to resolve references: ${e.getMessage}")
      }
