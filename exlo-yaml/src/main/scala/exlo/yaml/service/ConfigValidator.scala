package exlo.yaml.service

import com.fasterxml.jackson.databind.JsonNode
import zio.*
import exlo.yaml.domain.YamlRuntimeError

/**
 * JSON Schema validation service.
 *
 * Design (Onion Architecture - Service Layer):
 * - Service trait defining validation capability
 * - No implementation details - just the interface
 * - Used by YamlConnector to validate user config against connector schema
 * - Implementation in infra layer (ConfigValidatorLive)
 *
 * Validates user-provided config against connector's connection_specification (JSON Schema).
 * Ensures config matches required structure before making API calls.
 *
 * Example:
 * {{{
 * // Schema as Jackson JsonNode:
 * {
 *   "type": "object",
 *   "required": ["client_id"],
 *   "properties": {
 *     "client_id": { "type": "string" },
 *     "organization_ids": {
 *       "type": "array",
 *       "items": { "type": "string" }
 *     }
 *   }
 * }
 *
 * // Config as JsonNode:
 * {
 *   "client_id": "abc123",
 *   "organization_ids": ["org1", "org2"]
 * }
 *
 * validate(config, schema)  // Success(config)
 *
 * // Bad config: wrong type
 * { "client_id": 123 }  // Failure(ConfigValidationError)
 * }}}
 */
trait ConfigValidator:

  /**
   * Validate config JSON against a JSON Schema.
   *
   * @param config
   *   User-provided configuration (from EXLO_CONNECTOR_CONFIG)
   * @param schema
   *   JSON Schema from connector's connection_specification
   * @return
   *   Validated config on success, ConfigValidationError on failure
   */
  def validate(config: JsonNode, schema: JsonNode): IO[YamlRuntimeError.ConfigValidationError, JsonNode]

object ConfigValidator:

  def validate(
    config: JsonNode,
    schema: JsonNode
  ): ZIO[ConfigValidator, YamlRuntimeError.ConfigValidationError, JsonNode] =
    ZIO.serviceWithZIO[ConfigValidator](_.validate(config, schema))
