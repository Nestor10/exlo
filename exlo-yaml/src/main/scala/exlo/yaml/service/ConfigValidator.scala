package exlo.yaml.service

import io.circe.Json
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
 * val schema = Json.obj(
 *   "type" -> Json.fromString("object"),
 *   "required" -> Json.arr(Json.fromString("client_id")),
 *   "properties" -> Json.obj(
 *     "client_id" -> Json.obj("type" -> Json.fromString("string")),
 *     "organization_ids" -> Json.obj(
 *       "type" -> Json.fromString("array"),
 *       "items" -> Json.obj("type" -> Json.fromString("string"))
 *     )
 *   )
 * )
 *
 * val config = Json.obj(
 *   "client_id" -> Json.fromString("abc123"),
 *   "organization_ids" -> Json.arr(Json.fromString("org1"), Json.fromString("org2"))
 * )
 *
 * validate(config, schema)  // Success(config)
 *
 * val badConfig = Json.obj("client_id" -> Json.fromInt(123))  // Wrong type
 * validate(badConfig, schema)  // Failure(ConfigValidationError)
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
  def validate(config: Json, schema: Json): IO[YamlRuntimeError.ConfigValidationError, Json]

object ConfigValidator:
  def validate(config: Json, schema: Json): ZIO[ConfigValidator, YamlRuntimeError.ConfigValidationError, Json] =
    ZIO.serviceWithZIO[ConfigValidator](_.validate(config, schema))
