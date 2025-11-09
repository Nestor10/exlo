package exlo.yaml.infra

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import zio.*
import exlo.yaml.domain.{ConnectorConfig, YamlRuntimeError}
import exlo.yaml.service.ConfigValidator

/**
 * Loads and validates connector configuration from environment.
 *
 * Design (Onion Architecture - Infrastructure Layer):
 * - Reads EXLO_CONNECTOR_CONFIG environment variable
 * - Parses JSON string to Jackson JsonNode
 * - Validates against connector's JSON Schema
 * - Returns validated ConnectorConfig domain model
 *
 * Flow:
 * 1. Read env var EXLO_CONNECTOR_CONFIG
 * 2. Parse as JSON (fail if invalid JSON)
 * 3. Extract connection_specification from resolved YAML spec
 * 4. Validate config against schema (fail if validation errors)
 * 5. Return ConnectorConfig(validatedJsonNode)
 *
 * Example:
 * {{{
 * export EXLO_CONNECTOR_CONFIG='{
 *   "client_id": "abc123",
 *   "organization_ids": ["org1", "org2"],
 *   "region": "us"
 * }'
 *
 * ConfigLoader.loadAndValidate(connectionSpec)
 *   .provideLayer(ConfigValidatorLive.layer)
 * }}}
 */
object ConfigLoader:

  private val jsonMapper = new ObjectMapper()

  /**
   * Load config from EXLO_CONNECTOR_CONFIG and validate against schema.
   *
   * @param connectionSpec
   *   JSON Schema from connector's connection_specification
   * @return
   *   Validated ConnectorConfig
   */
  def loadAndValidate(connectionSpec: JsonNode): ZIO[ConfigValidator, YamlRuntimeError, ConnectorConfig] =
    (for
      // 1. Read env var
      configJsonString <- System
        .env("EXLO_CONNECTOR_CONFIG")
        .someOrFail(
          YamlRuntimeError.InvalidSpec(
            "EXLO_CONNECTOR_CONFIG",
            "Environment variable not set. Provide connector config as JSON string."
          )
        )

      // 2. Parse JSON using Jackson
      configNode       <- ZIO
        .attempt(jsonMapper.readTree(configJsonString))
        .mapError(err =>
          YamlRuntimeError.InvalidSpec(
            "EXLO_CONNECTOR_CONFIG",
            s"Invalid JSON: ${err.getMessage}"
          )
        )

      // 3. Validate against schema
      validatedNode    <- ConfigValidator
        .validate(configNode, connectionSpec)
        .mapError(e => e: YamlRuntimeError) // Widen to YamlRuntimeError

    // 4. Return domain model
    yield ConnectorConfig(validatedNode)).refineToOrDie[YamlRuntimeError]

  /**
   * Load config without validation (for testing or when schema not available).
   *
   * Use this sparingly - prefer loadAndValidate for production code.
   */
  def loadUnvalidated: Task[ConnectorConfig] =
    for
      configJsonString <- System
        .env("EXLO_CONNECTOR_CONFIG")
        .someOrFail(
          new RuntimeException("EXLO_CONNECTOR_CONFIG environment variable not set")
        )

      configNode <- ZIO
        .attempt(jsonMapper.readTree(configJsonString))
        .mapError(err => new RuntimeException(s"Invalid JSON in EXLO_CONNECTOR_CONFIG: ${err.getMessage}"))
    yield ConnectorConfig(configNode)
