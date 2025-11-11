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
   * Load connector config from EXLO_CONNECTOR_CONFIG environment variable.
   *
   * Reads JSON string from env var and parses to ConnectorConfig.
   *
   * @return
   *   Connector configuration
   */
  def load: Task[ConnectorConfig] =
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
