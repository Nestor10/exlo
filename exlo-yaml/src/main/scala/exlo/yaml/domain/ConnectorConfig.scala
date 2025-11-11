package exlo.yaml.domain

import com.fasterxml.jackson.databind.JsonNode

/**
 * Connector-specific configuration values.
 *
 * Represents dynamic, user-provided configuration that varies per connector deployment.
 *
 * Design (Onion Architecture - Domain Layer):
 * - Pure domain model - no ZIO effects, no external dependencies
 * - Represents validated connector config (YAML connection_specification schema)
 * - Loaded from EXLO_CONNECTOR_CONFIG environment variable (JSON string)
 * - Validated against connector's JSON Schema before construction
 * - Made available to Jinja2 templates as {{ config }}
 *
 * Type Choice:
 * - Uses Jackson JsonNode (not io.circe.Json or Map[String, String])
 * - Supports arrays: organization_ids: ["org1", "org2"]
 * - Supports nested objects: credentials: { type: "oauth", client_id: "..." }
 * - Supports all JSON types: strings, numbers, booleans, nulls, arrays, objects
 * - Zero conversion overhead with json-schema-validator (uses Jackson)
 * - Jinjava requires conversion: use JsonUtils.toAny to convert JsonNode → Java Objects
 *
 * Rationale:
 * YAML connectors define their own config schema using full JSON Schema spec.
 * We cannot know the structure at compile time. Examples:
 * - Shopify needs {{ config.shop }}, {{ config.api_token }}
 * - Snapchat needs {{ config.organization_ids }} (array)
 * - OAuth connectors need {{ config.credentials.client_id }} (nested object)
 *
 * Validation occurs in service layer via ConfigValidator before this model is constructed.
 *
 * Example Kubernetes Secret:
 * {{{
 * apiVersion: v1
 * kind: Secret
 * metadata:
 *   name: snapchat-config
 * stringData:
 *   EXLO_CONNECTOR_CONFIG: |
 *     {
 *       "client_id": "abc123",
 *       "client_secret": "secret",
 *       "organization_ids": ["org1", "org2", "org3"],
 *       "refresh_token": "refresh_token_value"
 *     }
 * }}}
 *
 * @param node
 *   Validated JSON configuration object (Jackson JsonNode)
 */
case class ConnectorConfig(node: JsonNode)
