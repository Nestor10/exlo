package exlo.yaml.infra

import com.fasterxml.jackson.databind.JsonNode
import com.networknt.schema.JsonSchemaFactory
import com.networknt.schema.SpecVersion
import com.networknt.schema.ValidationMessage
import exlo.yaml.domain.YamlRuntimeError
import exlo.yaml.service.ConfigValidator
import zio.*

import scala.jdk.CollectionConverters.*

/**
 * JSON Schema validator implementation using networknt json-schema-validator.
 *
 * Design (Onion Architecture - Infrastructure Layer):
 * - Implements ConfigValidator service trait
 * - Uses external library (json-schema-validator) for validation
 * - Works directly with Jackson JsonNode (zero conversion overhead)
 * - Provides ZLayer for dependency injection
 *
 * Validates user config against connector's JSON Schema (connection_specification).
 * Supports full JSON Schema Draft 7 spec: arrays, objects, enums, oneOf, validation rules.
 *
 * Example validation errors:
 * - Missing required field: "organization_ids: required field missing"
 * - Wrong type: "organization_ids: expected array, got string"
 * - Enum violation: "region: must be one of [us, eu, asia], got 'invalid'"
 * - Pattern mismatch: "api_token: does not match pattern '^shpat_[a-zA-Z0-9]+$'"
 */
final class ConfigValidatorLive extends ConfigValidator:

  private val schemaFactory = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V7)

  override def validate(
    config: JsonNode,
    schema: JsonNode
  ): IO[YamlRuntimeError.ConfigValidationError, JsonNode] =
    ZIO
      .attempt {
        // Create JSON Schema validator from schema
        val jsonSchema = schemaFactory.getSchema(schema)

        // Validate config against schema
        val validationMessages: java.util.Set[ValidationMessage] = jsonSchema.validate(config)

        if validationMessages.isEmpty then config // Valid!
        else
          // Collect all validation errors
          val errors = validationMessages.asScala.map { msg =>
            // ValidationMessage.toString() includes path and message
            msg.toString
          }.toList
          throw YamlRuntimeError.ConfigValidationError(errors)
      }
      .refineOrDie {
        case e: YamlRuntimeError.ConfigValidationError =>
          e
      }

object ConfigValidatorLive:

  /** ZLayer for dependency injection. */
  val layer: ULayer[ConfigValidator] =
    ZLayer.succeed(new ConfigValidatorLive)
