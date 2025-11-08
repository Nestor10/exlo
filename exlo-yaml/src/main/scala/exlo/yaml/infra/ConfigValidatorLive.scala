package exlo.yaml.infra

import com.networknt.schema.JsonSchemaFactory
import com.networknt.schema.SpecVersion
import com.networknt.schema.ValidationMessage
import exlo.yaml.domain.YamlRuntimeError
import exlo.yaml.service.ConfigValidator
import io.circe.Json
import zio.*

import scala.jdk.CollectionConverters.*

/**
 * JSON Schema validator implementation using networknt json-schema-validator.
 *
 * Design (Onion Architecture - Infrastructure Layer):
 * - Implements ConfigValidator service trait
 * - Uses external library (json-schema-validator) for validation
 * - Converts between Circe Json and Jackson JsonNode
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
    config: Json,
    schema: Json
  ): IO[YamlRuntimeError.ConfigValidationError, Json] =
    ZIO
      .attempt {
        // Convert Circe Json to Jackson JsonNode
        val configNode = circeToJackson(config)
        val schemaNode = circeToJackson(schema)

        // Create JSON Schema validator
        val jsonSchema = schemaFactory.getSchema(schemaNode)

        // Validate
        val validationMessages: java.util.Set[ValidationMessage] = jsonSchema.validate(configNode)

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

  /**
   * Convert Circe Json to Jackson JsonNode.
   *
   * networknt json-schema-validator uses Jackson internally, so we need to convert.
   */
  private def circeToJackson(json: Json): com.fasterxml.jackson.databind.JsonNode =
    import com.fasterxml.jackson.databind.ObjectMapper
    val mapper = new ObjectMapper()
    mapper.readTree(json.noSpaces)

object ConfigValidatorLive:

  /** ZLayer for dependency injection. */
  val layer: ULayer[ConfigValidator] =
    ZLayer.succeed(new ConfigValidatorLive)
