package exlo.yaml.infra

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import exlo.yaml.domain.YamlRuntimeError
import io.zenwave360.jsonrefparser.*
import zio.*


object YamlToJsonResolver:

  private val yamlMapper = new ObjectMapper(new YAMLFactory())
  private val jsonMapper = new ObjectMapper()

  /**
   * Parse YAML file and resolve all $ref references.
   *
   * Uses json-schema-ref-parser-jvm to parse and dereference all $ref.
   *
   * @param yamlContent
   *   Raw YAML string
   * @return
   *   Fully resolved JsonNode
   */
  def resolveYaml(yamlContent: String): IO[YamlRuntimeError.InvalidSpec, JsonNode] =
    ZIO
      .attempt {
        // Use $RefParser with the YAML string directly
        val parser = new $RefParser(yamlContent)
        val refs   = parser
          .parse()
          .dereference()
          .getRefs()

        // Get the fully resolved schema as an Object (returns Java Map/List)
        val resolved: Any = refs.schema()

        // Convert to JSON string first, then parse back to JsonNode
        // This avoids class cast issues with Jackson's type system
        val jsonString = jsonMapper.writeValueAsString(resolved)
        jsonMapper.readTree(jsonString)
      }
      .refineOrDie {
        case e: Exception =>
          YamlRuntimeError.InvalidSpec("$ref-resolution", s"Failed to resolve references: ${e.getMessage}")
      }
