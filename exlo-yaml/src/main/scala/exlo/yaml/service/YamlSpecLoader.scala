package exlo.yaml.service

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import exlo.yaml.domain.YamlRuntimeError
import exlo.yaml.infra.YamlToJsonResolver
import exlo.yaml.spec.ConnectorSpec
import zio.*

import scala.io.Source

/**
 * Service for loading and parsing YAML connector specifications.
 *
 * Handles file I/O and YAML parsing, converting declarative YAML specs into
 * typed ConnectorSpec ADTs.
 */
trait YamlSpecLoader:

  /**
   * Load YAML spec from filesystem path.
   *
   * @param path
   *   Filesystem path to YAML file
   * @return
   *   Parsed ConnectorSpec
   */
  def loadSpec(path: String): IO[Throwable, ConnectorSpec]

object YamlSpecLoader:

  /**
   * Accessor for loadSpec.
   *
   * Use: `YamlSpecLoader.loadSpec(path)`
   */
  def loadSpec(path: String): ZIO[YamlSpecLoader, Throwable, ConnectorSpec] =
    ZIO.serviceWithZIO[YamlSpecLoader](_.loadSpec(path))

  /**
   * Convenience helper that loads a spec without requiring the `YamlSpecLoader`
   * service in the environment. This is useful for early (synchronous) init
   * or for bootstrapping code that doesn't want to construct a ZLayer.
   */
  def loadSpecDirect(path: String): IO[Throwable, ConnectorSpec] =
    Live().loadSpec(path)

  /**
   * Parse YAML string directly into ConnectorSpec.
   *
   * Useful for:
   *   - Loading from environment variable (EXLO_CONNECTOR_SPEC)
   *   - Loading from HTTP response body
   *   - Testing with inline YAML
   *
   * @param yamlContent
   *   YAML string content
   * @return
   *   Parsed ConnectorSpec
   */
  def parseYamlString(yamlContent: String): IO[Throwable, ConnectorSpec] =
    Live.parseYamlString(yamlContent)

  /** Live implementation using Jackson YAML parser. */
  case class Live() extends YamlSpecLoader:

    private val yamlMapper: YAMLMapper =
      val mapper = new YAMLMapper()
      mapper.registerModule(DefaultScalaModule)
      mapper

    private val jsonMapper: ObjectMapper =
      val mapper = new ObjectMapper()
      mapper.registerModule(DefaultScalaModule)
      mapper

    override def loadSpec(path: String): IO[Throwable, ConnectorSpec] =
      for
        // Read file content
        content <- ZIO
          .attemptBlocking {
            val source = Source.fromFile(path)
            try source.mkString
            finally source.close()
          }
          .mapError(e =>
            YamlRuntimeError.InvalidSpec(
              path,
              s"Failed to read file: ${e.getMessage}"
            )
          )

        // Parse the YAML content
        spec    <- parseYaml(content).mapError {
          case e: YamlRuntimeError => e
          case e                   => YamlRuntimeError.InvalidSpec(path, e.getMessage)
        }
      yield spec

    /**
     * Parse YAML string into ConnectorSpec.
     *
     * Separated from loadSpec so it can be used for inline YAML and HTTP
     * responses.
     */
    def parseYaml(yamlContent: String): IO[Throwable, ConnectorSpec] =
      for
        // Parse YAML to JsonNode
        jsonNode <- ZIO
          .attempt(yamlMapper.readTree(yamlContent))
          .mapError(e =>
            YamlRuntimeError.InvalidSpec(
              "<inline>",
              s"Failed to parse YAML: ${e.getMessage}"
            )
          )

        // Resolve $ref references
        resolved <- YamlToJsonResolver.resolveYaml(yamlContent)

        // Deserialize JsonNode to ConnectorSpec
        spec <- ZIO
          .attempt(jsonMapper.treeToValue(resolved, classOf[ConnectorSpec]))
          .mapError(e =>
            YamlRuntimeError.InvalidSpec(
              "<inline>",
              s"Failed to decode ConnectorSpec: ${e.getMessage}"
            )
          )
      yield spec

  object Live:

    /**
     * ZLayer for YamlSpecLoader.
     *
     * No dependencies - pure file I/O service.
     */
    val layer: ULayer[YamlSpecLoader] =
      ZLayer.succeed(Live())

    /**
     * Parse YAML string without needing a service instance.
     *
     * Exposed as a static method for convenience.
     */
    def parseYamlString(yamlContent: String): IO[Throwable, ConnectorSpec] =
      Live().parseYaml(yamlContent)
