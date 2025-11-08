package exlo.yaml.service

import exlo.yaml.domain.YamlRuntimeError
import exlo.yaml.spec.ConnectorSpec
import exlo.yaml.spec.given
import io.circe.yaml.parser
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

  /** Live implementation using circe-yaml for parsing. */
  case class Live() extends YamlSpecLoader:

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

        // Parse YAML to JSON
        json    <- ZIO
          .fromEither(parser.parse(content))
          .mapError(e =>
            YamlRuntimeError.InvalidSpec(
              path,
              s"Failed to parse YAML: ${e.getMessage}"
            )
          )

        // Decode to ConnectorSpec
        spec    <- ZIO
          .fromEither(json.as[ConnectorSpec])
          .mapError(e =>
            YamlRuntimeError.InvalidSpec(
              path,
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
