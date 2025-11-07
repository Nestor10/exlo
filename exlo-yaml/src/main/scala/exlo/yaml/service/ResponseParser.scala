package exlo.yaml.service

import exlo.yaml.YamlRuntimeError
import exlo.yaml.spec.Extractor
import io.circe.Json
import io.circe.optics.JsonPath.*
import monocle.Optional
import zio.*

/**
 * Service for extracting records from JSON responses.
 *
 * Uses circe-optics for type-safe JSON traversal via field paths.
 */
trait ResponseParser:

  /**
   * Extract records from JSON response using extractor configuration.
   *
   * @param response
   *   JSON response from HTTP request
   * @param extractor
   *   Extraction configuration (field path)
   * @return
   *   List of extracted JSON records
   */
  def extract(
    response: Json,
    extractor: Extractor
  ): IO[Throwable, List[Json]]

object ResponseParser:

  /**
   * Accessor for extract.
   *
   * Use: `ResponseParser.extract(response, extractor)`
   */
  def extract(
    response: Json,
    extractor: Extractor
  ): ZIO[ResponseParser, Throwable, List[Json]] =
    ZIO.serviceWithZIO[ResponseParser](_.extract(response, extractor))

  /** Live implementation using circe-optics. */
  case class Live() extends ResponseParser:

    override def extract(
      response: Json,
      extractor: Extractor
    ): IO[Throwable, List[Json]] =
      extractor match
        case Extractor.DPath(fieldPath) =>
          if fieldPath.isEmpty then
            // Empty path means extract from root
            ZIO.succeed(
              response.asArray match
                case Some(array) => array.toList
                case None        => List(response)
            )
          else extractByPath(response, fieldPath.mkString("."))

    /**
     * Extract values from JSON using dot-separated field path.
     *
     * Examples:
     *   - "data" → extracts json.data
     *   - "data.items" → extracts json.data.items
     *   - "response.users" → extracts json.response.users
     *
     * If the extracted value is an array, returns its elements. If it's a
     * single object, returns it wrapped in a list.
     */
    private def extractByPath(
      json: Json,
      fieldPath: String
    ): IO[Throwable, List[Json]] =
      ZIO
        .attempt {
          // Split path into segments
          val segments = fieldPath.split("\\.").toList

          // Build optic from path segments
          val optic = segments.foldLeft[Optional[Json, Json]](
            Optional.id[Json]
          )((acc, segment) => acc.andThen(root.selectDynamic(segment).json))

          // Extract value using optic
          optic.getOption(json) match
            case None =>
              // Path doesn't exist - return empty list
              List.empty[Json]

            case Some(extracted) =>
              // If array, return elements; if object, wrap in list
              extracted.asArray match
                case Some(array) => array.toList
                case None        => List(extracted)
        }
        .mapError(e =>
          YamlRuntimeError.ParseError(
            fieldPath,
            s"Failed to extract: ${e.getMessage}"
          )
        )

  object Live:

    /**
     * ZLayer for ResponseParser.
     *
     * No dependencies - pure JSON transformation service.
     */
    val layer: ULayer[ResponseParser] =
      ZLayer.succeed(Live())
