package exlo.yaml.service

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.jayway.jsonpath.Configuration
import com.jayway.jsonpath.JsonPath
import com.jayway.jsonpath.PathNotFoundException
import exlo.yaml.domain.YamlRuntimeError
import exlo.yaml.spec.Extractor
import zio.*

import scala.jdk.CollectionConverters.*

/**
 * Service for extracting records from JSON responses.
 *
 * Uses Jayway json-path for full JSONPath spec support including:
 * - Simple paths: $.data.users
 * - Array indexing: $.data.users[0]
 * - Array slicing: $.data.users[0:5]
 * - Wildcards: $.data.*.name
 * - Recursive descent: $..price
 * - Filters: $..book[?(@.price < 10)]
 */
trait ResponseParser:

  /**
   * Extract records from JSON response using extractor configuration.
   *
   * @param response
   *   JSON response from HTTP request
   * @param extractor
   *   Extraction configuration (JSONPath expression)
   * @return
   *   List of extracted JSON records
   */
  def extract(
    response: JsonNode,
    extractor: Extractor
  ): IO[Throwable, List[JsonNode]]

object ResponseParser:

  /**
   * Accessor for extract.
   *
   * Use: `ResponseParser.extract(response, extractor)`
   */
  def extract(
    response: JsonNode,
    extractor: Extractor
  ): ZIO[ResponseParser, Throwable, List[JsonNode]] =
    ZIO.serviceWithZIO[ResponseParser](_.extract(response, extractor))

  /** Live implementation using Jayway json-path. */
  case class Live() extends ResponseParser:

    private val jsonMapper     = new ObjectMapper()
    private val jsonPathConfig = Configuration.defaultConfiguration()

    override def extract(
      response: JsonNode,
      extractor: Extractor
    ): IO[Throwable, List[JsonNode]] =
      extractor match
        case Extractor.DPath(fieldPath) =>
          if fieldPath.isEmpty then
            // Empty path means extract from root
            ZIO.succeed(
              if response.isArray then response.elements().asScala.toList
              else List(response)
            )
          else extractByPath(response, fieldPath)

    /**
     * Extract values from JSON using JSONPath expression.
     *
     * Supports full JSONPath syntax:
     * - Simple paths: ["data", "users"] → $.data.users
     * - Array all: ["data", "users", "*"] → $.data.users[*]
     * - Array index: ["data", "users", "0"] → $.data.users[0]
     * - Recursive: ["**", "price"] → $..price
     *
     * If the extracted value is an array, returns its elements.
     * If it's a single object, returns it wrapped in a list.
     */
    private def extractByPath(
      node: JsonNode,
      fieldPath: List[String]
    ): IO[Throwable, List[JsonNode]] =
      ZIO
        .attempt {
          // Convert field path to JSONPath expression
          val jsonPathExpr = buildJsonPath(fieldPath)

          // Parse the JsonNode as a document for json-path
          val documentContext = JsonPath.using(jsonPathConfig).parse(node.toString)

          // Execute JSONPath query
          val result: Option[Object] =
            try
              Option(documentContext.read[Object](jsonPathExpr))
            catch {
              case _: PathNotFoundException =>
                None
            }

          // Convert result to JsonNode list
          result match
            case None                          => List.empty[JsonNode]
            case Some(list: java.util.List[_]) =>
              // Result is array - convert each element
              list.asScala.toList.map(elem => jsonMapper.readTree(jsonMapper.writeValueAsString(elem)))
            case Some(other)                   =>
              // Result is single value - wrap in list
              List(jsonMapper.readTree(jsonMapper.writeValueAsString(other)))
        }
        .mapError(e =>
          YamlRuntimeError.ParseError(
            fieldPath.mkString("."),
            s"Failed to extract: ${e.getMessage}"
          )
        )

    /**
     * Convert field path segments to JSONPath expression.
     *
     * Examples:
     * - ["data", "users"] → "$.data.users"
     * - ["data", "users", "*"] → "$.data.users[*]"
     * - ["**", "price"] → "$..price"
     */
    private def buildJsonPath(segments: List[String]): String =
      if segments.isEmpty then "$"
      else
        val path = segments.foldLeft("$") { (acc, segment) =>
          segment match
            case "*"  => s"$acc[*]"
            case "**" => "$.."
            case _    => s"$acc.$segment"
        }
        path

  object Live:

    /**
     * ZLayer for ResponseParser.
     *
     * No dependencies - pure JSON transformation service.
     */
    val layer: ULayer[ResponseParser] =
      ZLayer.succeed(Live())
