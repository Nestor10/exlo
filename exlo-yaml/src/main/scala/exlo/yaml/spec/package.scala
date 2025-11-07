package exlo.yaml

/**
 * Domain models for Airbyte-style YAML connector specifications.
 *
 * These are pure data types (ADTs) that represent the declarative connector spec.
 * Following FP principles: immutable, no behavior, just data.
 *
 * The interpreter (in `exlo.yaml.interpreter`) transforms these ADTs into ZIO effects.
 */
package object spec:

  /**
   * HTTP method for API requests.
   */
  enum HttpMethod:
    case GET, POST

  /**
   * Authentication configuration for API requests.
   *
   * Sealed trait ensures exhaustive pattern matching at compile time.
   */
  sealed trait Auth

  object Auth:
    /**
     * No authentication required.
     */
    case object NoAuth extends Auth

    /**
     * API key authentication via header.
     *
     * @param header
     *   Header name (e.g., "X-API-Key")
     * @param token
     *   API token value (supports Jinja templates: "{{ config.api_key }}")
     */
    case class ApiKey(header: String, token: String) extends Auth

    /**
     * Bearer token authentication.
     *
     * @param token
     *   Bearer token value (supports Jinja templates: "{{ config.token }}")
     */
    case class Bearer(token: String) extends Auth

    /**
     * OAuth 2.0 Client Credentials flow.
     *
     * Automatically obtains and refreshes access tokens.
     *
     * @param tokenUrl
     *   OAuth token endpoint URL
     * @param clientId
     *   OAuth client ID (supports Jinja templates: "{{ config.client_id }}")
     * @param clientSecret
     *   OAuth client secret (supports Jinja templates: "{{ config.client_secret }}")
     * @param scopes
     *   Optional space-separated scopes (e.g., "read write")
     */
    case class OAuth(
      tokenUrl: String,
      clientId: String,
      clientSecret: String,
      scopes: Option[String] = None
    ) extends Auth

  /**
   * HTTP request specification.
   *
   * @param url
   *   Full URL (supports Jinja templates: "https://{{ config.shop }}.example.com/api")
   * @param method
   *   HTTP method (GET or POST)
   * @param headers
   *   Additional headers (values support Jinja templates)
   * @param params
   *   Query parameters (values support Jinja templates)
   * @param auth
   *   Authentication configuration
   * @param body
   *   Request body for POST/PUT requests (supports Jinja templates for JSON)
   */
  case class Requester(
    url: String,
    method: HttpMethod = HttpMethod.GET,
    headers: Map[String, String] = Map.empty,
    params: Map[String, String] = Map.empty,
    auth: Auth = Auth.NoAuth,
    body: Option[String] = None
  )

  /**
   * Pagination strategy for multi-page API responses.
   *
   * Sealed trait for compile-time exhaustiveness checking.
   */
  sealed trait PaginationStrategy

  object PaginationStrategy:
    /**
     * No pagination (single page response).
     */
    case object NoPagination extends PaginationStrategy

    /**
     * Page number based pagination.
     *
     * Example: /api/users?page=1, /api/users?page=2
     *
     * @param pageSize
     *   Number of records per page
     * @param startFrom
     *   Initial page number (usually 0 or 1)
     */
    case class PageIncrement(pageSize: Int, startFrom: Int = 0) extends PaginationStrategy

    /**
     * Offset-based pagination.
     *
     * Example: /api/users?offset=0&limit=50, /api/users?offset=50&limit=50
     *
     * @param pageSize
     *   Number of records per page (the "limit")
     */
    case class OffsetIncrement(pageSize: Int) extends PaginationStrategy

    /**
     * Cursor-based pagination using a value from the response.
     *
     * Example: response contains {"next_page": "cursor_abc"} Most flexible and recommended for
     * Airbyte-style connectors.
     *
     * @param cursorValue
     *   Jinja template to extract cursor from response (e.g., "{{ response.next_page }}")
     * @param stopCondition
     *   Jinja template that evaluates to boolean (e.g., "{{ not response.next_page }}")
     */
    case class CursorPagination(cursorValue: String, stopCondition: String) extends PaginationStrategy

  /**
   * Record extraction configuration.
   *
   * Extracts records from JSON responses using JSONPath-style selectors.
   */
  sealed trait Extractor

  object Extractor:
    /**
     * JSONPath-based extractor.
     *
     * Example: ["data", "users", "*"] extracts all users from {"data": {"users": [...]}}
     *
     * @param fieldPath
     *   Path segments. Use "*" for array wildcard.
     */
    case class DPath(fieldPath: List[String]) extends Extractor

  /**
   * Record selector: extraction + optional filtering.
   *
   * @param extractor
   *   How to extract records from the response
   * @param filter
   *   Optional Jinja condition to filter records (e.g., "{{ record.age > 18 }}")
   */
  case class RecordSelector(
    extractor: Extractor,
    filter: Option[String] = None
  )

  /**
   * Complete stream specification.
   *
   * Defines how to fetch and process data from an API endpoint.
   *
   * @param name
   *   Stream name (e.g., "orders", "users")
   * @param requester
   *   HTTP request configuration
   * @param recordSelector
   *   How to extract records from responses
   * @param paginator
   *   Pagination strategy (optional, defaults to NoPagination)
   */
  case class StreamSpec(
    name: String,
    requester: Requester,
    recordSelector: RecordSelector,
    paginator: PaginationStrategy = PaginationStrategy.NoPagination
  )

  /**
   * Top-level connector specification.
   *
   * Can contain multiple streams (Phase 2 feature).
   *
   * @param streams
   *   List of stream configurations
   */
  case class ConnectorSpec(
    streams: List[StreamSpec]
  )

  // Circe codecs for automatic JSON/YAML deserialization
  import io.circe.{Decoder, Encoder}
  import io.circe.generic.semiauto.*

  given Decoder[HttpMethod] = Decoder[String].emap {
    case "GET"  => Right(HttpMethod.GET)
    case "POST" => Right(HttpMethod.POST)
    case other  => Left(s"Unknown HTTP method: $other")
  }

  given Encoder[HttpMethod] = Encoder[String].contramap(_.toString)

  given Decoder[Auth] = Decoder.instance { cursor =>
    cursor.downField("type").as[String].flatMap {
      case "NoAuth" => Right(Auth.NoAuth)
      case "ApiKey" =>
        for
          header <- cursor.downField("header").as[String]
          token  <- cursor.downField("token").as[String]
        yield Auth.ApiKey(header, token)
      case "Bearer" =>
        cursor.downField("token").as[String].map(Auth.Bearer.apply)
      case "OAuth"  =>
        for
          tokenUrl     <- cursor.downField("tokenUrl").as[String]
          clientId     <- cursor.downField("clientId").as[String]
          clientSecret <- cursor.downField("clientSecret").as[String]
          scopes       <- cursor.downField("scopes").as[Option[String]]
        yield Auth.OAuth(tokenUrl, clientId, clientSecret, scopes)
      case other    => Left(io.circe.DecodingFailure(s"Unknown auth type: $other", cursor.history))
    }
  }

  // Custom decoder for Extractor that unwraps DPath fields directly
  given Decoder[Extractor] = Decoder.instance { cursor =>
    cursor.downField("fieldPath").as[List[String]].map(Extractor.DPath.apply)
  }

  // Custom decoder for PaginationStrategy that uses "type" field for discrimination
  given Decoder[PaginationStrategy] = Decoder.instance { cursor =>
    cursor.downField("type").as[String].flatMap {
      case "NoPagination"     => Right(PaginationStrategy.NoPagination)
      case "PageIncrement"    =>
        for
          pageSize  <- cursor.downField("pageSize").as[Int]
          startFrom <- cursor.downField("startFrom").as[Option[Int]]
        yield PaginationStrategy.PageIncrement(pageSize, startFrom.getOrElse(0))
      case "OffsetIncrement"  =>
        cursor.downField("pageSize").as[Int].map(PaginationStrategy.OffsetIncrement.apply)
      case "CursorPagination" =>
        for
          cursorValue   <- cursor.downField("cursorValue").as[String]
          stopCondition <- cursor.downField("stopCondition").as[String]
        yield PaginationStrategy.CursorPagination(cursorValue, stopCondition)
      case other              => Left(io.circe.DecodingFailure(s"Unknown pagination type: $other", cursor.history))
    }
  }

  given Decoder[Requester] = deriveDecoder[Requester]
  given Decoder[RecordSelector] = deriveDecoder[RecordSelector]
  given Decoder[StreamSpec] = deriveDecoder[StreamSpec]
  given Decoder[ConnectorSpec] = deriveDecoder[ConnectorSpec]
