package exlo.yaml.interpreter

import exlo.domain.StreamElement
import exlo.yaml.service.*
import exlo.yaml.spec.*
import io.circe.Json
import zio.*
import zio.stream.*

import scala.jdk.CollectionConverters.*

/**
 * Interprets YAML spec ADTs into executable ZIO effects.
 *
 * Following functional programming principles: pure interpreter functions that
 * convert data (ADTs) into behavior (ZIO effects).
 *
 * The interpreter depends on services (HttpClient, TemplateEngine, etc.) via
 * ZIO environment, following Zionomicon Chapter 17 dependency injection
 * patterns.
 */
object YamlInterpreter:

  /**
   * Interpret a StreamSpec into a stream of JSON records.
   *
   * This is the main entry point that orchestrates:
   *   1. Build initial HTTP request from Requester
   *   1. Execute request with pagination
   *   1. Extract records from each response
   *   1. Apply optional filters
   *
   * @param spec
   *   Stream specification from YAML
   * @param context
   *   Template context (config values, state)
   * @return
   *   Stream of JSON records
   */
  def interpretStream(
    spec: StreamSpec,
    context: Map[String, Any]
  ): ZStream[
    HttpClient & TemplateEngine & ResponseParser & Authenticator,
    Throwable,
    Json
  ] =
    spec.paginator match
      case PaginationStrategy.NoPagination =>
        // Single page - execute once
        ZStream
          .fromZIO(executeRequest(spec.requester, context))
          .flatMap(response => extractRecords(response, spec.recordSelector, context))

      case pageIncrement: PaginationStrategy.PageIncrement =>
        interpretPageIncrement(spec, pageIncrement, context)

      case offsetIncrement: PaginationStrategy.OffsetIncrement =>
        interpretOffsetIncrement(spec, offsetIncrement, context)

      case cursorPagination: PaginationStrategy.CursorPagination =>
        interpretCursorPagination(spec, cursorPagination, context)

  /**
   * Execute a single HTTP request with template rendering and auth.
   *
   * @param requester
   *   Request specification
   * @param context
   *   Template context for rendering
   * @return
   *   JSON response
   */
  private def executeRequest(
    requester: Requester,
    context: Map[String, Any]
  ): ZIO[
    HttpClient & TemplateEngine & Authenticator,
    Throwable,
    Json
  ] =
    for
      // Render URL template
      url <- TemplateEngine.render(requester.url, context)

      // Render header values
      renderedHeaders <- ZIO
        .foreach(requester.headers.toList) {
          case (k, v) =>
            TemplateEngine.render(v, context).map(k -> _)
        }
        .map(_.toMap)

      // Render query parameter values
      renderedParams  <- ZIO
        .foreach(requester.params.toList) {
          case (k, v) =>
            TemplateEngine.render(v, context).map(k -> _)
        }
        .map(_.toMap)

      // Apply authentication
      headersWithAuth <- Authenticator.authenticate(
        requester.auth,
        renderedHeaders
      )

      // Render body template (if present)
      renderedBody    <- requester.body match
        case Some(bodyTemplate) =>
          TemplateEngine.render(bodyTemplate, context).map(Some(_))
        case None               => ZIO.succeed(None)

      // Execute HTTP request
      response        <- HttpClient.execute(
        url,
        requester.method,
        headersWithAuth,
        renderedParams,
        renderedBody
      )
    yield response

  /**
   * Extract records from response using RecordSelector.
   *
   * @param response
   *   JSON response from API
   * @param selector
   *   Record extraction configuration
   * @param context
   *   Template context for filter evaluation
   * @return
   *   Stream of extracted JSON records
   */
  private def extractRecords(
    response: Json,
    selector: RecordSelector,
    context: Map[String, Any]
  ): ZStream[
    TemplateEngine & ResponseParser,
    Throwable,
    Json
  ] =
    ZStream
      .fromIterableZIO(
        ResponseParser.extract(response, selector.extractor)
      )
      .filterZIO { record =>
        selector.filter match
          case None             => ZIO.succeed(true)
          case Some(filterExpr) =>
            // Add record to context for filter evaluation (convert to Java for Jinjava)
            val filterContext = context + ("record" -> jsonToJava(record))
            TemplateEngine.evaluateCondition(filterExpr, filterContext)
      }

  /**
   * Interpret page increment pagination.
   *
   * Continues fetching pages until no more records are returned.
   *
   * Example: /api/users?page=1, /api/users?page=2, ...
   */
  private def interpretPageIncrement(
    spec: StreamSpec,
    pagination: PaginationStrategy.PageIncrement,
    context: Map[String, Any]
  ): ZStream[
    HttpClient & TemplateEngine & ResponseParser & Authenticator,
    Throwable,
    Json
  ] =
    ZStream
      .unfoldZIO(pagination.startFrom) { pageNum =>
        val pageContext = context + ("page" -> pageNum)

        executeRequest(spec.requester, pageContext).flatMap { response =>
          ResponseParser.extract(response, spec.recordSelector.extractor).flatMap { records =>
            if records.isEmpty then
              // No more records - stop pagination
              ZIO.succeed(None)
            else
              // Return records and next page number
              ZIO.succeed(Some((records, pageNum + 1)))
          }
        }
      }
      .flatMap(records => ZStream.fromIterable(records))
      .filterZIO { record =>
        spec.recordSelector.filter match
          case None             => ZIO.succeed(true)
          case Some(filterExpr) =>
            val filterContext = context + ("record" -> jsonToJava(record))
            TemplateEngine.evaluateCondition(filterExpr, filterContext)
      }

  /**
   * Interpret offset increment pagination.
   *
   * Continues fetching pages using offset/limit pattern.
   *
   * Example: /api/users?offset=0&limit=50, /api/users?offset=50&limit=50, ...
   */
  private def interpretOffsetIncrement(
    spec: StreamSpec,
    pagination: PaginationStrategy.OffsetIncrement,
    context: Map[String, Any]
  ): ZStream[
    HttpClient & TemplateEngine & ResponseParser & Authenticator,
    Throwable,
    Json
  ] =
    ZStream
      .unfoldZIO(0) { offset =>
        val offsetContext = context + ("offset" -> offset) + ("limit" -> pagination.pageSize)

        executeRequest(spec.requester, offsetContext).flatMap { response =>
          ResponseParser.extract(response, spec.recordSelector.extractor).flatMap { records =>
            if records.isEmpty then
              // No more records - stop pagination
              ZIO.succeed(None)
            else
              // Return records and next offset
              ZIO.succeed(Some((records, offset + pagination.pageSize)))
          }
        }
      }
      .flatMap(records => ZStream.fromIterable(records))
      .filterZIO { record =>
        spec.recordSelector.filter match
          case None             => ZIO.succeed(true)
          case Some(filterExpr) =>
            val filterContext = context + ("record" -> jsonToJava(record))
            TemplateEngine.evaluateCondition(filterExpr, filterContext)
      }

  /**
   * Interpret cursor-based pagination.
   *
   * Uses a cursor from the response to fetch next page. Stops when stop
   * condition evaluates to true.
   *
   * Example: {"data": [...], "next_cursor": "abc123"}
   */
  private def interpretCursorPagination(
    spec: StreamSpec,
    pagination: PaginationStrategy.CursorPagination,
    context: Map[String, Any]
  ): ZStream[
    HttpClient & TemplateEngine & ResponseParser & Authenticator,
    Throwable,
    Json
  ] =
    ZStream
      .unfoldZIO(Option.empty[String]) { maybeCursor =>
        val cursorContext = maybeCursor match
          case Some(cursor) => context + ("next_page_token" -> cursor)
          case None         => context

        executeRequest(spec.requester, cursorContext).flatMap { response =>
          for
            // Check stop condition
            shouldStop <- TemplateEngine.evaluateCondition(
              pagination.stopCondition,
              context + ("response" -> response)
            )

            result <-
              if shouldStop then ZIO.succeed(None)
              else
                for
                  // Extract records
                  records    <- ResponseParser.extract(
                    response,
                    spec.recordSelector.extractor
                  )

                  // Extract next cursor
                  nextCursor <- TemplateEngine.render(
                    pagination.cursorValue,
                    context + ("response" -> jsonToJava(response))
                  )
                yield Some((records, Some(nextCursor)))
          yield result
        }
      }
      .flatMap(records => ZStream.fromIterable(records))
      .filterZIO { record =>
        spec.recordSelector.filter match
          case None             => ZIO.succeed(true)
          case Some(filterExpr) =>
            val filterContext = context + ("record" -> jsonToJava(record))
            TemplateEngine.evaluateCondition(filterExpr, filterContext)
      }

  /**
   * Convert Circe Json to Java objects for Jinjava.
   *
   * Jinjava expects Java objects (Map, List, primitives), not Scala/Circe
   * types.
   */
  private def jsonToJava(json: Json): Any =
    json.fold(
      jsonNull = null,
      jsonBoolean = identity,
      jsonNumber = n => n.toInt.orElse(n.toLong).getOrElse(n.toDouble),
      jsonString = identity,
      jsonArray = arr => arr.map(jsonToJava).asJava,
      jsonObject = obj => obj.toMap.view.mapValues(jsonToJava).toMap.asJava
    )
