package exlo.yaml.interpreter

import com.fasterxml.jackson.databind.JsonNode
import exlo.domain.StreamElement
import exlo.yaml.domain.YamlRuntimeError
import exlo.yaml.service.*
import exlo.yaml.spec.*
import exlo.yaml.template.TemplateValue
import exlo.yaml.util.JsonUtils
import zio.*
import zio.stream.*

/**
 * Interprets YAML spec ADTs into executable ZIO effects.
 *
 * Following functional programming principles: pure interpreter functions that
 * convert data (ADTs) into behavior (ZIO effects).
 *
 * The interpreter depends on services via ZIO environment, following
 * Zionomicon Chapter 17 dependency injection patterns.
 *
 * Request execution is delegated to RequestExecutor service, which handles
 * all cross-cutting concerns (template rendering, auth, retry, rate limiting).
 * This interpreter focuses purely on pagination logic.
 */
object YamlInterpreter:

  /**
   * Interpret a StreamSpec into a stream of JSON records.
   *
   * This is the main entry point that orchestrates:
   *   1. Execute request with pagination (via RequestExecutor)
   *   1. Extract records from each response
   *   1. Apply optional filters
   *
   * @param spec
   *   Stream specification from YAML
   * @param context
   *   Template context (config values, state)
   * @return
   *   Stream of JsonNode records
   */
  def interpretStream(
    spec: StreamSpec,
    context: Map[String, TemplateValue]
  ): ZStream[
    RequestExecutor & TemplateEngine & ResponseParser,
    Throwable,
    JsonNode
  ] =
    spec.paginator match
      case PaginationStrategy.NoPagination =>
        // Single page - execute once
        ZStream
          .fromZIO(ZIO.serviceWithZIO[RequestExecutor](_.execute(spec.requester, context)))
          .flatMap(response => extractRecords(response, spec.recordSelector, context))

      case pageIncrement: PaginationStrategy.PageIncrement =>
        interpretPageIncrement(spec, pageIncrement, context)

      case offsetIncrement: PaginationStrategy.OffsetIncrement =>
        interpretOffsetIncrement(spec, offsetIncrement, context)

      case cursorPagination: PaginationStrategy.CursorPagination =>
        interpretCursorPagination(spec, cursorPagination, context)

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
    response: JsonNode,
    selector: RecordSelector,
    context: Map[String, TemplateValue]
  ): ZStream[
    TemplateEngine & ResponseParser,
    Throwable,
    JsonNode
  ] =
    ZStream
      .fromIterableZIO(
        ResponseParser.extract(response, selector.extractor)
      )
      .filterZIO { record =>
        selector.filter match
          case None             => ZIO.succeed(true)
          case Some(filterExpr) =>
            // Add record to context for filter evaluation
            val filterContext = context + ("record" -> TemplateValue.fromJsonNode(record))
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
    context: Map[String, TemplateValue]
  ): ZStream[
    RequestExecutor & TemplateEngine & ResponseParser,
    Throwable,
    JsonNode
  ] =
    ZStream
      .unfoldZIO(pagination.startFrom) { pageNum =>
        val pageContext = context + ("page" -> TemplateValue.Num(pageNum))

        ZIO.serviceWithZIO[RequestExecutor](_.execute(spec.requester, pageContext)).flatMap { response =>
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
            import exlo.yaml.util.JsonUtils
            val filterContext = context + ("record" -> TemplateValue.fromJsonNode(record))
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
    context: Map[String, TemplateValue]
  ): ZStream[
    RequestExecutor & TemplateEngine & ResponseParser,
    Throwable,
    JsonNode
  ] =
    ZStream
      .unfoldZIO(0) { offset =>
        val offsetContext =
          context + ("offset" -> TemplateValue.Num(offset)) + ("limit" -> TemplateValue.Num(pagination.pageSize))

        ZIO.serviceWithZIO[RequestExecutor](_.execute(spec.requester, offsetContext)).flatMap { response =>
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
            import exlo.yaml.util.JsonUtils
            val filterContext = context + ("record" -> TemplateValue.fromJsonNode(record))
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
    context: Map[String, TemplateValue]
  ): ZStream[
    RequestExecutor & TemplateEngine & ResponseParser,
    Throwable,
    JsonNode
  ] =
    ZStream
      .unfoldZIO(Option.empty[String]) { maybeCursor =>
        val cursorContext = maybeCursor match
          case Some(cursor) => context + ("next_page_token" -> TemplateValue.Str(cursor))
          case None         => context

        ZIO.serviceWithZIO[RequestExecutor](_.execute(spec.requester, cursorContext)).flatMap { response =>
          for
            // Check stop condition
            shouldStop <- TemplateEngine.evaluateCondition(
              pagination.stopCondition,
              context + ("response" -> TemplateValue.fromJsonNode(response))
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
                    context + ("response" -> TemplateValue.fromJsonNode(response))
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
            import exlo.yaml.util.JsonUtils
            val filterContext = context + ("record" -> TemplateValue.fromJsonNode(record))
            TemplateEngine.evaluateCondition(filterExpr, filterContext)
      }
