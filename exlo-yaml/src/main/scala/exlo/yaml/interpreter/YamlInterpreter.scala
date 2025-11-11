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
   * Interpret a stream specification into a record stream.
   *
   * Context (config, state, pagination vars) is read from RuntimeContext.
   *
   * @param spec
   *   Stream configuration from YAML
   * @return
   *   Stream of JsonNode records
   */
  def interpretStream(
    spec: StreamSpec
  ): ZStream[
    RequestExecutor & TemplateEngine & ResponseParser & RuntimeContext,
    Throwable,
    JsonNode
  ] =
    spec.paginator match
      case PaginationStrategy.NoPagination =>
        // Single page - execute once
        ZStream
          .fromZIO(ZIO.serviceWithZIO[RequestExecutor](_.execute(spec.requester)))
          .flatMap(response => extractRecords(response, spec.recordSelector))

      case pageIncrement: PaginationStrategy.PageIncrement =>
        interpretPageIncrement(spec, pageIncrement)

      case offsetIncrement: PaginationStrategy.OffsetIncrement =>
        interpretOffsetIncrement(spec, offsetIncrement)

      case cursorPagination: PaginationStrategy.CursorPagination =>
        interpretCursorPagination(spec, cursorPagination)

  /**
   * Extract records from response using RecordSelector.
   *
   * Context for filter evaluation is read from RuntimeContext, with record added.
   *
   * @param response
   *   JSON response from API
   * @param selector
   *   Record extraction configuration
   * @return
   *   Stream of extracted JSON records
   */
  private def extractRecords(
    response: JsonNode,
    selector: RecordSelector
  ): ZStream[
    TemplateEngine & ResponseParser & RuntimeContext,
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
            // Set record variable in RuntimeContext for filter evaluation
            RuntimeContext.setPaginationVar("record", TemplateValue.fromJsonNode(record)) *>
              TemplateEngine.evaluateCondition(filterExpr)
      }

  /**
   * Interpret page increment pagination.
   *
   * Continues fetching pages until no more records are returned.
   * Page number is set in RuntimeContext for template interpolation.
   *
   * Example: /api/users?page=1, /api/users?page=2, ...
   */
  private def interpretPageIncrement(
    spec: StreamSpec,
    pagination: PaginationStrategy.PageIncrement
  ): ZStream[
    RequestExecutor & TemplateEngine & ResponseParser & RuntimeContext,
    Throwable,
    JsonNode
  ] =
    ZStream
      .unfoldZIO(pagination.startFrom) { pageNum =>
        RuntimeContext.setPaginationVar("page", TemplateValue.Num(pageNum)) *>
          ZIO.serviceWithZIO[RequestExecutor](_.execute(spec.requester)).flatMap { response =>
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
            // Set record variable in RuntimeContext for filter evaluation
            RuntimeContext.setPaginationVar("record", TemplateValue.fromJsonNode(record)) *>
              TemplateEngine.evaluateCondition(filterExpr)
      }

  /**
   * Interpret offset increment pagination.
   *
   * Continues fetching pages using offset/limit pattern.
   * Offset and limit are set in RuntimeContext for template interpolation.
   *
   * Example: /api/users?offset=0&limit=50, /api/users?offset=50&limit=50, ...
   */
  private def interpretOffsetIncrement(
    spec: StreamSpec,
    pagination: PaginationStrategy.OffsetIncrement
  ): ZStream[
    RequestExecutor & TemplateEngine & ResponseParser & RuntimeContext,
    Throwable,
    JsonNode
  ] =
    ZStream
      .unfoldZIO(0) { offset =>
        RuntimeContext.setPaginationVar("offset", TemplateValue.Num(offset)) *>
          RuntimeContext.setPaginationVar("limit", TemplateValue.Num(pagination.pageSize)) *>
          ZIO.serviceWithZIO[RequestExecutor](_.execute(spec.requester)).flatMap { response =>
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
            // Set record variable in RuntimeContext for filter evaluation
            RuntimeContext.setPaginationVar("record", TemplateValue.fromJsonNode(record)) *>
              TemplateEngine.evaluateCondition(filterExpr)
      }

  /**
   * Interpret cursor-based pagination.
   *
   * Uses a cursor from the response to fetch next page. Stops when stop
   * condition evaluates to true.
   * Cursor token and response are set in RuntimeContext for template interpolation.
   *
   * Example: {"data": [...], "next_cursor": "abc123"}
   */
  private def interpretCursorPagination(
    spec: StreamSpec,
    pagination: PaginationStrategy.CursorPagination
  ): ZStream[
    RequestExecutor & TemplateEngine & ResponseParser & RuntimeContext,
    Throwable,
    JsonNode
  ] =
    ZStream
      .unfoldZIO(Option.empty[String]) { maybeCursor =>
        val setCursor = maybeCursor match
          case Some(cursor) => RuntimeContext.setPaginationVar("next_page_token", TemplateValue.Str(cursor))
          case None         => ZIO.unit

        setCursor *>
          ZIO.serviceWithZIO[RequestExecutor](_.execute(spec.requester)).flatMap { response =>
            // Set response in context for stop condition and cursor extraction
            RuntimeContext.setPaginationVar("response", TemplateValue.fromJsonNode(response)) *>
              TemplateEngine.evaluateCondition(pagination.stopCondition).flatMap { shouldStop =>
                if shouldStop then ZIO.succeed(None)
                else
                  for
                    // Extract records
                    records    <- ResponseParser.extract(
                      response,
                      spec.recordSelector.extractor
                    )

                    // Extract next cursor
                    nextCursor <- TemplateEngine.render(pagination.cursorValue)
                  yield Some((records, Some(nextCursor)))
              }
          }
      }
      .flatMap(records => ZStream.fromIterable(records))
      .filterZIO { record =>
        spec.recordSelector.filter match
          case None             => ZIO.succeed(true)
          case Some(filterExpr) =>
            // Set record variable in RuntimeContext for filter evaluation
            RuntimeContext.setPaginationVar("record", TemplateValue.fromJsonNode(record)) *>
              TemplateEngine.evaluateCondition(filterExpr)
      }
