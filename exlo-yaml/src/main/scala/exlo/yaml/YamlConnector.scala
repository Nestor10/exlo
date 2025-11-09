package exlo.yaml

import exlo.ExloApp
import exlo.config.StreamConfig
import exlo.domain.StreamElement
import exlo.yaml.infra.HttpClient
import exlo.yaml.interpreter.YamlInterpreter
import exlo.yaml.service.*
import exlo.yaml.spec.ConnectorSpec
import exlo.yaml.template.TemplateValue
import zio.*
import zio.http.Client
import zio.stream.*

/**
 * YAML-based connector implementation.
 *
 * Loads a declarative YAML specification and interprets it into a functioning
 * ExloApp connector. This enables building connectors without writing code -
 * just configure a YAML file.
 *
 * Architecture:
 *   - Extends ExloApp (user-facing trait with Throwable error type)
 *   - Loads ConnectorSpec from YAML file
 *   - Uses YamlInterpreter to execute spec as ZStream
 *   - Wires all service dependencies via ZLayer
 *
 * Following Zionomicon patterns:
 *   - Ch17: Dependency injection via ZLayer
 *   - Ch3: Error model (Throwable for framework code)
 *   - Ch14: Resource management (scoped HTTP client)
 */
object YamlConnector extends ExloApp:

  override def connectorId: String = "yaml-connector"

  override def connectorVersion: String = "0.1.0"

  /**
   * User environment includes all YAML runtime services.
   *
   * Services required:
   *   - YamlSpecLoader: Load YAML spec files
   *   - HttpClient: Execute HTTP requests (infrastructure)
   *   - TemplateEngine: Render Jinja2 templates
   *   - ResponseParser: Extract records from JSON
   *   - Authenticator: Apply auth to requests
   *   - StateTracker: Track incremental sync state (cursor values)
   *   - ErrorHandlerService: YAML-configurable retry policies
   *   - RequestExecutor: Consolidated request execution with all cross-cutting concerns
   */
  override type Env = YamlSpecLoader & RequestExecutor & TemplateEngine & ResponseParser & StateTracker

  override def environment: ZLayer[Any, Any, Env] =
    ZLayer.make[Env](
      // Infrastructure layers
      Client.default, // zio-http Client (scoped)

      // Service layers
      YamlSpecLoader.Live.layer,
      HttpClient.Live.layer,    // depends on Client
      TemplateEngine.Live.layer,
      ResponseParser.Live.layer,
      Authenticator.Live.layer,
      StateTracker.Live.layer,  // Stateful cursor tracking
      ErrorHandlerService.live, // YAML-configurable error handling
      RequestExecutor.live      // Consolidated request execution
    )

  override def extract(state: String): ZStream[Env, Throwable, StreamElement] =
    ZStream
      .scoped {
        for
          // Load stream config to get which stream to execute
          streamConfig <- ZIO.config(StreamConfig.config)

          // Load YAML spec from config
          // TODO: Get path from config in Phase 2
          specPath <- ZIO.succeed("connector.yaml") // Hardcoded for MVP

          spec <- YamlSpecLoader.loadSpec(specPath)

          // Validate at least one stream exists
          _          <- ZIO.when(spec.streams.isEmpty)(
            ZIO.fail(
              new RuntimeException("No streams defined in YAML spec")
            )
          )

          // Select the stream to execute based on config (streamName is now required)
          streamSpec <- ZIO
            .fromOption(spec.streams.find(_.name == streamConfig.streamName))
            .orElseFail(
              new RuntimeException(
                s"Stream '${streamConfig.streamName}' not found in YAML spec. Available streams: ${spec.streams.map(_.name).mkString(", ")}"
              )
            )
        yield streamSpec
      }
      .flatMap { streamSpec =>
        // Initialize StateTracker with current state, then process stream
        ZStream.fromZIO(StateTracker.reset(state)) *>
          ZStream
            .fromZIO(buildContext(state))
            .flatMap { context =>
              // Interpret stream spec into record stream
              YamlInterpreter
                .interpretStream(streamSpec, context)
                .mapZIO { jsonNode =>
                  // Update state tracker for each record (if cursor field configured)
                  StateTracker
                    .updateFromRecord(jsonNode, streamSpec.cursorField)
                    .as(StreamElement.Data(jsonNode.toString))
                }
            }
            .grouped(100) // Batch records for efficiency
            .flatMap { chunk =>
              // Emit data records + checkpoint after each batch
              val dataRecords = ZStream.fromChunk(chunk)

              // Get current state from StateTracker for checkpoint
              val checkpoint = ZStream.fromZIO(
                StateTracker.getStateJson.map(stateJson => StreamElement.Checkpoint(stateJson))
              )

              dataRecords ++ checkpoint
            }
      }

  /**
   * Build template context from state and config.
   *
   * Context is available to Jinja2 templates in the YAML spec.
   *
   * Provides:
   * - `state`: Parsed JSON state (empty object if fresh start)
   * - `config`: Connector-specific config from EXLO_CONNECTOR_CONFIG env var
   *
   * Example templates:
   * - `{{ config.api_key }}` - Access API key from config
   * - `{{ config.shop }}.myshopify.com` - Build URL with config value
   * - `{{ state.cursor }}` - Access cursor from state (incremental sync)
   * - `{{ state.lastTimestamp }}` - Access timestamp from state
   * - `{{ config.organization_ids }}` - Access array values (Snapchat example)
   *
   * State Format:
   * State is JSON that tracks what's been extracted (for incremental sync):
   * ```json
   * {
   *   "cursor": "next_page_token_value",
   *   "lastTimestamp": "2024-01-15T10:30:00Z",
   *   "lastProcessedId": 12345
   * }
   * ```
   */
  private def buildContext(state: String): Task[Map[String, TemplateValue]] =
    import com.fasterxml.jackson.databind.ObjectMapper
    import exlo.yaml.infra.ConfigLoader
    import exlo.yaml.template.TemplateValue

    for
      connectorConfig <- ConfigLoader.loadUnvalidated

      // Parse state JSON (empty object if no state)
      stateValue  <- ZIO
        .attempt {
          if state.isEmpty then TemplateValue.Obj(Map.empty)
          else
            val mapper   = new ObjectMapper()
            val jsonNode = mapper.readTree(state)
            TemplateValue.fromJsonNode(jsonNode)
        }
        .mapError(e => new RuntimeException(s"Failed to parse state JSON: ${e.getMessage}", e))
    yield Map[String, TemplateValue](
      "state"  -> stateValue,
      "config" -> TemplateValue.fromJsonNode(connectorConfig.node)
    )
