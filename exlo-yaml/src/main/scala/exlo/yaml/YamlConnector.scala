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

  // Load spec once at initialization and cache it
  // We need the spec for both version (at startup) and extract (at runtime)
  private lazy val cachedSpec: ConnectorSpec = {
    import zio.Unsafe.unsafe
    implicit val u: Unsafe = unsafe(identity)

    val specPath = sys.env.getOrElse("EXLO_CONNECTOR_SPEC_PATH", "connector.yaml")
    val runtime  = Runtime.default

    runtime.unsafe
      .run(
        YamlSpecLoader.loadSpecDirect(specPath)
      )
      .getOrThrowFiberFailure()
  }

  override def connectorId: String = "yaml-connector"

  override def connectorVersion: String = cachedSpec.version

  /**
   * User environment includes all YAML runtime services.
   *
   * Services required:
   *   - YamlSpecLoader: Load YAML spec files
   *   - HttpClient: Execute HTTP requests (infrastructure)
   *   - TemplateEngine: Render Jinja2 templates
   *   - ResponseParser: Extract records from JSON
   *   - Authenticator: Apply auth to requests
   *   - RuntimeContext: Execution-scoped state (headers, cursor, pagination vars)
   *   - ErrorHandlerService: YAML-configurable retry policies (reads from RuntimeContext)
   *   - RequestExecutor: Consolidated request execution with all cross-cutting concerns
   */
  type Env = YamlSpecLoader & RequestExecutor & TemplateEngine & ResponseParser & ErrorHandlerService & RuntimeContext

  override def environment: ZLayer[Any, Any, Env] =
    ZLayer.make[Env](
      // Infrastructure layers
      Client.default, // zio-http Client (scoped)

      // RuntimeContext layer - loads config from env, state initialized in extract()
      RuntimeContext.Live.layer,

      // Service layers
      YamlSpecLoader.Live.layer,
      HttpClient.Live.layerWithContext,    // depends on Client & RuntimeContext
      TemplateEngine.Live.layer,
      ResponseParser.Live.layer,
      Authenticator.Live.layer,
      ErrorHandlerService.liveWithContext, // YAML-configurable error handling with RuntimeContext
      RateLimiter.Live.layer,              // Rate limiting to prevent 429 errors
      RequestExecutor.live                 // Consolidated request execution
    )

  override def extract(state: String): ZStream[Env, Throwable, StreamElement] =
    ZStream
      .scoped {
        for
          // Load stream config to get which stream to execute
          streamConfig <- ZIO.config(StreamConfig.config)

          // Select the stream to execute based on config (streamName is now required)
          streamSpec <- ZIO
            .fromOption(cachedSpec.streams.find(_.name == streamConfig.streamName))
            .orElseFail(
              new RuntimeException(
                s"Stream '${streamConfig.streamName}' not found in YAML spec. Available streams: ${cachedSpec.streams.map(_.name).mkString(", ")}"
              )
            )
        yield streamSpec
      }
      .flatMap { streamSpec =>
        // Initialize RuntimeContext with cursor from state parameter (if present)
        val initState = ZStream.fromZIO {
          if state.isEmpty then ZIO.unit
          else
            ZIO
              .attempt {
                val mapper   = new com.fasterxml.jackson.databind.ObjectMapper()
                val jsonNode = mapper.readTree(state)
                Option(jsonNode.get("cursor")).map(_.asText())
              }
              .flatMap {
                case Some(cursor) => RuntimeContext.updateCursor(cursor)
                case None         => ZIO.unit
              }
        }

        initState *>
          // Interpret stream spec into record stream (context read from RuntimeContext)
          YamlInterpreter
            .interpretStream(streamSpec)
            .mapZIO { jsonNode =>
              // Update RuntimeContext cursor for each record (if cursor field configured)
              streamSpec.cursorField match
                case Some(fieldName) =>
                  Option(jsonNode.get(fieldName))
                    .map(_.asText())
                    .map(RuntimeContext.updateCursor)
                    .getOrElse(ZIO.unit)
                    .as(StreamElement.Data(jsonNode.toString))
                case None            =>
                  ZIO.succeed(StreamElement.Data(jsonNode.toString))
            }
            .grouped(1000) // Batch records for efficiency
            .flatMap { chunk =>
              // Emit data records + checkpoint after each batch
              val dataRecords = ZStream.fromChunk(chunk)

              // Get current state from RuntimeContext for checkpoint
              val checkpoint = ZStream.fromZIO(
                RuntimeContext.getStateJson.map(stateJson => StreamElement.Checkpoint(stateJson))
              )

              dataRecords ++ checkpoint
            }
      }
