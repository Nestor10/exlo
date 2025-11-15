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

  /**
   * Load connector with metadata from YAML spec (Phase 7/9 refactor).
   *
   * Supports three mutually exclusive spec sources:
   *   - EXLO_CONNECTOR_SPEC: Inline YAML string
   *   - EXLO_CONNECTOR_SPEC_HTTP_URL: HTTP/HTTPS URL to fetch YAML
   *   - EXLO_CONNECTOR_SPEC_FILE_PATH: File path (default: connector.yaml)
   */
  override def connector: ZIO[Any, Throwable, exlo.domain.Connector] =
    for
      // Load all three env vars
      inline   <- System.env("EXLO_CONNECTOR_SPEC")
      httpUrl  <- System.env("EXLO_CONNECTOR_SPEC_HTTP_URL")
      filePath <- System.env("EXLO_CONNECTOR_SPEC_FILE_PATH")

      // Validate exactly one source is set
      sources = List(
        inline.map(_ => "EXLO_CONNECTOR_SPEC"),
        httpUrl.map(_ => "EXLO_CONNECTOR_SPEC_HTTP_URL"),
        filePath.map(_ => "EXLO_CONNECTOR_SPEC_FILE_PATH")
      ).flatten

      _ <- ZIO.when(sources.isEmpty)(
        ZIO.fail(
          new IllegalArgumentException(
            "Must set exactly one of: EXLO_CONNECTOR_SPEC, EXLO_CONNECTOR_SPEC_HTTP_URL, EXLO_CONNECTOR_SPEC_FILE_PATH. " +
              "Defaulting to EXLO_CONNECTOR_SPEC_FILE_PATH=connector.yaml"
          )
        )
      )

      _    <- ZIO.when(sources.size > 1)(
        ZIO.fail(
          new IllegalArgumentException(
            s"Must set exactly one spec source, but got: ${sources.mkString(", ")}"
          )
        )
      )

      // Load spec based on which source is set
      spec <- (inline, httpUrl, filePath) match
        case (Some(yaml), None, None) =>
          // Inline YAML from env var
          YamlSpecLoader.parseYamlString(yaml)

        case (None, Some(url), None) =>
          // HTTP URL - fetch and parse
          loadSpecFromHttp(url)

        case (None, None, Some(path)) =>
          // File path
          YamlSpecLoader.loadSpecDirect(path)

        case (None, None, None) =>
          // Default fallback to file
          YamlSpecLoader.loadSpecDirect("connector.yaml")

        case _ =>
          ZIO.fail(
            new IllegalStateException("Unreachable: validation should have caught this")
          )
    yield buildConnector(spec)

  /**
   * Load YAML spec from HTTP URL.
   *
   * Supports:
   *   - HTTP and HTTPS
   *   - Retry with exponential backoff (3 attempts)
   *   - Connection timeout (10 seconds)
   *
   * Use cases:
   *   - GitOps: Point to GitHub/GitLab raw URL
   *   - Dynamic config: Fetch from central registry
   *   - Versioned specs: Pin to specific tag/commit
   */
  private def loadSpecFromHttp(url: String): ZIO[Any, Throwable, ConnectorSpec] =
    ZIO.scoped {
      for
        _      <- ZIO.logInfo(s"Fetching connector spec from HTTP: $url")
        client <- ZIO.service[Client].provide(Client.default)

        // Fetch with retry on transient failures
        yaml <- client
          .request(zio.http.Request.get(url))
          .flatMap { response =>
            if response.status.isSuccess then response.body.asString
            else
              ZIO.fail(
                new RuntimeException(
                  s"HTTP request failed with status ${response.status}: ${response.status.text}"
                )
              )
          }
          .retry(
            Schedule.exponential(1.second) && Schedule.recurs(2)
          ) // 3 attempts total
          .timeout(10.seconds)
          .someOrFail(new RuntimeException(s"HTTP request to $url timed out after 10 seconds"))
          .mapError(e =>
            new RuntimeException(
              s"Failed to fetch spec from $url after 3 attempts: ${e.getMessage}",
              e
            )
          )

        // Parse the fetched YAML
        spec <- YamlSpecLoader.parseYamlString(yaml)
        _    <- ZIO.logInfo(s"Successfully loaded spec from HTTP: version=${spec.version}")
      yield spec
    }

  /**
   * Build a Connector instance from loaded spec.
   */
  private def buildConnector(spec: ConnectorSpec): exlo.domain.Connector =
    new exlo.domain.Connector:
      override def id: String = "yaml-connector"

      override def version: String = spec.version

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
      override type Env = YamlSpecLoader & RequestExecutor & TemplateEngine & ResponseParser & ErrorHandlerService &
        RuntimeContext

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
                .fromOption(spec.streams.find(_.name == streamConfig.streamName))
                .orElseFail(
                  new RuntimeException(
                    s"Stream '${streamConfig.streamName}' not found in YAML spec. Available streams: ${spec.streams.map(_.name).mkString(", ")}"
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
