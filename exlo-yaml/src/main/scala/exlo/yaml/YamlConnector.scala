package exlo.yaml

import exlo.ExloApp
import exlo.config.StreamConfig
import exlo.domain.StreamElement
import exlo.yaml.interpreter.YamlInterpreter
import exlo.yaml.service.*
import exlo.yaml.spec.ConnectorSpec
import io.circe.syntax.*
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
   *   - HttpClient: Execute HTTP requests
   *   - TemplateEngine: Render Jinja2 templates
   *   - ResponseParser: Extract records from JSON
   *   - Authenticator: Apply auth to requests
   */
  override type Env = YamlSpecLoader & HttpClient & TemplateEngine & ResponseParser & Authenticator

  override def environment: ZLayer[Any, Any, Env] =
    ZLayer.make[Env](
      // Infrastructure layers
      Client.default, // zio-http Client (scoped)

      // Service layers
      YamlSpecLoader.Live.layer,
      HttpClient.Live.layer, // depends on Client
      TemplateEngine.Live.layer,
      ResponseParser.Live.layer,
      Authenticator.Live.layer
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
        // Build template context for this stream (loads EXLO_CONFIG)
        ZStream
          .fromZIO(buildContext(state))
          .flatMap { context =>
            // Interpret stream spec into record stream
            YamlInterpreter
              .interpretStream(streamSpec, context)
              .map { json =>
                // Convert JSON to StreamElement.Data
                StreamElement.Data(json.noSpaces)
              }
          }
          .grouped(100) // Batch records for efficiency
          .flatMap { chunk =>
            // Emit data records + checkpoint after each batch
            val dataRecords = ZStream.fromChunk(chunk)
            val checkpoint  = ZStream(
              StreamElement.Checkpoint(
                // TODO: Implement proper state tracking in Phase 2
                // For now, just emit empty state
                """{}"""
              )
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
   * - `state`: Parsed JSON state (empty if fresh start)
   * - `config`: Connector-specific config from EXLO_CONNECTOR_CONFIG env var
   *
   * Example templates:
   * - `{{ config.api_key }}` - Access API key from config
   * - `{{ config.shop }}.myshopify.com` - Build URL with config value
   * - `{{ state.cursor }}` - Access cursor from state
   * - `{{ config.organization_ids }}` - Access array values (Snapchat example)
   *
   * TODO Phase 2:
   *   - Validate config against connection_specification JSON Schema
   *   - Parse state JSON (currently just string)
   *   - Add pagination context (cursor, page, offset)
   */
  private def buildContext(state: String): Task[Map[String, Any]] =
    import exlo.yaml.infra.ConfigLoader
    import scala.jdk.CollectionConverters.*

    for
      connectorConfig <- ConfigLoader.loadUnvalidated
      // Convert Circe Json to Java Map for Jinja2
      configMap  = circeJsonToJavaMap(connectorConfig.json)
    yield Map(
      "state"  -> (if state.isEmpty then Map.empty else state),
      "config" -> configMap
    )

  /** Convert Circe Json to java.util.Map for Jinja2. */
  private def circeJsonToJavaMap(json: io.circe.Json): java.util.Map[String, Any] =
    import io.circe.*
    import scala.jdk.CollectionConverters.{MapHasAsJava, SeqHasAsJava}

    json.asObject match
      case Some(obj) =>
        obj.toMap.view
          .mapValues {
            case j if j.isNull    => null
            case j if j.isBoolean => j.asBoolean.get
            case j if j.isNumber  =>
              j.asNumber.flatMap(_.toLong.map(identity)).getOrElse(j.asNumber.get.toDouble)
            case j if j.isString  => j.asString.get
            case j if j.isArray   => j.asArray.get.toList.map(circeJsonToJavaValue).asJava
            case j if j.isObject  => circeJsonToJavaMap(j)
            case j                => j.noSpaces
          }
          .toMap
          .asJava
      case None      => Map.empty[String, Any].asJava

  private def circeJsonToJavaValue(json: io.circe.Json): Any =
    import scala.jdk.CollectionConverters.SeqHasAsJava
    json match
      case j if j.isNull    => null
      case j if j.isBoolean => j.asBoolean.get
      case j if j.isNumber  =>
        j.asNumber.flatMap(_.toLong.map(identity)).getOrElse(j.asNumber.get.toDouble)
      case j if j.isString  => j.asString.get
      case j if j.isArray   => j.asArray.get.toList.map(circeJsonToJavaValue).asJava
      case j if j.isObject  => circeJsonToJavaMap(j)
      case j                => j.noSpaces
