package exlo

import exlo.config.ExloConfigProvider
import exlo.domain.Connector
import exlo.domain.StreamElement
import zio.*
import zio.stream.ZStream

/**
 * Base trait for EXLO connectors.
 *
 * Implement `connector` method to return a Connector instance loaded inside
 * ZIO context. This solves the phase mismatch problem where metadata loading
 * requires effects (file I/O, HTTP, config parsing).
 *
 * Example:
 * {{{
 * object MyConnector extends ExloApp {
 *   override def connector: ZIO[Any, Throwable, Connector] =
 *     for
 *       spec <- loadYamlSpec("connector.yaml")
 *     yield new Connector:
 *       def id = "my-connector"
 *       def version = spec.version  // Loaded from spec!
 *       type Env = MyEnv
 *       def extract(state: String) = ZStream(...)
 *       def environment = ZLayer.make[Env](...)
 * }
 * }}}
 */
trait ExloApp extends ZIOAppDefault:

  /**
   * Load connector with metadata inside ZIO context.
   *
   * Override this method to load metadata effectfully (from files, HTTP, config, etc.)
   * without resorting to Unsafe runtime or lazy vals.
   */
  def connector: ZIO[Any, Throwable, Connector]

  /**
   * ConfigProvider setup.
   *
   * Default: ExloConfigProvider.default for environment variables with SCREAMING_SNAKE_CASE and
   * HOCON fallback.
   *
   * Override if you need custom config sources.
   */
  override val bootstrap: ZLayer[ZIOAppArgs, Any, Any] =
    ZLayer.succeed(ZIOAppArgs.empty) >>> Runtime.setConfigProvider(ExloConfigProvider.default)

  /**
   * Run the connector.
   *
   * This is the entry point - framework handles everything:
   * 1. Load connector (with metadata)
   * 2. Load EXLO config from environment (via bootstrap ConfigProvider)
   * 3. Build infrastructure layers (StorageConfig, IcebergCatalog, Table)
   * 4. Run PipelineOrchestrator with connector's extract function
   */
  override def run: ZIO[Any, Any, Unit] =
    connector.flatMap(c => ExloRunner.run(c).provide(c.environment))
