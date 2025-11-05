package exlo

import exlo.config.ExloConfigProvider
import exlo.domain.StreamElement
import zio.*
import zio.stream.ZStream

/**
 * Base trait for EXLO connectors.
 *
 * Users extend this trait and implement:
 * - connectorId: Unique identifier
 * - connectorVersion: Semantic version
 * - extract: State => Stream of data/checkpoints
 *
 * Optionally override:
 * - environment: ZLayer providing user dependencies (default: ZLayer.empty)
 * - bootstrap: ConfigProvider setup (default: ExloConfigProvider.default)
 *
 * Example:
 * {{{
 * object MyConnector extends ExloApp {
 *   override def connectorId = "my-connector"
 *   override def connectorVersion = "1.0.0"
 *   override def extract(state: String) =
 *     ZStream(
 *       StreamElement.Data("""{"id": 1}"""),
 *       StreamElement.Checkpoint("""{"cursor": "done"}""")
 *     )
 * }
 * }}}
 */
trait ExloApp extends ZIOAppDefault:

  /**
   * Unique identifier for this connector.
   *
   * Example: "shopify-orders", "stripe-charges"
   */
  def connectorId: String

  /**
   * Semantic version of connector logic.
   *
   * Used for reproducibility and debugging.
   * Example: "1.0.0", "2.1.0"
   */
  def connectorVersion: String

  /**
   * User's extraction logic.
   *
   * @param state
   *   JSON string with extraction state (cursor, page, timestamp)
   * @return
   *   Stream of data records and checkpoints
   */
  type Env
  def extract(state: String): ZStream[Env, Throwable, StreamElement]

  /**
   * User's environment layer.
   *
   * Provide your dependencies here (HTTP clients, databases, etc.). Framework layers
   * (StorageConfig, IcebergCatalog, Table) are added automatically.
   *
   * Example:
   * {{{
   * type Env = Client & MyConfig
   * override def environment = ZLayer.make[Env](
   *   Client.default,
   *   MyConfig.layer
   * )
   * }}}
   */
  def environment: ZLayer[Any, Any, Env]

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
   * 1. Load EXLO config from environment (via bootstrap ConfigProvider)
   * 2. Build infrastructure layers (StorageConfig, IcebergCatalog, Table)
   * 3. Run PipelineOrchestrator with user's extract function
   *
   * User only needs to provide their environment layer.
   */
  override def run: ZIO[Any, Any, Unit] =
    ExloRunner
      .run(connectorId, connectorVersion, extract)
      .provide(environment)
      .catchAll(e =>
        ZIO.logError(s"Connector failed: $e") *>
          ZIO.fail(e)
      )
