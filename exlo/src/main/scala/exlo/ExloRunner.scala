package exlo

import exlo.config.StreamConfig
import exlo.config.SyncConfig
import exlo.domain.ExloError
import exlo.domain.StateConfig
import exlo.domain.StorageConfig
import exlo.domain.StreamElement
import exlo.domain.SyncMetadata
import exlo.infra.IcebergCatalog
import exlo.infra.IcebergCatalogSelector
import exlo.pipeline.PipelineOrchestrator
import exlo.service.Table
import zio.*
import zio.stream.ZStream

import java.time.Instant
import java.util.UUID

/**
 * Main entry point for running EXLO connectors.
 *
 * Provides user environment integration with the framework's orchestration.
 */
object ExloRunner:

  /**
   * Run EXLO pipeline with a domain.Connector.
   *
   * This method:
   *   1. Loads EXLO config from ConfigProvider (set in bootstrap)
   *   1. Adapts domain.Connector to service.Connector for framework
   *   1. Builds framework layers with config-specific parameters
   *   1. Runs PipelineOrchestrator with connector and framework layers
   *
   * The connector's environment must be provided by the caller (e.g., ExloApp.run).
   *
   * @param connector
   *   The domain connector to run (contains id, version, extract logic, environment)
   * @return
   *   Effect that runs the pipeline, requires connector.Env
   */
  def run(connector: exlo.domain.Connector): ZIO[connector.Env, Throwable, Unit] =
    ZIO.scoped {
      // Create sync metadata
      val syncMetadata = SyncMetadata(
        syncId = UUID.randomUUID(),
        startedAt = Instant.now(),
        connectorVersion = connector.version
      )

      // Build framework layers with runtime config
      // Each layer pulls its own config via ZIO.config - fully decoupled
      val storageLayer = ZLayer.fromZIO(StorageConfig.load)

      val frameworkLayer = ZLayer.make[IcebergCatalog & Table & StateConfig & StreamConfig](
        storageLayer,                                   // Provides StorageConfig for catalog layer
        IcebergCatalogSelector.layer,                   // Pulls StreamConfig via ZIO.config
        Table.Live.layer,                               // Pulls StreamConfig via ZIO.config
        StateConfig.layer,
        ZLayer.fromZIO(ZIO.config(StreamConfig.config)) // Load StreamConfig from env
      )

      for
        // Capture user environment
        userEnv <- ZIO.environment[connector.Env]

        // Adapt domain.Connector to service.Connector for framework
        serviceConnector = new exlo.service.Connector:
          override def connectorId:            String                                 = connector.id
          override def extract(state: String): ZStream[Any, ExloError, StreamElement] =
            connector
              .extract(state)
              .provideEnvironment(userEnv)
              .mapError(e => ExloError.ConnectorError(s"Connector ${connector.id} failed", e))

        // Load sync config for orchestrator
        syncConfig <- ZIO.config(SyncConfig.config)

        // Run orchestrator with all layers
        _ <- PipelineOrchestrator
          .run(syncMetadata, syncConfig.stateVersion)
          .provide(
            ZLayer.succeed(serviceConnector), // Provide service.Connector adapter
            frameworkLayer                    // Provide all framework services
          )
          .mapError(e => new RuntimeException(s"Pipeline failed: $e"))
      yield ()
    }
