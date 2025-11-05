package exlo

import exlo.config.{StreamConfig, SyncConfig}
import exlo.domain.{ExloError, StateConfig, StorageConfig, StreamElement, SyncMetadata}
import exlo.infra.{IcebergCatalog, IcebergCatalogSelector}
import exlo.pipeline.PipelineOrchestrator
import exlo.service.{Connector, Table}
import zio.*
import zio.stream.ZStream

import java.time.Instant
import java.util.UUID

/** Main entry point for running EXLO connectors.
  *
  * Provides user environment integration with the framework's orchestration.
  */
object ExloRunner:

  /** Run EXLO pipeline.
    *
    * This method:
    *   1. Loads EXLO config from ConfigProvider (set in bootstrap)
    *   1. Creates a Connector implementation that captures user's environment R
    *   1. Builds framework layers with config-specific parameters
    *   1. Runs PipelineOrchestrator with all layers
    *
    * The key insight: We create a Connector implementation that captures the
    * user's environment R using ZIO.environment, then provides it when extract
    * is called. This allows the framework to wire its own layers while the
    * user's R requirement propagates outward.
    *
    * @param connectorId
    *   Unique identifier for this connector
    * @param connectorVersion
    *   Semantic version of connector logic
    * @param extract
    *   User's extraction function
    * @tparam R
    *   User's environment requirements
    * @return
    *   Effect that runs the pipeline, requires user's R
    */
  def run[R](
      connectorId: String,
      connectorVersion: String,
      extract: String => ZStream[R, Throwable, StreamElement]
  ): ZIO[R, Throwable, Unit] =

    ZIO.scoped {
      for
        // Capture user's environment to make it available during extract
        userEnv <- ZIO.environment[R]

        // Create connector implementation
        // Use separate vars to avoid shadowing
        id = connectorId
        version = connectorVersion
        extractFn = extract
        connector = new Connector {
          override def connectorId: String = id
          override def extract(state: String) =
            extractFn(state)
              .provideEnvironment(userEnv)
              .mapError(e =>
                ExloError.ConnectorError(
                  s"Connector $id failed",
                  e
                )
              )
        }

        // Create sync metadata
        syncMetadata = SyncMetadata(
          syncId = UUID.randomUUID(),
          startedAt = Instant.now(),
          connectorVersion = connectorVersion
        )

        // Build framework layers with runtime config
        // Each layer pulls its own config via ZIO.config - fully decoupled
        storageLayer = ZLayer.fromZIO(StorageConfig.load)
        
        frameworkLayer = ZLayer.make[IcebergCatalog & Table & StateConfig](
          storageLayer,              // Provides StorageConfig for catalog layer
          IcebergCatalogSelector.layer,  // Pulls StreamConfig via ZIO.config
          Table.Live.layer,          // Pulls StreamConfig via ZIO.config
          StateConfig.layer
        )

        // Load sync config for orchestrator
        syncConfig <- ZIO.config(SyncConfig.config)

        // Run orchestrator with all layers
        _ <- PipelineOrchestrator
          .run(syncMetadata, syncConfig.stateVersion)
          .provide(
            ZLayer.succeed(connector), // Provide connector instance
            frameworkLayer // Provide all framework services
          )
          .mapError(e => new RuntimeException(s"Pipeline failed: $e"))
      yield ()
    }
