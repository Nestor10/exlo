package exlo.yaml

import exlo.yaml.service.YamlSpecLoader
import exlo.yaml.spec.ConnectorSpec
import zio.*

/**
 * Loads YAML connector spec and provides version metadata.
 *
 * This wrapper exists to pull the version from connector.yaml and make it
 * available to YamlConnector.connectorVersion synchronously.
 *
 * Usage in main:
 * {{{
 * object Main extends ZIOAppDefault:
 *   def run = YamlConnectorLoader.loadAndRun
 * }}}
 */
object YamlConnectorLoader:

  /**
   * Load connector spec, extract version, and run YamlConnector with it.
   */
  def loadAndRun: ZIO[Any, Any, Unit] =
    ZIO.scoped {
      for
        // Build minimal environment just for loading spec
        specLoader <- ZIO.service[YamlSpecLoader].provide(YamlSpecLoader.Live.layer)

        // Load spec to get version
        specPath <- ZIO.succeed(sys.env.getOrElse("EXLO_CONNECTOR_SPEC_PATH", "connector.yaml"))
        spec     <- specLoader.loadSpec(specPath)

        // Set version in environment for YamlConnector to read
        _ <- ZIO.succeed(java.lang.System.setProperty("exlo.connector.version", spec.version))

        // Now run the actual connector
        _ <- YamlConnector.run
      yield ()
    }
