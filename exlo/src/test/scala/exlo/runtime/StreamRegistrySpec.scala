package exlo.runtime

import zio.*
import zio.test.*

object StreamRegistrySpec extends ZIOSpecDefault:

  /** Build a registry whose entries log to a Ref so tests can verify which one ran. */
  private def makeRegistry(log: Ref[List[String]]): StreamRegistry = new StreamRegistry:
    val streams = Map[String, ZIO[Any, Throwable, Unit]](
      "alpha" -> log.update("alpha" :: _),
      "beta"  -> log.update("beta" :: _),
      "gamma" -> log.update("gamma" :: _)
    )

  def spec = suite("StreamRegistry")(
    test("runSelected dispatches to the entry whose name matches EXLO_STREAM") {
      for
        log <- Ref.make(List.empty[String])
        reg = makeRegistry(log)
        _ <- StreamRegistry.runSelected(reg)
               .withConfigProvider(ConfigProvider.fromMap(Map("exlo.stream" -> "beta")))
        ran <- log.get
      yield assertTrue(ran == List("beta"))
    },
    test("runSelected fails with a helpful message when EXLO_STREAM is unknown") {
      for
        log <- Ref.make(List.empty[String])
        reg = makeRegistry(log)
        result <- StreamRegistry.runSelected(reg)
                    .withConfigProvider(ConfigProvider.fromMap(Map("exlo.stream" -> "zeta")))
                    .either
        ran <- log.get
      yield assertTrue(
        ran.isEmpty,
        result.left.exists { e =>
          val msg = e.getMessage
          msg.contains("Unknown stream 'zeta'") && msg.contains("alpha") && msg.contains("beta") && msg.contains("gamma")
        }
      )
    },
    test("runSelected fails with a config error when EXLO_STREAM is missing") {
      for
        log <- Ref.make(List.empty[String])
        reg = makeRegistry(log)
        result <- StreamRegistry.runSelected(reg)
                    .withConfigProvider(ConfigProvider.fromMap(Map.empty))
                    .either
      yield assertTrue(result.left.exists(_.isInstanceOf[Config.Error]))
    }
  )
