package exlo.runtime

import zio.*
import zio.telemetry.opentelemetry.core.trace.Tracer
import zio.test.*

/**
 * Verifies the auto-selection logic of [[Telemetry.select]].
 *
 * We can't easily distinguish the live and noop tracers by reflection, so the contract
 * we test is: the layer always builds successfully (never fails the run) regardless of
 * env, and the live path's failure mode falls back to noop without surfacing the error.
 */
object TelemetrySpec extends ZIOSpecDefault:

  /** Build a layer to a Tracer and verify it succeeds. */
  private def buildSucceeds(layer: TaskLayer[Tracer]) =
    ZIO.scoped(layer.build.map(_ => assertCompletes))

  def spec = suite("Telemetry.auto / select")(
    test("env unset → noop, succeeds") {
      buildSucceeds(Telemetry.select(_ => None))
    },
    test("only OTEL_SDK_DISABLED set → noop, succeeds") {
      buildSucceeds(Telemetry.select {
        case "OTEL_SDK_DISABLED" => Some("true")
        case _                   => None
      })
    },
    test("endpoint set + OTEL_SDK_DISABLED=true → noop (disabled wins), succeeds") {
      buildSucceeds(Telemetry.select {
        case "OTEL_EXPORTER_OTLP_ENDPOINT" => Some("http://localhost:4317")
        case "OTEL_SDK_DISABLED"           => Some("true")
        case _                             => None
      })
    },
    test("endpoint blank → treated as unset, noop, succeeds") {
      buildSucceeds(Telemetry.select {
        case "OTEL_EXPORTER_OTLP_ENDPOINT" => Some("   ")
        case _                             => None
      })
    },
    test("endpoint set → live path; bad SDK config falls back to noop without failing") {
      // Picking a non-existent traces exporter forces AutoConfiguredOpenTelemetrySdk.build()
      // to throw at construction time. The auto layer must catch this and yield noop.
      val badEnv: String => Option[String] = {
        case "OTEL_EXPORTER_OTLP_ENDPOINT" => Some("http://localhost:4317")
        case "OTEL_TRACES_EXPORTER"        => Some("definitely_not_a_real_exporter")
        case _                             => None
      }
      // Mirror the env into JVM system properties so AutoConfiguredOpenTelemetrySdk picks
      // them up (it also reads system props, not just OS env, which we can't mutate).
      val withProps = ZIO.acquireRelease(
        ZIO.attempt {
          val prev = sys.props.get("otel.traces.exporter")
          sys.props.update("otel.traces.exporter", "definitely_not_a_real_exporter")
          prev
        }
      )(prev =>
        ZIO.succeed {
          prev match
            case Some(v) => sys.props.update("otel.traces.exporter", v)
            case None    => sys.props.remove("otel.traces.exporter"); ()
        }
      )
      ZIO.scoped(
        withProps *> Telemetry.select(badEnv).build.map(_ => assertCompletes)
      )
    }
  )
