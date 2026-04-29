package exlo.runtime

import io.opentelemetry.api.OpenTelemetry as JOpenTelemetry
import io.opentelemetry.instrumentation.runtimemetrics.java17.RuntimeMetrics
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdk
import zio.*
import zio.telemetry.opentelemetry.OpenTelemetry
import zio.telemetry.opentelemetry.core.context.ContextPropagator
import zio.telemetry.opentelemetry.core.trace.Tracer

/**
 * Telemetry wiring. Three layers:
 *
 *   - `live` builds the OTel SDK via `AutoConfiguredOpenTelemetrySdk` (honors `OTEL_*` env
 *     vars including `OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_SERVICE_NAME`, etc.), registers
 *     JVM `RuntimeMetrics`, and exposes a [[Tracer]]. `logAnnotated = true` makes ZIO log
 *     annotations carry `trace_id` / `span_id` for the active span.
 *   - `noop` is a zero-overhead Tracer for tests and local dev with no collector. Spans are
 *     dropped; `logAnnotated = true` is harmless because no span is ever active.
 *   - `auto` picks `live` vs `noop` at runtime based on env: if `OTEL_EXPORTER_OTLP_ENDPOINT`
 *     is set and `OTEL_SDK_DISABLED` is not `true`, it tries `live` and falls back to `noop`
 *     (with a WARN log) if SDK init fails. Otherwise it returns `noop` silently. Telemetry
 *     should never kill the app.
 *
 * Context storage is `FiberRef`-based by default (the 4.x `OpenTelemetry.custom` /
 * `noop` use `ContextStorage.zioFiberRefScoped`). That makes parent/child propagation
 * work across `.fork`, `.flatMap`, and `foreachPar`. `forkDaemon` severs inheritance —
 * acceptable for now; we'll fix if it produces broken traces.
 */
object Telemetry:

  /** SDK lifecycle: autoconfigure + JVM metrics, both released on shutdown. */
  private val sdk: ZIO[Scope, Throwable, JOpenTelemetry] =
    ZIO.acquireRelease(
      ZIO.attempt {
        val instance   = AutoConfiguredOpenTelemetrySdk.builder().build().getOpenTelemetrySdk
        val jvmMetrics = RuntimeMetrics.create(instance)
        (instance, jvmMetrics)
      }
    ) { case (instance, jvmMetrics) =>
      ZIO.attempt(jvmMetrics.close()).ignoreLogged *>
        ZIO.attempt(instance.close()).ignoreLogged
    }.map(_._1)

  val live: TaskLayer[Tracer] =
    ZLayer.make[Tracer](
      OpenTelemetry.custom(ContextPropagator.default, logAnnotated = true)(sdk),
      OpenTelemetry.tracer("exlo-core")
    )

  val noop: TaskLayer[Tracer] =
    ZLayer.make[Tracer](
      OpenTelemetry.noop(logAnnotated = true),
      OpenTelemetry.tracer("exlo-noop")
    )

  /**
   * Auto-select `live` vs `noop` based on env. If `OTEL_EXPORTER_OTLP_ENDPOINT` is set and
   * `OTEL_SDK_DISABLED` is not `"true"`, try `live` and fall back to `noop` with a single
   * WARN log on init failure. Otherwise return `noop` silently — no SDK is created, so the
   * exporter cannot spam SEVERE shutdown logs.
   *
   * Telemetry must never kill the app: every failure path returns a working Tracer.
   */
  val auto: TaskLayer[Tracer] = select(sys.env.get)

  /**
   * Selector exposed for tests. Lookup is a function so callers can substitute a fixed map
   * instead of relying on the ambient process environment.
   */
  private[runtime] def select(getEnv: String => Option[String]): TaskLayer[Tracer] =
    val endpoint = getEnv("OTEL_EXPORTER_OTLP_ENDPOINT").exists(_.trim.nonEmpty)
    val disabled = getEnv("OTEL_SDK_DISABLED").exists(_.equalsIgnoreCase("true"))
    if endpoint && !disabled then liveOrNoop else noop

  /** `live` with a WARN log on init failure, then `noop` so the run continues. */
  private val liveOrNoop: TaskLayer[Tracer] =
    live.tapErrorCause(c =>
      ZIO.logWarningCause(
        "OpenTelemetry SDK init failed; falling back to noop tracer. " +
          "Telemetry is disabled for this run.",
        c
      )
    ).orElse(noop)
