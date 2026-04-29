package exlo.runtime

import io.opentelemetry.api.OpenTelemetry as JOpenTelemetry
import io.opentelemetry.instrumentation.runtimemetrics.java17.RuntimeMetrics
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdk
import zio.*
import zio.telemetry.opentelemetry.OpenTelemetry
import zio.telemetry.opentelemetry.core.context.ContextPropagator
import zio.telemetry.opentelemetry.core.trace.Tracer

/**
 * Telemetry wiring. Two layers:
 *
 *   - `live` builds the OTel SDK via `AutoConfiguredOpenTelemetrySdk` (honors `OTEL_*` env
 *     vars including `OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_SERVICE_NAME`, etc.), registers
 *     JVM `RuntimeMetrics`, and exposes a [[Tracer]]. `logAnnotated = true` makes ZIO log
 *     annotations carry `trace_id` / `span_id` for the active span.
 *   - `noop` is a zero-overhead Tracer for tests and local dev with no collector. Spans are
 *     dropped; `logAnnotated = true` is harmless because no span is ever active.
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
