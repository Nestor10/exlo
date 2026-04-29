# Telemetry

exlo emits OpenTelemetry traces, JVM metrics, and structured JSON logs with
`trace_id` / `span_id` baked in. Pure-ZIO, no Java agent.

## Spans

- `connector.run <id>` — root span per run. Attributes: `exlo.sync_id`,
  `exlo.connector_id`, `exlo.connector_version`.
- `<connector_id>.slice` — one per `HttpSlice` slice. Attributes:
  `exlo.connector_id`, `exlo.slice`.
- `HTTP <method>` — one per HTTP attempt cycle (retries fold into the span
  duration). Attributes: `http.method`, `http.url`, `http.status_code`.
- `sink.commit` — one per drain-and-commit tick.

## Three layers: `auto`, `live`, `noop`

- `Telemetry.auto` — **the default for application code**. At startup it inspects the
  process env:
  - If `OTEL_EXPORTER_OTLP_ENDPOINT` is set (and `OTEL_SDK_DISABLED` is not `"true"`),
    it builds the `live` SDK; if SDK init throws, it logs a single WARN and falls back
    to `noop`. The run never fails because of telemetry.
  - Otherwise it returns `noop` silently. No SDK is constructed, so the OTLP exporter
    can't spam SEVERE shutdown logs in local dev / CI runs without a collector.
- `Telemetry.live` — explicit live SDK. Use only if you want to fail loudly when
  telemetry can't initialize.
- `Telemetry.noop` — explicit noop. Use in tests where deterministic behavior matters.

```scala
.provide(
  Client.default,
  DestinationFactory.layer[State](id),
  Telemetry.auto      // picks live or noop from env, never crashes the app
)
```

## Configure via env

The framework uses OpenTelemetry's autoconfigure, so any standard `OTEL_*`
variable works:

```
OTEL_SERVICE_NAME=exlo-zendesk
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317     # OTLP gRPC — also flips Telemetry.auto to live
OTEL_EXPORTER_OTLP_PROTOCOL=grpc                       # or http/protobuf
OTEL_TRACES_SAMPLER=parentbased_traceidratio
OTEL_TRACES_SAMPLER_ARG=0.1
OTEL_SDK_DISABLED=true                                 # force noop even if endpoint is set
```

No collector running? With `Telemetry.auto`, you simply don't set
`OTEL_EXPORTER_OTLP_ENDPOINT` and the app uses `noop` — no SDK, no exporter, no
shutdown noise. With `Telemetry.live`, exports fail silently and the app keeps
working, but the OTLP exporter will log SEVERE on shutdown.

## Logs

Apps that extend `ZIOAppDefault` get JSON logs by setting `bootstrap`:

```scala
override val bootstrap: ZLayer[Any, Nothing, Unit] =
  Runtime.removeDefaultLoggers >>> consoleJsonLogger(ConsoleLoggerConfig.default)
```

With `Telemetry.live` provided (or `Telemetry.auto` when the OTLP endpoint env
var is set), every log line emitted inside an active span carries `trace_id`
and `span_id` annotations.

## Tests

Use `Telemetry.noop` instead of `Telemetry.live`. Spans are dropped; the env
shape stays identical.

```scala
.provideSome[Client](ZLayer.succeed[Destination[State]](dest) ++ Telemetry.noop)
```
