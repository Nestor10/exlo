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

## Configure via env

The framework uses OpenTelemetry's autoconfigure, so any standard `OTEL_*`
variable works:

```
OTEL_SERVICE_NAME=exlo-zendesk
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317     # OTLP gRPC
OTEL_EXPORTER_OTLP_PROTOCOL=grpc                       # or http/protobuf
OTEL_TRACES_SAMPLER=parentbased_traceidratio
OTEL_TRACES_SAMPLER_ARG=0.1
```

No collector running? Exports fail silently. The app keeps working.

## Logs

Apps that extend `ZIOAppDefault` get JSON logs by setting `bootstrap`:

```scala
override val bootstrap: ZLayer[Any, Nothing, Unit] =
  Runtime.removeDefaultLoggers >>> consoleJsonLogger(ConsoleLoggerConfig.default)
```

With `Telemetry.live` provided, every log line emitted inside an active span
carries `trace_id` and `span_id` annotations.

## Tests

Use `Telemetry.noop` instead of `Telemetry.live`. Spans are dropped; the env
shape stays identical.

```scala
.provideSome[Client](ZLayer.succeed[Destination[State]](dest) ++ Telemetry.noop)
```
