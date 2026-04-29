# exlo Documentation

Short, focused guides for building exlo connectors.

## Start here

- [Getting Started](./getting-started.md) — the three connector shapes (`fullPull`,
  `HttpExtract`, `HttpSlice`) walked through as small sets of questions
- [Configuration](./configuration.md) — `EXLO_*` environment variables
- [Local Development](./local-development.md) — run a connector locally with logging
  or a local Iceberg warehouse
- [Telemetry](./telemetry.md) — OTel spans, JVM metrics, JSON logs with `trace_id`

## Patterns

- [Adaptive Slicing](./adaptive-slicing.md) — slice factories that react to mid-run
  state mutations (bisection, poison-pill skip, dynamic window width)

## Testing

- [Integration Testing](./integration-testing.md) — tests that hit real APIs

## Examples

Compilable, runnable examples live in `examples/src/main/scala/`. Start with
`examples/pokeapi/PokeApiApp.scala` (full-pull) and work up from there.

## Architecture & internals

For framework internals beyond what a connector author needs:
- `/context/DEVELOPER_GUIDE.md` — architecture and design
- `/context/CONFIG.md` — full configuration reference with deploy recipes
- `/context/PHASE_9_EKS_DEPLOY.md` — production EKS deploy walkthrough
