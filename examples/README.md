# EXLO Examples

Compilable example connectors demonstrating common patterns.

## Running Examples

```bash
# From project root
sbt "examples/runMain examples.SimpleConnector"
sbt "examples/runMain examples.PaginatedConnector"
sbt "examples/runMain examples.HttpClientConnector"
sbt "examples/runMain examples.IncrementalConnector"
```

## Examples

### SimpleConnector
The minimal connector - hardcoded data, no dependencies.
- See: `examples/src/main/scala/SimpleConnector.scala`
- Docs: `/docs/getting-started.md`

### PaginatedConnector
API pagination pattern using `ZStream.unfoldZIO`.
- See: `examples/src/main/scala/PaginatedConnector.scala`
- Docs: `/docs/pagination.md`

### HttpClientConnector
Using external dependencies (HTTP client).
- See: `examples/src/main/scala/HttpClientConnector.scala`
- Docs: `/docs/dependencies.md`

### IncrementalConnector
Timestamp-based incremental sync pattern.
- See: `examples/src/main/scala/IncrementalConnector.scala`
- Docs: `/docs/incremental-sync.md`
