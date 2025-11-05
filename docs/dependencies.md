# Adding Dependencies

How to use external services (HTTP clients, databases, etc.) in your connector.

## Pattern Overview

1. Declare requirement in extract function signature: `ZStream[Client, ...]`
2. Get service from environment: `ZStream.service[Client]`
3. Provide layer via `environment` override

## Complete Example

See: `/examples/src/main/scala/HttpClientConnector.scala`

Run: `sbt "examples/runMain examples.HttpClientConnector"`

## Common Dependencies

- `zio.http.Client` - HTTP client
- `zio.jdbc.ZConnectionPool` - Database connection pool
- Custom config types

## Multiple Dependencies

Use intersection types:

```scala
def extract(state: String): ZStream[Client & MyConfig, Throwable, StreamElement]
```

Provide with `ZLayer.make`:

```scala
override def environment = ZLayer.make[Client & MyConfig](
  Client.default,
  MyConfig.layer
)
```

## Related

- [Configuration](./configuration.md)
- [Testing](./testing.md)
