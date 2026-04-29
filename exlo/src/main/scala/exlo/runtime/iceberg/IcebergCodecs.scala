package exlo.runtime.iceberg

import zio.json.{JsonCodec, JsonDecoder, JsonEncoder}

/**
 * Convenience JSON codecs for state types that show up in stateless / trivial connectors.
 * `import exlo.runtime.iceberg.IcebergCodecs.given` to bring them into scope.
 */
object IcebergCodecs:

  /**
   * Codec for `Unit` — for stateless connectors. The framework still serializes a state
   * value into the sidecar `StateStore` on every commit (atomicity is per-commit, not
   * conditional); for `Unit` we round-trip a fixed sentinel.
   */
  given unitCodec: JsonCodec[Unit] = JsonCodec(
    JsonEncoder.string.contramap[Unit](_ => "{}"),
    JsonDecoder.string.map[Unit](_ => ())
  )
