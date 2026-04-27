package exlo.runtime

import zio.*

/**
 * Per-run identity, ambient via `FiberRef`s. Set by `Exlo.run` for the duration of a
 * connector run (forked sink fibers inherit). Read by destinations when they need to
 * stamp records with operational metadata (sync_id, connector, version).
 *
 * `FiberRef` matches the contract perfectly: scoped via `.locally`, inherited on fork,
 * cleaned up automatically when the scope exits. No global mutable state, no layer
 * signature surgery, no manual threading through `Destination` impls.
 */
object RunContext:

  val syncId: FiberRef[String] =
    Unsafe.unsafe(implicit u => FiberRef.unsafe.make("unknown"))

  val connectorId: FiberRef[String] =
    Unsafe.unsafe(implicit u => FiberRef.unsafe.make("unknown"))

  val connectorVersion: FiberRef[String] =
    Unsafe.unsafe(implicit u => FiberRef.unsafe.make("0.0.0"))

  /**
   * Stream name within the connector source (e.g. `tickets`, `ticket_metrics`, `kalos`).
   * Every connector ships through a `StreamRegistry` — even single-stream sources
   * register exactly one entry — so this is always populated by
   * `StreamRegistry.runSelected` from `EXLO_STREAM`. The default `unset` is a loud
   * sentinel: if it shows up in destination records, something bypassed the registry.
   */
  val streamName: FiberRef[String] =
    Unsafe.unsafe(implicit u => FiberRef.unsafe.make("unset"))

  /**
   * Scope a connector run's context. Inside `zio`, the connector-identity FiberRefs hold
   * the given values; on exit they revert. Forked fibers within `zio` inherit the values
   * at fork time. Stream name is set separately by `StreamRegistry.runSelected`.
   */
  def withRun[R, E, A](
      syncIdValue: String,
      connectorIdValue: String,
      connectorVersionValue: String
  )(zio: ZIO[R, E, A]): ZIO[R, E, A] =
    syncId.locally(syncIdValue) {
      connectorId.locally(connectorIdValue) {
        connectorVersion.locally(connectorVersionValue)(zio)
      }
    }

  /** Snapshot of (syncId, connectorId, connectorVersion, streamName). */
  val snapshot: UIO[(String, String, String, String)] =
    for
      s  <- syncId.get
      c  <- connectorId.get
      v  <- connectorVersion.get
      sn <- streamName.get
    yield (s, c, v, sn)
