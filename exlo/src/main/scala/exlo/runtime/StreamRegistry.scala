package exlo.runtime

import zio.*

/**
 * A connector source's catalog of runnable streams.
 *
 * Real connectors (Zendesk, Brandwatch, YouTube, …) almost always expose multiple
 * endpoints that share auth / base URL / pagination but have different schemas. Each
 * stream wants to run as its own batch job (separate Argo Workflow Job, separate Iceberg
 * table, independent state) — but they should ship as ONE container image, with stream
 * selection deferred to runtime via env.
 *
 * Pattern:
 *   - The connector author provides a `StreamRegistry` listing every stream the source
 *     supports, keyed by a stable string name.
 *   - Each entry is a fully self-contained `ZIO[Any, Throwable, Unit]` — the connector +
 *     initial state + all `provide`d layers wrapped together.
 *   - The deploy app reads `EXLO_STREAM` from env, dispatches via [[runSelected]].
 *
 * Operationally: one Docker image per source, N Argo Workflow templates that differ only
 * in `EXLO_STREAM` and the destination's `EXLO_TABLE_NAME` env vars.
 */
trait StreamRegistry:
  def streams: Map[String, ZIO[Any, Throwable, Unit]]

object StreamRegistry:

  /**
   * Read `EXLO_STREAM` from env, dispatch to the matching entry, fail loudly on miss. The
   * selected name is also bound to `RunContext.streamName` for the duration of the run, so
   * destinations can stamp records with the stream they originated from.
   */
  def runSelected(reg: StreamRegistry): ZIO[Any, Throwable, Unit] =
    for
      name <- ZIO.config(Config.string("stream").nested("exlo"))
      runnable <- ZIO
                    .fromOption(reg.streams.get(name))
                    .orElseFail(
                      new RuntimeException(
                        s"Unknown stream '$name'. Known streams: " +
                          reg.streams.keys.toList.sorted.mkString(", ")
                      )
                    )
      _ <- RunContext.streamName.locally(name)(runnable)
    yield ()
