package exlo.runtime

/**
 * Per-stream startup behavior. Set as a default on the connector
 * (`HttpExloApp.syncMode`) and overridable per-run via the `EXLO_SYNC_MODE`
 * env var.
 *
 * Both modes commit state forward as the run progresses (Marks emitted,
 * StateStore merged). The only difference is whether existing state is
 * loaded on cold start.
 */
enum SyncMode:
  /** Load existing state from StateStore on cold start; fall back to
   *  `initialState` if empty. Normal operating mode. */
  case Incremental

  /** Ignore existing state on cold start; always begin from `initialState`.
   *  Useful for backfills, repairs, or schema migrations. The run still
   *  writes state forward, so a subsequent Incremental run resumes from
   *  where the FullSync left off. */
  case FullSync
