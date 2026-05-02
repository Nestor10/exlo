package exlo.http

import exlo.domain.{Emission, ExloError, Stage}
import exlo.runtime.{Codec, SyncMode}
import zio.*
import zio.http.Request
import zio.json.*
import zio.stream.ZStream

/**
 * One stream within an HTTP-based connector. A connector
 * ([[exlo.HttpExloApp]]) is a collection of streams that share auth, base
 * URL, etc.; each stream declares its own pagination, parsing, and state
 * shape.
 *
 * Owns the four user-defined primitives (`request`, `records`, `nextCtx`,
 * `nextState`), the persistence-vs-ephemeral split between `state` and
 * `ctx`, and the adapter from those into a generic [[Stage]] the runner can
 * drive.
 *
 * Type parameters:
 *
 *   - `Parent`: the upstream stage's record type. Use `Unit` for root
 *     streams (no parent).
 *   - `Out`: the record type this stream emits. Leaf streams that write to
 *     a `DataSink` must have `Out = String` (opaque text). Inner streams
 *     can emit any user type that the next stage consumes.
 *
 * For each parent record, the stream runs an inner walker over `(state,
 * ctx)`:
 *
 *   - `nextCtx` returns the bag the *next iteration in this run* should
 *     see; `None` ends the per-parent walk and the stream advances to the
 *     next parent record (if any).
 *   - `nextState` returns the bag *future runs* should see (when
 *     `syncMode = Incremental`). Default: `None` — never persists.
 *
 * State is shared across all parent records within a run: each Mark updates
 * an internal Ref that subsequent iterations (including those triggered by
 * later parent records) read on the next request.
 */
trait HttpStream[-Parent, +Out]:

  // ---------- identity ----------
  def name: String

  // ---------- behavior config ----------
  def syncMode: SyncMode = SyncMode.Incremental

  /**
   * Number of parent records processed concurrently. Default `1`
   * (sequential — safe for any HttpStream). Set higher to fan out
   * per-parent walks: e.g., brandwatch's mentions stream sets
   * `parallelism = 4` to fetch mentions for 4 queries at once.
   *
   * Concurrency invariants:
   *   - State (the shared `Ref[Map]`) is updated atomically; each Mark's
   *     `state ++ next` is a CAS, so concurrent updates can't lose data.
   *   - Marks emitted from different parents are serialized by the
   *     runner's `WatermarkTracker` based on emission seq#; they fold via
   *     `reduce` in seq order, so commit semantics are unchanged.
   *   - Parents whose state keys overlap WILL race on the value (last
   *     CAS wins). Use disjoint keys per parent (e.g., namespace by
   *     `<queryId>` like brandwatch's `cursor_<queryId>`) when running
   *     parallel.
   */
  def parallelism: Int = 1

  // ---------- data slots ----------
  def initialState: Map[String, String] = Map.empty
  def initialCtx:   Map[String, String] = Map.empty

  // ---------- primitives ----------
  def request(parent: Parent, state: Map[String, String], ctx: Map[String, String]): Request

  def records(parent: Parent, state: Map[String, String], ctx: Map[String, String], r: HttpResponse): Chunk[Out]

  /** Some(c) = next iteration uses ctx `c`; None = walk for this parent is done. */
  def nextCtx(parent: Parent, state: Map[String, String], ctx: Map[String, String], r: HttpResponse): Option[Map[String, String]]

  /** Some(s) = persist new state (Mark emitted); None = no state advance. */
  def nextState(parent: Parent, state: Map[String, String], ctx: Map[String, String], r: HttpResponse): Option[Map[String, String]] = None

  // ---------- adapter to Stage ----------

  /** Materialize this stream as a [[Stage]]. The Stage's `id` is just the
   *  stream's intrinsic name (e.g., `"mentions"`); the connector id is
   *  carried separately through `Exlo.run` / `RunContext.connectorId`. */
  final def asStage(connectorVersion: String)
      : Stage[Parent, Out, Map[String, String], HttpExec] =
    val streamRef = this
    new Stage[Parent, Out, Map[String, String], HttpExec]:
      val id           = streamRef.name
      val version      = connectorVersion
      val initialState = streamRef.initialState
      val codec        = HttpStream.mapCodec
      def reduce(a: Map[String, String], b: Map[String, String]): Map[String, String] = a ++ b

      def run[R0](
          input:  ZStream[R0, ExloError, Parent],
          resume: Map[String, String]
      ): ZStream[HttpExec & R0, ExloError, Emission[Out, Map[String, String]]] =
        // FullSync wipes the runner-loaded resume. Incremental honors it.
        val effectiveResume = streamRef.syncMode match
          case SyncMode.Incremental => resume
          case SyncMode.FullSync    => streamRef.initialState

        ZStream.unwrap {
          // State is shared across parents within a run: Marks update it,
          // subsequent iterations (and subsequent parent records) read the
          // updated value on their next `request`.
          Ref.make(effectiveResume).map { stateRef =>
            input.flatMapPar(streamRef.parallelism) { parent =>
              ZStream.unwrap {
                Ref.make(streamRef.initialCtx).map { ctxRef =>
                  ZStream.unfoldChunkZIO[
                    HttpExec, ExloError, Emission[Out, Map[String, String]], Boolean
                  ](true) {
                    case false => ZIO.none
                    case true  =>
                      for
                        state <- stateRef.get
                        ctx   <- ctxRef.get
                        req    = streamRef.request(parent, state, ctx)
                        resp  <- HttpExec.run(req)
                        recs    = streamRef.records(parent, state, ctx, resp)
                        nState  = streamRef.nextState(parent, state, ctx, resp)
                        nCtx    = streamRef.nextCtx(parent, state, ctx, resp)
                        emissions =
                          recs.map[Emission[Out, Map[String, String]]](Emission.Record(_)) ++
                          Chunk.fromIterable(
                            nState.map(s => Emission.Mark(s): Emission[Out, Map[String, String]])
                          )
                        _ <- nState.fold(ZIO.unit)(s => stateRef.set(s))
                        _ <- nCtx.fold(ZIO.unit)(c => ctxRef.set(c))
                        continue = nCtx.isDefined
                      yield Some((emissions, continue))
                  }
                }
              }
            }
          }
        }

object HttpStream:

  given JsonEncoder[Map[String, String]] = JsonEncoder.map[String, String]
  given JsonDecoder[Map[String, String]] = JsonDecoder.map[String, String]

  val mapCodec: Codec[Map[String, String]] = Codec.fromJson[Map[String, String]]
