package exlo.http

import exlo.domain.{RecoveryAction, SlicedConnector}
import exlo.runtime.ExloState
import zio.*
import zio.http.*
import zio.stream.ZStream
import zio.telemetry.opentelemetry.core.trace.Tracer

/**
 * Slice-paginated HTTP extraction. Connector author enumerates independent slices and
 * describes how to fetch each, with optional intra-slice cursor pagination for slices that
 * span multiple pages.
 *
 * The framework runs slices in parallel up to the configured parallelism. All slices emit
 * records via the shared `ExloState`; STM-assigned IDs maintain a global record ordering,
 * and concurrent `ExloState.update` calls are serialized.
 *
 * Two-stage builder:
 *   Stage 1 (`.slices` + `.request`) → `.parse` → Stage 2 (`.records` / `.nextRequest`
 *   intra-slice / `.advance` / `.parallelism`).
 */
object HttpSlice:

  sealed trait BuilderState
  sealed trait Missing  extends BuilderState
  sealed trait Provided extends BuilderState

  def apply[Slice, S]: Pre[Slice, S, Missing, Missing] = new Pre(None, None)

  // ---- Stage 1: slices + request ---------------------------------------------------------

  final class Pre[Slice, S, SlicesSet <: BuilderState, ReqSet <: BuilderState] private[http] (
      private[http] val slicesFn: Option[S => ZStream[Any, Throwable, Slice]],
      private[http] val requestFn: Option[(Slice, S) => Request]
  ):
    def slices(f: S => ZStream[Any, Throwable, Slice]): Pre[Slice, S, Provided, ReqSet] =
      new Pre(Some(f), requestFn)

    def request(f: (Slice, S) => Request): Pre[Slice, S, SlicesSet, Provided] =
      new Pre(slicesFn, Some(f))

  extension [Slice, S](p: Pre[Slice, S, Provided, Provided])
    def parse[P](
        f: Response => ZIO[Any, Throwable, P]
    ): Builder[Slice, S, P, Missing, Missing] =
      new Builder(p.slicesFn.get, p.requestFn.get, f, None, None, None, defaultParallelism)

  // ---- Stage 2 ---------------------------------------------------------------------------

  private val defaultParallelism = 4

  final class Builder[Slice, S, P, RecSet <: BuilderState, AdvSet <: BuilderState] private[http] (
      private[http] val slicesFn: S => ZStream[Any, Throwable, Slice],
      private[http] val requestFn: (Slice, S) => Request,
      private[http] val parseFn: Response => ZIO[Any, Throwable, P],
      private[http] val recordsFn: Option[P => Chunk[String]],
      private[http] val nextRequestFn: Option[(Slice, S, P) => Option[Request]],
      private[http] val advanceFn: Option[(S, Slice, P) => S],
      private[http] val parallelism_ : Int,
      private[http] val recoverFn: Option[PartialFunction[(Slice, S, Throwable), RecoveryAction[S]]] = None,
      private[http] val cfg: HttpExecConfig = HttpExecConfig()
  ):

    private def withCfg(c: HttpExecConfig): Builder[Slice, S, P, RecSet, AdvSet] =
      new Builder(slicesFn, requestFn, parseFn, recordsFn, nextRequestFn, advanceFn, parallelism_, recoverFn, c)

    def records(f: P => Chunk[String]): Builder[Slice, S, P, Provided, AdvSet] =
      new Builder(slicesFn, requestFn, parseFn, Some(f), nextRequestFn, advanceFn, parallelism_, recoverFn, cfg)

    def nextRequest(f: (Slice, S, P) => Option[Request]): Builder[Slice, S, P, RecSet, AdvSet] =
      new Builder(slicesFn, requestFn, parseFn, recordsFn, Some(f), advanceFn, parallelism_, recoverFn, cfg)

    def advance(f: (S, Slice, P) => S): Builder[Slice, S, P, RecSet, Provided] =
      new Builder(slicesFn, requestFn, parseFn, recordsFn, nextRequestFn, Some(f), parallelism_, recoverFn, cfg)

    def parallelism(n: Int): Builder[Slice, S, P, RecSet, AdvSet] =
      new Builder(slicesFn, requestFn, parseFn, recordsFn, nextRequestFn, advanceFn, n, recoverFn, cfg)

    /**
     * Convert a specific class of error inside one slice's walk into a state mutation
     * that re-enters the slice at the request stage. The handler receives the current
     * slice as well so it can encode poison-pill skipping per-slice. See
     * [[RecoveryAction]] for semantics. Errors not matched by the partial fail the
     * slice (and therefore the run).
     */
    def recover(
        pf: PartialFunction[(Slice, S, Throwable), RecoveryAction[S]]
    ): Builder[Slice, S, P, RecSet, AdvSet] =
      new Builder(slicesFn, requestFn, parseFn, recordsFn, nextRequestFn, advanceFn, parallelism_, Some(pf), cfg)

    def header(h: Header): Builder[Slice, S, P, RecSet, AdvSet]                = withCfg(cfg.addHeader(h))
    def bearer(token: String): Builder[Slice, S, P, RecSet, AdvSet]            = withCfg(cfg.addHeader(Header.Authorization.Bearer(token)))
    def basicAuth(u: String, p: String): Builder[Slice, S, P, RecSet, AdvSet]  = withCfg(cfg.addHeader(Header.Authorization.Basic(u, p)))
    def retry(s: Schedule[Any, Any, Any]): Builder[Slice, S, P, RecSet, AdvSet] = withCfg(cfg.withRetry(s))
    def retryOnStatus(p: Status => Boolean): Builder[Slice, S, P, RecSet, AdvSet] = withCfg(cfg.withRetryOnStatus(p))
    def oauth(flow: OAuthFlow): Builder[Slice, S, P, RecSet, AdvSet]           = withCfg(cfg.withOAuth(flow))

  extension [Slice, S, P](b: Builder[Slice, S, P, Provided, Provided])
    def toSlicedConnector(id: String, version: String)(using
        Tag[S]
    ): SlicedConnector[Slice, S, Client & Tracer, Throwable] =
      require(
        "^[a-zA-Z0-9_]+$".r.matches(id),
        s"Connector id '$id' contains invalid characters. Use alphanumerics and underscores only."
      )
      val slicesFn  = b.slicesFn
      val request   = b.requestFn
      val parse     = b.parseFn
      val records   = b.recordsFn.get
      val maybeNext = b.nextRequestFn
      val advance   = b.advanceFn.get
      val recover   = b.recoverFn
      val par       = b.parallelism_
      val cid       = id
      val cversion  = version

      new SlicedConnector[Slice, S, Client & Tracer, Throwable]:
        def id: String                = cid
        def version: String           = cversion
        override def parallelism: Int = par

        def slices(state: S): ZStream[Client & Tracer, Throwable, Slice] = slicesFn(state)

        def extract(slice: Slice): ZStream[Client & Tracer & ExloState[S], Throwable, Unit] =
          // Cursor walk within one slice. Records and state advances flow through ExloState;
          // STM serializes concurrent slice fibers' state updates. Each slice builds its own
          // TokenManager (small token-endpoint overhead per slice; trade-off accepted to keep
          // the code simple — parallel slices = a few extra initial fetches at run start).
          sealed trait Step
          case object Start                  extends Step
          final case class More(req: Request) extends Step
          case object Done                   extends Step

          def runStep(step: Step, tm: Option[TokenManager]): ZIO[Client & Tracer & ExloState[S], Throwable, Step] =
            for
              state <- ExloState.current[S]
              req = step match
                      case Start     => request(slice, state)
                      case More(r)   => r
                      case Done      => throw new MatchError(step) // unreachable
              attempt <- HttpExec
                           .execute(req, b.cfg, tm)
                           .flatMap(parse)
                           .either
              next <- attempt match
                        case Right(page) =>
                          for
                            _        <- ExloState.emit[S](records(page))
                            _        <- ExloState.update[S](s => advance(s, slice, page))
                            newState <- ExloState.current[S]
                          yield maybeNext.flatMap(_(slice, newState, page).map(More(_))).getOrElse(Done)
                        case Left(err) =>
                          recover.flatMap(_.lift((slice, state, err))) match
                            case Some(RecoveryAction.Continue(newState)) =>
                              ExloState.update[S](_ => newState).as(Start)
                            case _ =>
                              ZIO.fail(err)
            yield next

          // One per-slice span wraps the unfold so all HTTP-attempt spans for this slice
          // share a parent. The slice key is best-effort — `Slice.toString` is fine for
          // common cases (case classes, primitives) and the framework doesn't constrain it.
          ZStream.unwrap {
            ZIO.foreach(b.cfg.oauthFlow)(TokenManager.make).map { tm =>
              val unfold = ZStream.unfoldZIO[Client & Tracer & ExloState[S], Throwable, Unit, Step](Start) {
                case Done => ZIO.succeed(None)
                case step => runStep(step, tm).map(next => Some(((), next)))
              }
              ZStream.fromZIO {
                ZIO.serviceWithZIO[Tracer] { tracer =>
                  tracer.span(s"$cid.slice") { span =>
                    span.setAttribute("exlo.connector_id", cid) *>
                      span.setAttribute("exlo.slice", slice.toString) *>
                      unfold.runDrain
                  }
                }
              }.drain
            }
          }
