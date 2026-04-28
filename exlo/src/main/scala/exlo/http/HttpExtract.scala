package exlo.http

import exlo.domain.Connector
import exlo.runtime.ExloState
import zio.*
import zio.http.*
import zio.stream.ZStream

/**
 * Cursor-paginated HTTP extraction. The connector author describes:
 *
 *   - how to build the initial request from current state — `.request`
 *   - how to parse a response into a typed page — `.parse`
 *   - how to extract records from the parsed page — `.records`
 *   - (optional) how to derive the next request from state + parsed page — `.nextRequest`
 *   - how to advance state given the parsed page — `.advance`
 *
 * The framework drives a single sequential walk: request → parse → records → advance →
 * maybe-next. Output flows via `ExloState`: records via `emit`, state via `update`. The
 * watermark model in the runtime ensures state writes only land after their corresponding
 * records are durable.
 *
 * For embarrassingly-parallel windowed/range sources, see [[HttpSlice]].
 */
object HttpExtract:

  // Phantom type tags. Empty/Provided distinction is only visible to the compiler.
  sealed trait BuilderState
  sealed trait Missing  extends BuilderState
  sealed trait Provided extends BuilderState

  /** Start a new builder. Stage 1: only `.request` is available. */
  def apply[S]: RequestBuilder[S, Missing] = new RequestBuilder(None)

  /**
   * Stateless one-shot extraction — full pull, no pagination, no state. Many B2B endpoints
   * fit this shape (reference data, lookups, slow-changing dimensions).
   */
  object fullPull:
    def request(req: => Request): FullPullPre[Provided] = new FullPullPre(Some(() => req))

  // ---- Stage 1: collect the request function ---------------------------------------------

  final class RequestBuilder[S, ReqSet <: BuilderState] private[http] (
      private[http] val requestFn: Option[S => Request]
  ):
    def request(f: S => Request): RequestBuilder[S, Provided] =
      new RequestBuilder(Some(f))

  /** `.parse` is only available once `.request` has been provided. */
  extension [S](rb: RequestBuilder[S, Provided])
    def parse[P](f: Response => ZIO[Any, Throwable, P]): ParsedBuilder[S, P, Missing, Missing] =
      new ParsedBuilder(rb.requestFn.get, f, None, None, None)

  // ---- Stage 2: records / nextRequest / advance ------------------------------------------

  final class ParsedBuilder[S, P, RecSet <: BuilderState, AdvSet <: BuilderState] private[http] (
      private[http] val requestFn: S => Request,
      private[http] val parseFn: Response => ZIO[Any, Throwable, P],
      private[http] val recordsFn: Option[P => Chunk[String]],
      private[http] val nextRequestFn: Option[(S, P) => Option[Request]],
      private[http] val advanceFn: Option[(S, P) => S],
      private[http] val cfg: HttpExecConfig = HttpExecConfig()
  ):

    private def withCfg(c: HttpExecConfig): ParsedBuilder[S, P, RecSet, AdvSet] =
      new ParsedBuilder(requestFn, parseFn, recordsFn, nextRequestFn, advanceFn, c)

    def records(f: P => Chunk[String]): ParsedBuilder[S, P, Provided, AdvSet] =
      new ParsedBuilder(requestFn, parseFn, Some(f), nextRequestFn, advanceFn, cfg)

    def nextRequest(f: (S, P) => Option[Request]): ParsedBuilder[S, P, RecSet, AdvSet] =
      new ParsedBuilder(requestFn, parseFn, recordsFn, Some(f), advanceFn, cfg)

    def advance(f: (S, P) => S): ParsedBuilder[S, P, RecSet, Provided] =
      new ParsedBuilder(requestFn, parseFn, recordsFn, nextRequestFn, Some(f), cfg)

    def header(h: Header): ParsedBuilder[S, P, RecSet, AdvSet]            = withCfg(cfg.addHeader(h))
    def bearer(token: String): ParsedBuilder[S, P, RecSet, AdvSet]        = withCfg(cfg.addHeader(Header.Authorization.Bearer(token)))
    def basicAuth(u: String, p: String): ParsedBuilder[S, P, RecSet, AdvSet] = withCfg(cfg.addHeader(Header.Authorization.Basic(u, p)))
    def retry(s: Schedule[Any, Any, Any]): ParsedBuilder[S, P, RecSet, AdvSet] = withCfg(cfg.withRetry(s))
    def retryOnStatus(p: Status => Boolean): ParsedBuilder[S, P, RecSet, AdvSet] = withCfg(cfg.withRetryOnStatus(p))

    /**
     * OAuth 2 — `Authorization: Bearer <token>` injected per request, with token
     * acquisition + refresh handled automatically by [[TokenManager]]. Pick the flow
     * that matches your provider:
     *   - [[OAuthFlow.ClientCredentials]] for M2M
     *   - [[OAuthFlow.RefreshToken]] for user-delegated APIs (Google, GitHub, etc.)
     *   - [[OAuthFlow.Password]] for legacy systems still using ROPC
     */
    def oauth(flow: OAuthFlow): ParsedBuilder[S, P, RecSet, AdvSet] = withCfg(cfg.withOAuth(flow))

  /** `.toConnector` available when both `records` and `advance` are set. */
  extension [S, P](b: ParsedBuilder[S, P, Provided, Provided])
    def toConnector(id: String, version: String)(using Tag[S]): Connector[S, Client, Throwable] =
      Connector.fromStream[S, Client, Throwable](id, version) {
        val initial   = b.requestFn
        val parse     = b.parseFn
        val records   = b.recordsFn.get
        val maybeNext = b.nextRequestFn
        val advance   = b.advanceFn.get

        sealed trait Step
        case object Start                  extends Step
        final case class More(req: Request) extends Step
        case object Done                   extends Step

        // Build a TokenManager once per connector run (if oauth was configured); it caches
        // the access token + handles refresh. Inherited by all unfold iterations via closure.
        ZStream.unwrap {
          ZIO.foreach(b.cfg.oauthFlow)(TokenManager.make).map { tm =>
            ZStream.unfoldZIO[Client & ExloState[S], Throwable, Unit, Step](Start) {
              case Done => ZIO.succeed(None)
              case step =>
                for
                  state <- ExloState.current[S]
                  req = step match
                          case Start     => initial(state)
                          case More(r)   => r
                          case Done      => throw new MatchError(step) // unreachable
                  resp <- HttpExec.execute(req, b.cfg, tm)
                  page <- parse(resp)
                  recs = records(page)
                  _ <- ExloState.emit[S](recs)
                  _ <- ExloState.update[S](s => advance(s, page))
                  newState <- ExloState.current[S]
                  nextStep = maybeNext
                               .flatMap(_(newState, page).map(More(_)))
                               .getOrElse(Done)
                yield Some(((), nextStep))
            }
          }
        }
      }

  // ---- fullPull: stateless single-request -----------------------------------------------

  final class FullPullPre[ReqSet <: BuilderState] private[http] (
      private[http] val requestThunk: Option[() => Request]
  )

  extension (p: FullPullPre[Provided])
    def parse[P](f: Response => ZIO[Any, Throwable, P]): FullPullParsed[P, Missing] =
      new FullPullParsed(p.requestThunk.get, f, None)

  final class FullPullParsed[P, RecSet <: BuilderState] private[http] (
      private[http] val requestThunk: () => Request,
      private[http] val parseFn: Response => ZIO[Any, Throwable, P],
      private[http] val recordsFn: Option[P => Chunk[String]],
      private[http] val cfg: HttpExecConfig = HttpExecConfig()
  ):
    private def withCfg(c: HttpExecConfig): FullPullParsed[P, RecSet] =
      new FullPullParsed(requestThunk, parseFn, recordsFn, c)

    def records(f: P => Chunk[String]): FullPullParsed[P, Provided] =
      new FullPullParsed(requestThunk, parseFn, Some(f), cfg)

    def header(h: Header): FullPullParsed[P, RecSet]                   = withCfg(cfg.addHeader(h))
    def bearer(token: String): FullPullParsed[P, RecSet]               = withCfg(cfg.addHeader(Header.Authorization.Bearer(token)))
    def basicAuth(u: String, p: String): FullPullParsed[P, RecSet]     = withCfg(cfg.addHeader(Header.Authorization.Basic(u, p)))
    def retry(s: Schedule[Any, Any, Any]): FullPullParsed[P, RecSet]   = withCfg(cfg.withRetry(s))
    def retryOnStatus(p: Status => Boolean): FullPullParsed[P, RecSet] = withCfg(cfg.withRetryOnStatus(p))
    def oauth(flow: OAuthFlow): FullPullParsed[P, RecSet]              = withCfg(cfg.withOAuth(flow))

  extension [P](b: FullPullParsed[P, Provided])
    def toConnector(id: String, version: String): Connector[Unit, Client, Throwable] =
      Connector.fromStream[Unit, Client, Throwable](id, version) {
        val req     = b.requestThunk
        val parse   = b.parseFn
        val records = b.recordsFn.get

        ZStream.unwrap {
          ZIO.foreach(b.cfg.oauthFlow)(TokenManager.make).map { tm =>
            ZStream.fromZIO {
              for
                resp <- HttpExec.execute(req(), b.cfg, tm)
                page <- parse(resp)
                _    <- ExloState.emit[Unit](records(page))
                _    <- ExloState.update[Unit](_ => ())
              yield ()
            }
          }
        }
      }
