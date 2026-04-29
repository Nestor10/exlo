package exlo.http

import zio.*
import zio.http.*
import zio.json.*

import java.net.URLEncoder
import java.time.Instant

/**
 * OAuth 2 flows the framework supports out of the box. Cover the modern classics plus the
 * legacy Resource Owner Password Credentials grant (still in the wild despite being
 * formally discouraged).
 *
 * Not supported: the Authorization Code grant itself (requires browser interaction — not a
 * server-side connector concern). The framework consumes the *result* of an auth code
 * grant via [[OAuthFlow.RefreshToken]] — the user authorizes once, hands the connector a
 * refresh token, the connector exchanges it for access tokens forever.
 *
 * Also out of scope: implicit (deprecated), device code (rare for connector workloads).
 */
sealed trait OAuthFlow

object OAuthFlow:

  /**
   * Client Credentials grant — pure machine-to-machine. App POSTs `client_id` +
   * `client_secret`, gets back `access_token`. No user, no refresh token. Re-fetched from
   * the token endpoint on every refresh. Common for Slack apps, Twitter API v2,
   * server-side Microsoft Graph apps.
   */
  final case class ClientCredentials(
      tokenUrl: URL,
      clientId: String,
      clientSecret: String,
      scope: Option[String]    = None,
      audience: Option[String] = None
  ) extends OAuthFlow

  /**
   * Refresh Token grant — used after a one-time Authorization Code grant. The user
   * authorized the app once (via browser), the app stored the resulting refresh token,
   * and exchanges it for access tokens as needed. Typically the most useful flow for
   * connectors against user-facing APIs (Google, GitHub, Microsoft, Salesforce).
   *
   * If the token endpoint returns a new `refresh_token` in the response (token
   * rotation), the framework caches it and uses it for the next refresh.
   */
  final case class RefreshToken(
      tokenUrl: URL,
      clientId: String,
      clientSecret: String,
      refreshToken: String,
      scope: Option[String] = None
  ) extends OAuthFlow

  /**
   * Resource Owner Password Credentials grant — the legacy "user types password into the
   * app" flow. RFC 6749 marked it deprecated; OAuth 2.1 omits it. Still in use by some
   * older systems (older Salesforce, on-prem identity providers). Avoid for new
   * integrations; included for compatibility.
   *
   * If the response includes a `refresh_token`, the framework switches to refresh-token
   * grant for subsequent refreshes (no need to re-send the password).
   *
   * `grantType` defaults to the standard `"password"` value but can be overridden for
   * providers that ship a non-standard variant of ROPC under a different grant name.
   *
   * `queryParams` are merged into the token URL's query string rather than the POST
   * body. Some providers demand certain fields — `username`, `client_id`, even
   * `grant_type` — as URL query parameters while keeping the password in the body.
   * Use this to model those non-standard layouts. Defaults to empty for the standard
   * ROPC path.
   */
  final case class Password(
      tokenUrl: URL,
      clientId: String,
      clientSecret: Option[String]    = None,
      username: String,
      password: String,
      scope: Option[String]           = None,
      grantType: String               = "password",
      queryParams: Map[String, String] = Map.empty
  ) extends OAuthFlow

/** Standard OAuth 2 token endpoint response (RFC 6749 §5.1). */
final case class TokenResponse(
    access_token: String,
    expires_in: Option[Long]    = None,
    refresh_token: Option[String] = None,
    token_type: Option[String]  = None,
    scope: Option[String]       = None
)
object TokenResponse:
  given JsonDecoder[TokenResponse] = DeriveJsonDecoder.gen[TokenResponse]

/**
 * Stateful access-token cache + on-demand refresh. Built once per connector run via
 * [[TokenManager.make]]. The `token` accessor returns a valid access token, refreshing
 * automatically when expiry is within a 60-second safety margin.
 */
trait TokenManager:
  /** Returns the current valid access token, refreshing if expired. */
  def token: ZIO[Client, Throwable, String]

object TokenManager:

  /** Refresh tokens this far in advance of their expiry. */
  private val refreshMargin: Duration = 60.seconds

  /** When `expires_in` isn't provided, assume tokens last this long. */
  private val defaultLifetime: Duration = 1.hour

  /**
   * Build a Scoped [[TokenManager]] for the given flow. The cache is per-instance; each
   * connector run gets its own (fresh tokens, no cross-run state leakage).
   */
  def make(flow: OAuthFlow): UIO[TokenManager] =
    Ref.make(Option.empty[CachedToken]).map(new Live(flow, _))

  // ---- internals -------------------------------------------------------------------------

  private final case class CachedToken(
      accessToken: String,
      expiresAt: Instant,
      refreshToken: Option[String]
  ):
    def isFresh(now: Instant): Boolean =
      now.plusSeconds(refreshMargin.getSeconds).isBefore(expiresAt)

  private final class Live(initialFlow: OAuthFlow, cache: Ref[Option[CachedToken]])
      extends TokenManager:

    def token: ZIO[Client, Throwable, String] =
      for
        now    <- Clock.instant
        cached <- cache.get
        result <- cached match
                    case Some(c) if c.isFresh(now) => ZIO.succeed(c.accessToken)
                    case _                          => fetchAndCache(cached.flatMap(_.refreshToken))
      yield result

    private def fetchAndCache(
        cachedRefresh: Option[String]
    ): ZIO[Client, Throwable, String] =
      for
        resp   <- fetchToken(initialFlow, cachedRefresh)
        now    <- Clock.instant
        expiry = now.plusSeconds(resp.expires_in.getOrElse(defaultLifetime.getSeconds))
        // Prefer the response's new refresh_token if present; else carry forward the
        // cached one (some providers issue a refresh once, expect you to keep using it).
        nextRefresh = resp.refresh_token.orElse(cachedRefresh)
        _ <- cache.set(Some(CachedToken(resp.access_token, expiry, nextRefresh)))
      yield resp.access_token

    private def fetchToken(
        flow: OAuthFlow,
        cachedRefresh: Option[String]
    ): ZIO[Client, Throwable, TokenResponse] =
      val (url, body) = buildRequest(flow, cachedRefresh)
      val req = Request
        .post(url, Body.fromString(body))
        .addHeader(Header.ContentType(MediaType.application.`x-www-form-urlencoded`))
      for
        resp <- ZClient.batched(req)
        text <- resp.body.asString
        _ <- ZIO
               .fail(
                 new RuntimeException(
                   s"OAuth token endpoint ${url.encode} returned ${resp.status.code}: ${truncate(text, 500)}"
                 )
               )
               .when(resp.status.code >= 400)
        parsed <- ZIO
                    .fromEither(text.fromJson[TokenResponse])
                    .mapError(e => new RuntimeException(s"OAuth token parse error: $e (body=${truncate(text, 500)})"))
      yield parsed

    private def buildRequest(
        flow: OAuthFlow,
        cachedRefresh: Option[String]
    ): (URL, String) = flow match

      case OAuthFlow.ClientCredentials(url, id, secret, scope, audience) =>
        val params = Map(
          "grant_type"    -> "client_credentials",
          "client_id"     -> id,
          "client_secret" -> secret
        ) ++ scope.map("scope" -> _) ++ audience.map("audience" -> _)
        url -> formEncode(params)

      case OAuthFlow.RefreshToken(url, id, secret, originalRt, scope) =>
        val rt = cachedRefresh.getOrElse(originalRt)
        val params = Map(
          "grant_type"    -> "refresh_token",
          "client_id"     -> id,
          "client_secret" -> secret,
          "refresh_token" -> rt
        ) ++ scope.map("scope" -> _)
        url -> formEncode(params)

      case OAuthFlow.Password(url, id, secretOpt, user, pass, scope, grantType, queryParams) =>
        // After the first fetch, prefer refresh_token grant if the token endpoint issued
        // one (avoids retransmitting the password on every refresh).
        cachedRefresh match
          case Some(rt) =>
            val params = Map(
              "grant_type"    -> "refresh_token",
              "client_id"     -> id,
              "refresh_token" -> rt
            ) ++ secretOpt.map("client_secret" -> _) ++ scope.map("scope" -> _)
            url -> formEncode(params)
          case None =>
            val params = Map(
              "grant_type" -> grantType,
              "client_id"  -> id,
              "username"   -> user,
              "password"   -> pass
            ) ++ secretOpt.map("client_secret" -> _) ++ scope.map("scope" -> _)
            withQueryParams(url, queryParams) -> formEncode(params)

    private def formEncode(params: Map[String, String]): String =
      params
        .map { case (k, v) =>
          s"${URLEncoder.encode(k, "UTF-8")}=${URLEncoder.encode(v, "UTF-8")}"
        }
        .mkString("&")

    /**
     * Merge additional query parameters into a token URL. Used by the Password flow's
     * `queryParams` field to support providers that demand certain credentials in the
     * URL rather than the POST body.
     */
    private def withQueryParams(url: URL, extras: Map[String, String]): URL =
      if extras.isEmpty then url
      else extras.foldLeft(url) { case (u, (k, v)) => u.addQueryParam(k, v) }

    private def truncate(s: String, n: Int): String =
      if s.length <= n then s else s.take(n) + s"… (+${s.length - n} chars)"
