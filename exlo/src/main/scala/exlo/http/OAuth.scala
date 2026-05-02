package exlo.http

import exlo.domain.ExloError
import zio.*
import zio.http.*
import zio.json.*

import java.net.URLEncoder
import java.time.Instant

/**
 * OAuth 2 flows the framework supports.
 *
 * Covers the modern flows useful for server-side connector workloads
 * plus legacy Resource Owner Password Credentials (still required by
 * older systems despite being formally discouraged in OAuth 2.1).
 *
 * Not supported:
 *   - **Authorization Code grant** itself (requires browser interaction
 *     — not a connector concern). The framework consumes the *result*
 *     via [[OAuthFlow.RefreshToken]]: the user authorizes once in a
 *     browser, hands the connector a refresh token, the connector
 *     trades it for access tokens forever.
 *   - **Implicit** (deprecated, replaced by code+PKCE).
 *   - **Device code** (rare for connector workloads).
 */
sealed trait OAuthFlow

object OAuthFlow:

  /** Pure machine-to-machine. App POSTs `client_id`+`client_secret`,
   *  gets back `access_token`. No user, no refresh token; refetched
   *  every time the cache expires. Common for Slack apps, Twitter API
   *  v2, server-side Microsoft Graph apps. */
  final case class ClientCredentials(
      tokenUrl:     URL,
      clientId:     String,
      clientSecret: String,
      scope:        Option[String] = None,
      audience:     Option[String] = None
  ) extends OAuthFlow

  /** Long-lived refresh token from a prior Authorization Code grant. The
   *  user authorized the app once (via browser), the app stored the
   *  refresh token, and exchanges it for access tokens as needed. Most
   *  useful flow for connectors against user-facing APIs (Google,
   *  GitHub, Microsoft, Salesforce). If the response rotates the
   *  refresh token, the cached one is replaced. */
  final case class RefreshToken(
      tokenUrl:     URL,
      clientId:     String,
      clientSecret: String,
      refreshToken: String,
      scope:        Option[String] = None
  ) extends OAuthFlow

  /** Resource Owner Password Credentials (ROPC) — the legacy "user
   *  types password into the app" grant. Marked deprecated in RFC 6749;
   *  omitted from OAuth 2.1. Still in use by some older systems (older
   *  Salesforce, on-prem identity providers).
   *
   *  If the response includes a refresh_token, subsequent refreshes use
   *  it (no need to retransmit the password).
   *
   *  `grantType` defaults to the standard `"password"` but can be
   *  overridden for providers shipping a non-standard ROPC variant.
   *  `queryParams` are merged into the token URL's query string for
   *  providers that demand certain fields (`username`, `client_id`,
   *  even `grant_type`) as URL params rather than POST body fields. */
  final case class Password(
      tokenUrl:     URL,
      clientId:     String,
      username:     String,
      password:     String,
      clientSecret: Option[String]      = None,
      scope:        Option[String]      = None,
      grantType:    String              = "password",
      queryParams:  Map[String, String] = Map.empty
  ) extends OAuthFlow

/** Standard OAuth 2 token endpoint response (RFC 6749 §5.1). */
final case class TokenResponse(
    access_token:  String,
    expires_in:    Option[Long]   = None,
    refresh_token: Option[String] = None,
    token_type:    Option[String] = None,
    scope:         Option[String] = None
)

object TokenResponse:
  given JsonDecoder[TokenResponse] = DeriveJsonDecoder.gen[TokenResponse]

/**
 * Stateful access-token cache with auto-refresh. Built once per
 * connector run via [[TokenManager.make]]. `token` returns a valid
 * access token, refreshing automatically when expiry is within a
 * 60-second safety margin.
 */
trait TokenManager:
  def token: IO[ExloError, String]

object TokenManager:

  /** Refresh tokens this far in advance of their nominal expiry. */
  private val refreshMargin: Duration = 60.seconds

  /** Assumed lifetime when `expires_in` isn't supplied. */
  private val defaultLifetime: Duration = 1.hour

  /** Build a TokenManager scoped to the given flow + client. The cache
   *  is per-instance — each connector run gets its own (no cross-run
   *  state leakage). */
  def make(flow: OAuthFlow, client: Client): UIO[TokenManager] =
    Ref.make(Option.empty[CachedToken]).map(new Live(flow, client, _))

  private final case class CachedToken(
      accessToken:  String,
      expiresAt:    Instant,
      refreshToken: Option[String]
  ):
    def isFresh(now: Instant): Boolean =
      now.plusSeconds(refreshMargin.getSeconds).isBefore(expiresAt)

  private final class Live(
      initialFlow: OAuthFlow,
      client:      Client,
      cache:       Ref[Option[CachedToken]]
  ) extends TokenManager:

    def token: IO[ExloError, String] =
      for
        now    <- Clock.instant
        cached <- cache.get
        result <- cached match
                    case Some(c) if c.isFresh(now) => ZIO.succeed(c.accessToken)
                    case _                         => fetchAndCache(cached.flatMap(_.refreshToken))
      yield result

    private def fetchAndCache(cachedRefresh: Option[String]): IO[ExloError, String] =
      for
        resp        <- fetchToken(initialFlow, cachedRefresh)
        now         <- Clock.instant
        expiry       = now.plusSeconds(resp.expires_in.getOrElse(defaultLifetime.getSeconds))
        // Prefer the response's new refresh_token if present (token rotation);
        // else carry forward the cached one (some providers issue once, expect reuse).
        nextRefresh  = resp.refresh_token.orElse(cachedRefresh)
        _           <- cache.set(Some(CachedToken(resp.access_token, expiry, nextRefresh)))
      yield resp.access_token

    private def fetchToken(flow: OAuthFlow, cachedRefresh: Option[String]): IO[ExloError, TokenResponse] =
      val (url, body) = buildRequest(flow, cachedRefresh)
      val req = Request
        .post(url, Body.fromString(body))
        .addHeader(Header.ContentType(MediaType.application.`x-www-form-urlencoded`))
      val effect: ZIO[Client, ExloError, TokenResponse] =
        for
          resp <- ZClient.batched(req).mapError(t =>
                    ExloError.ConnectorFailure(s"OAuth token endpoint request failed: ${t.getMessage}", t)
                  )
          text <- resp.body.asString.mapError(t =>
                    ExloError.ConnectorFailure(s"OAuth token body read failed: ${t.getMessage}", t)
                  )
          _    <- ZIO
                    .fail(ExloError.ConnectorFailure(
                      s"OAuth token endpoint ${url.encode} returned ${resp.status.code}: ${truncate(text, 500)}"
                    ))
                    .when(resp.status.code >= 400)
          parsed <- ZIO.fromEither(text.fromJson[TokenResponse]).mapError(msg =>
                      ExloError.ConnectorFailure(
                        s"OAuth token parse error: $msg (body=${truncate(text, 500)})",
                        new RuntimeException(msg)
                      )
                    )
        yield parsed
      effect.provideEnvironment(ZEnvironment(client))

    private def buildRequest(flow: OAuthFlow, cachedRefresh: Option[String]): (URL, String) = flow match

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

      case OAuthFlow.Password(url, id, user, pass, secretOpt, scope, grantType, queryParams) =>
        // After the first fetch, prefer refresh_token if the endpoint issued
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

    private def withQueryParams(url: URL, extras: Map[String, String]): URL =
      if extras.isEmpty then url
      else extras.foldLeft(url) { case (u, (k, v)) => u.addQueryParam(k, v) }

    private def truncate(s: String, n: Int): String =
      if s.length <= n then s else s.take(n) + s"… (+${s.length - n} chars)"
