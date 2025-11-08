package exlo.yaml.service

import exlo.yaml.domain.YamlRuntimeError
import exlo.yaml.spec.Auth
import io.circe.parser.*
import zio.*
import zio.http.*

import java.time.Instant

/**
 * Service for applying authentication to HTTP requests.
 *
 * Transforms Auth ADT into HTTP headers. Handles OAuth token acquisition and
 * caching.
 */
trait Authenticator:

  /**
   * Apply authentication configuration to request headers.
   *
   * @param auth
   *   Authentication configuration
   * @param existingHeaders
   *   Current request headers
   * @return
   *   Updated headers with auth applied
   */
  def authenticate(
    auth: Auth,
    existingHeaders: Map[String, String]
  ): IO[Throwable, Map[String, String]]

object Authenticator:

  /**
   * Accessor for authenticate.
   *
   * Use: `Authenticator.authenticate(auth, headers)`
   */
  def authenticate(
    auth: Auth,
    existingHeaders: Map[String, String]
  ): ZIO[Authenticator, Throwable, Map[String, String]] =
    ZIO.serviceWithZIO[Authenticator](_.authenticate(auth, existingHeaders))

  /** OAuth token cache entry. */
  case class TokenCacheEntry(
    accessToken: String,
    expiresAt: Instant
  )

  /** Live implementation with OAuth token caching. */
  case class Live(
    client: Client,
    tokenCache: Ref[Map[String, TokenCacheEntry]]
  ) extends Authenticator:

    override def authenticate(
      auth: Auth,
      existingHeaders: Map[String, String]
    ): IO[Throwable, Map[String, String]] =
      auth match
        case Auth.NoAuth =>
          ZIO.succeed(existingHeaders)

        case Auth.ApiKey(headerName, value) =>
          ZIO.succeed(existingHeaders + (headerName -> value))

        case Auth.Bearer(token) =>
          ZIO.succeed(existingHeaders + ("Authorization" -> s"Bearer $token"))

        case oauth: Auth.OAuth =>
          obtainOAuthToken(oauth).map(token => existingHeaders + ("Authorization" -> s"Bearer $token"))

    /**
     * Obtain OAuth token (from cache or by requesting new one).
     *
     * @param oauth
     *   OAuth configuration
     * @return
     *   Access token
     */
    private def obtainOAuthToken(oauth: Auth.OAuth): IO[Throwable, String] =
      val cacheKey = s"${oauth.tokenUrl}:${oauth.clientId}"

      for
        now   <- Clock.instant
        cache <- tokenCache.get
        token <- cache.get(cacheKey) match
          case Some(entry) if entry.expiresAt.isAfter(now) =>
            // Use cached token
            ZIO.succeed(entry.accessToken)
          case _                                           =>
            // Request new token
            requestNewToken(oauth, cacheKey)
      yield token

    /**
     * Request new OAuth token from token endpoint.
     *
     * @param oauth
     *   OAuth configuration
     * @param cacheKey
     *   Cache key for storing token
     * @return
     *   Access token
     */
    private def requestNewToken(
      oauth: Auth.OAuth,
      cacheKey: String
    ): IO[Throwable, String] =
      ZIO.scoped {
        for
          // Build token request body
          bodyParams <- ZIO.succeed(
            Map(
              "grant_type"    -> "client_credentials",
              "client_id"     -> oauth.clientId,
              "client_secret" -> oauth.clientSecret
            ) ++ oauth.scopes.map("scope" -> _).toMap
          )

          formBody <- ZIO.succeed(
            bodyParams.map { case (k, v) => s"$k=$v" }.mkString("&")
          )

          // Build request
          url      <- ZIO
            .fromEither(
              URL.decode(oauth.tokenUrl)
            )
            .mapError(e =>
              YamlRuntimeError.HttpError(
                oauth.tokenUrl,
                0,
                s"Invalid token URL: ${e.getMessage}"
              )
            )

          request = Request
            .post(url, Body.fromString(formBody))
            .addHeader(Header.ContentType(MediaType.application.`x-www-form-urlencoded`))

          // Execute request
          response    <- client
            .request(request)
            .mapError(e =>
              YamlRuntimeError.HttpError(
                oauth.tokenUrl,
                0,
                s"OAuth token request failed: ${e.getMessage}"
              )
            )

          // Check status
          _           <- ZIO.when(!response.status.isSuccess) {
            response.body.asString.flatMap { bodyText =>
              ZIO.fail(
                YamlRuntimeError.HttpError(
                  oauth.tokenUrl,
                  response.status.code,
                  s"OAuth token request failed: $bodyText"
                )
              )
            }
          }

          // Parse response
          bodyText    <- response.body.asString
          json        <- ZIO
            .fromEither(parse(bodyText))
            .mapError(e =>
              YamlRuntimeError.ParseError(
                oauth.tokenUrl,
                s"Failed to parse OAuth response: ${e.getMessage}"
              )
            )

          // Extract access token and expiration
          accessToken <- ZIO
            .fromEither(
              json.hcursor.downField("access_token").as[String]
            )
            .mapError(e =>
              YamlRuntimeError.ParseError(
                oauth.tokenUrl,
                s"Missing access_token in OAuth response: ${e.getMessage}"
              )
            )

          expiresIn <- ZIO
            .fromEither(
              json.hcursor.downField("expires_in").as[Int]
            )
            .orElse(ZIO.succeed(3600)) // Default to 1 hour

          // Cache token (subtract 60 seconds for safety margin)
          now       <- Clock.instant
          expiresAt = now.plusSeconds(expiresIn.toLong - 60)
          _ <- tokenCache.update(_ + (cacheKey -> TokenCacheEntry(accessToken, expiresAt)))
        yield accessToken
      }

  object Live:

    /**
     * ZLayer for Authenticator.
     *
     * Depends on zio.http.Client for OAuth token requests.
     */
    val layer: ZLayer[Client, Nothing, Authenticator] =
      ZLayer.fromZIO {
        for
          client <- ZIO.service[Client]
          cache  <- Ref.make(Map.empty[String, TokenCacheEntry])
        yield Live(client, cache)
      }
