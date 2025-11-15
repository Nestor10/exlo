package exlo.yaml.service

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import exlo.yaml.domain.YamlRuntimeError
import exlo.yaml.spec.Auth
import exlo.yaml.template.TemplateValue
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
  ): ZIO[RuntimeContext & TemplateEngine, Throwable, Map[String, String]]

object Authenticator:

  /**
   * Accessor for authenticate.
   *
   * Use: `Authenticator.authenticate(auth, headers)`
   */
  def authenticate(
    auth: Auth,
    existingHeaders: Map[String, String]
  ): ZIO[Authenticator & RuntimeContext & TemplateEngine, Throwable, Map[String, String]] =
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

    private val jsonMapper: ObjectMapper =
      val mapper = new ObjectMapper()
      mapper.registerModule(DefaultScalaModule)
      mapper

    override def authenticate(
      auth: Auth,
      existingHeaders: Map[String, String]
    ): ZIO[RuntimeContext & TemplateEngine, Throwable, Map[String, String]] =
      auth match
        case Auth.NoAuth =>
          ZIO.succeed(existingHeaders)

        case Auth.ApiKey(headerName, value) =>
          ZIO.succeed(existingHeaders + (headerName -> value))

        case Auth.ApiKeyAuthenticator(apiToken, injectInto) =>
          // Airbyte's ApiKeyAuthenticator with inject_into configuration
          // Render the api_token template (supports {{ config['api_key'] }})
          for
            templateEngine <- ZIO.service[TemplateEngine]
            renderedToken  <- templateEngine.render(apiToken)
            headers        <- injectInto.injectInto match
              case "header"            =>
                ZIO.succeed(existingHeaders + (injectInto.fieldName -> renderedToken))
              case "request_parameter" =>
                // For query params, we'd need to modify the request URL
                // For now, treat as header (will be enhanced in future)
                ZIO.succeed(existingHeaders + (injectInto.fieldName -> renderedToken))
              case other               =>
                ZIO.fail(
                  YamlRuntimeError.AuthError(
                    s"Unsupported inject_into location: $other. Supported: header, request_parameter"
                  )
                )
          yield headers

        case Auth.Bearer(token) =>
          ZIO.succeed(existingHeaders + ("Authorization" -> s"Bearer $token"))

        case Auth.BearerAuthenticator(apiToken) =>
          // Airbyte's BearerAuthenticator uses api_token field
          ZIO.succeed(existingHeaders + ("Authorization" -> s"Bearer $apiToken"))

        case oauth: Auth.OAuth =>
          obtainOAuthToken(oauth).map(token => existingHeaders + ("Authorization" -> s"Bearer $token"))

        case Auth.SelectiveAuth(selectionPath, authenticators) =>
          resolveSelectiveAuth(selectionPath, authenticators, existingHeaders)

    /**
     * Resolve SelectiveAuth by reading config path and selecting authenticator.
     *
     * @param selectionPath
     *   JSON path to read from config (e.g., ["credentials", "option_title"])
     * @param authenticators
     *   Map from selector value to authenticator
     * @param existingHeaders
     *   Current request headers
     * @return
     *   Updated headers with selected auth applied
     */
    private def resolveSelectiveAuth(
      selectionPath: List[String],
      authenticators: Map[String, Auth],
      existingHeaders: Map[String, String]
    ): ZIO[RuntimeContext & TemplateEngine, Throwable, Map[String, String]] =
      for
        // Read config to get selector value
        templateContext <- RuntimeContext.getTemplateContext

        // Navigate the config path to find selector value
        selectorValue <- ZIO
          .attempt {
            // Start with config from template context
            var current: TemplateValue = templateContext.getOrElse(
              "config",
              throw new RuntimeException(s"Config not available in runtime context")
            )

            // Navigate through each path segment
            selectionPath.foreach { key =>
              current match
                case TemplateValue.Obj(map) =>
                  current = map
                    .get(key)
                    .flatten
                    .getOrElse(
                      throw new RuntimeException(s"Path component '$key' not found")
                    )
                case _                      =>
                  throw new RuntimeException(s"Expected Obj at path component '$key', got: $current")
            }

            // Extract final string value
            current match
              case TemplateValue.Str(s) => s
              case other                => throw new RuntimeException(s"Expected Str value, got: $other")
          }
          .mapError(e =>
            YamlRuntimeError.AuthError(
              s"Failed to read authenticator selection path ${selectionPath.mkString(".")}: ${e.getMessage}"
            )
          )

        // Look up selected authenticator
        selectedAuth  <- ZIO
          .fromOption(authenticators.get(selectorValue))
          .mapError(_ =>
            YamlRuntimeError.AuthError(
              s"No authenticator found for selector value '$selectorValue'. Available: ${authenticators.keys.mkString(", ")}"
            )
          )

        // Recursively authenticate with selected authenticator
        result        <- authenticate(selectedAuth, existingHeaders)
      yield result

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
          jsonNode    <- ZIO
            .attempt(jsonMapper.readTree(bodyText))
            .mapError(e =>
              YamlRuntimeError.ParseError(
                oauth.tokenUrl,
                s"Failed to parse OAuth response: ${e.getMessage}"
              )
            )

          // Extract access token and expiration
          accessToken <- ZIO
            .attempt {
              val tokenNode = jsonNode.get("access_token")
              if (tokenNode == null || tokenNode.isNull)
                throw new Exception("Missing access_token field")
              tokenNode.asText()
            }
            .mapError(e =>
              YamlRuntimeError.ParseError(
                oauth.tokenUrl,
                s"Missing access_token in OAuth response: ${e.getMessage}"
              )
            )

          expiresIn <- ZIO
            .attempt {
              val expiresNode = jsonNode.get("expires_in")
              if (expiresNode == null || expiresNode.isNull) 3600
              else expiresNode.asInt()
            }
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
