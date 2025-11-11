package exlo.yaml.domain

/**
 * Typed errors for YAML runtime operations.
 *
 * Following Zionomicon Chapter 3: Error Model
 * - Use typed errors (E in ZIO[R, E, A]) for expected, recoverable failures
 * - Keep errors as ADTs for exhaustive pattern matching
 *
 * Note: YamlRuntimeError is its own sealed trait (not extending ExloError due to sealed restriction).
 * Can be converted to ExloError.RuntimeError when crossing module boundary.
 */
sealed trait YamlRuntimeError extends Throwable

object YamlRuntimeError:

  /**
   * YAML spec file could not be loaded or parsed.
   *
   * @param path
   *   Path to the YAML file
   * @param cause
   *   Underlying error message
   */
  case class InvalidSpec(path: String, cause: String) extends YamlRuntimeError:
    override def getMessage: String = s"Invalid YAML spec at $path: $cause"

  /**
   * Jinja template could not be rendered.
   *
   * @param template
   *   The template string that failed
   * @param cause
   *   Underlying error message
   */
  case class TemplateError(template: String, cause: String) extends YamlRuntimeError:
    override def getMessage: String = s"Template rendering failed: '$template' - $cause"

  /**
   * HTTP request failed.
   *
   * @param url
   *   The URL that was requested
   * @param status
   *   HTTP status code
   * @param body
   *   Response body (may be truncated)
   * @param headers
   *   Response headers (for retry/backoff logic)
   */
  case class HttpError(url: String, status: Int, body: String, headers: Map[String, String] = Map.empty)
      extends YamlRuntimeError:
    override def getMessage: String = s"HTTP $status from $url: ${body.take(200)}"

  /**
   * JSON parsing or extraction failed.
   *
   * @param path
   *   JSONPath that was being extracted
   * @param cause
   *   Underlying error message
   */
  case class ParseError(path: String, cause: String) extends YamlRuntimeError:
    override def getMessage: String = s"Parse error at path '$path': $cause"

  /**
   * Authentication configuration error.
   *
   * @param cause
   *   Description of the authentication error
   */
  case class AuthError(cause: String) extends YamlRuntimeError:
    override def getMessage: String = s"Authentication error: $cause"

  /**
   * Config validation failed against JSON Schema.
   *
   * @param errors
   *   List of validation errors from JSON Schema validator
   */
  case class ConfigValidationError(errors: List[String]) extends YamlRuntimeError:

    override def getMessage: String =
      s"Config validation failed:\n${errors.map(e => s"  - $e").mkString("\n")}"
