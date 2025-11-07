package exlo.yaml.service

import com.hubspot.jinjava.Jinjava
import exlo.yaml.YamlRuntimeError
import zio.*

import scala.jdk.CollectionConverters.*

/**
 * Service for Jinja2 template rendering and evaluation.
 *
 * Provides Jinja2 compatibility for Airbyte-style YAML specs using Jinjava
 * (HubSpot's JVM implementation).
 */
trait TemplateEngine:

  /**
   * Render Jinja2 template with context.
   *
   * @param template
   *   Jinja2 template string
   * @param context
   *   Variables available to template
   * @return
   *   Rendered string
   */
  def render(
    template: String,
    context: Map[String, Any]
  ): IO[Throwable, String]

  /**
   * Evaluate Jinja2 condition expression.
   *
   * @param condition
   *   Boolean expression (without {{ }})
   * @param context
   *   Variables available to expression
   * @return
   *   Boolean result
   */
  def evaluateCondition(
    condition: String,
    context: Map[String, Any]
  ): IO[Throwable, Boolean]

object TemplateEngine:

  /**
   * Accessor for render.
   *
   * Use: `TemplateEngine.render(template, context)`
   */
  def render(
    template: String,
    context: Map[String, Any]
  ): ZIO[TemplateEngine, Throwable, String] =
    ZIO.serviceWithZIO[TemplateEngine](_.render(template, context))

  /**
   * Accessor for evaluateCondition.
   *
   * Use: `TemplateEngine.evaluateCondition(condition, context)`
   */
  def evaluateCondition(
    condition: String,
    context: Map[String, Any]
  ): ZIO[TemplateEngine, Throwable, Boolean] =
    ZIO.serviceWithZIO[TemplateEngine](_.evaluateCondition(condition, context))

  /** Live implementation using Jinjava. */
  case class Live(jinjava: Jinjava) extends TemplateEngine:

    override def render(
      template: String,
      context: Map[String, Any]
    ): IO[Throwable, String] =
      ZIO
        .attempt {
          jinjava.render(template, context.asJava)
        }
        .mapError(e =>
          YamlRuntimeError.TemplateError(
            template,
            s"Failed to render: ${e.getMessage}"
          )
        )

    override def evaluateCondition(
      condition: String,
      context: Map[String, Any]
    ): IO[Throwable, Boolean] =
      ZIO
        .attempt {
          // Wrap condition in {{ }} for evaluation
          val result = jinjava.render(s"{{ $condition }}", context.asJava)
          // Parse result as boolean
          result.toLowerCase match
            case "true"  => true
            case "false" => false
            case _       => result.nonEmpty // Non-empty strings are truthy
        }
        .mapError(e =>
          YamlRuntimeError.TemplateError(
            condition,
            s"Failed to evaluate: ${e.getMessage}"
          )
        )

  object Live:

    /**
     * ZLayer for TemplateEngine.
     *
     * Creates a Jinjava instance with default configuration.
     */
    val layer: ULayer[TemplateEngine] =
      ZLayer.succeed {
        val jinjava = new Jinjava()
        Live(jinjava)
      }
