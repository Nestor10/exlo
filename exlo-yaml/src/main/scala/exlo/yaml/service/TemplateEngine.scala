package exlo.yaml.service

import com.hubspot.jinjava.Jinjava
import exlo.yaml.domain.YamlRuntimeError
import exlo.yaml.template.TemplateValue
import zio.*

/**
 * Service for Jinja2 template rendering and evaluation.
 *
 * Provides Jinja2 compatibility for Airbyte-style YAML specs using Jinjava
 * (HubSpot's JVM implementation).
 *
 * Type Safety:
 * Context values use the TemplateValue ADT which provides:
 * - Compile-time type safety for all template values
 * - Exhaustive pattern matching on supported types
 * - Automatic conversion to Java types required by Jinjava
 *
 * The ADT supports:
 * - TemplateValue.Str(String)
 * - TemplateValue.Num(Int)
 * - TemplateValue.Bool(Boolean)
 * - TemplateValue.Obj(Map[String, TemplateValue])
 * - TemplateValue.Arr(List[TemplateValue])
 * - TemplateValue.Null
 */
trait TemplateEngine:

  /**
   * Render Jinja2 template with context.
   *
   * @param template
   *   Jinja2 template string
   * @param context
   *   Variables available to template using TemplateValue ADT
   * @return
   *   Rendered string
   */
  def render(
    template: String,
    context: Map[String, TemplateValue]
  ): IO[Throwable, String]

  /**
   * Evaluate Jinja2 condition expression.
   *
   * @param condition
   *   Boolean expression (without {{ }})
   * @param context
   *   Variables available to expression using TemplateValue ADT
   * @return
   *   Boolean result
   */
  def evaluateCondition(
    condition: String,
    context: Map[String, TemplateValue]
  ): IO[Throwable, Boolean]

object TemplateEngine:

  /**
   * Accessor for render.
   *
   * Use: `TemplateEngine.render(template, context)`
   */
  def render(
    template: String,
    context: Map[String, TemplateValue]
  ): ZIO[TemplateEngine, Throwable, String] =
    ZIO.serviceWithZIO[TemplateEngine](_.render(template, context))

  /**
   * Accessor for evaluateCondition.
   *
   * Use: `TemplateEngine.evaluateCondition(condition, context)`
   */
  def evaluateCondition(
    condition: String,
    context: Map[String, TemplateValue]
  ): ZIO[TemplateEngine, Throwable, Boolean] =
    ZIO.serviceWithZIO[TemplateEngine](_.evaluateCondition(condition, context))

  /** Live implementation using Jinjava. */
  case class Live(jinjava: Jinjava) extends TemplateEngine:

    override def render(
      template: String,
      context: Map[String, TemplateValue]
    ): IO[Throwable, String] =
      ZIO
        .attempt {
          val javaContext = TemplateValue.contextToJava(context)
          jinjava.render(template, javaContext)
        }
        .mapError(e =>
          YamlRuntimeError.TemplateError(
            template,
            s"Failed to render: ${e.getMessage}"
          )
        )

    override def evaluateCondition(
      condition: String,
      context: Map[String, TemplateValue]
    ): IO[Throwable, Boolean] =
      ZIO
        .attempt {
          val javaContext = TemplateValue.contextToJava(context)
          // Wrap condition in {{ }} for evaluation
          val result      = jinjava.render(s"{{ $condition }}", javaContext)
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
