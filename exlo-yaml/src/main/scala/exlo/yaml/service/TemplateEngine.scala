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
   * Render Jinja2 template with context from RuntimeContext.
   *
   * Context includes config, state, and pagination variables from RuntimeContext.
   *
   * @param template
   *   Jinja2 template string
   * @return
   *   Rendered string
   */
  def render(
    template: String
  ): ZIO[RuntimeContext, Throwable, String]

  /**
   * Evaluate Jinja2 condition expression with context from RuntimeContext.
   *
   * @param condition
   *   Boolean expression (without {{ }})
   * @return
   *   Boolean result
   */
  def evaluateCondition(
    condition: String
  ): ZIO[RuntimeContext, Throwable, Boolean]

object TemplateEngine:

  /**
   * Accessor for render.
   *
   * Use: `TemplateEngine.render(template)`
   */
  def render(
    template: String
  ): ZIO[TemplateEngine & RuntimeContext, Throwable, String] =
    ZIO.serviceWithZIO[TemplateEngine](_.render(template))

  /**
   * Accessor for evaluateCondition.
   *
   * Use: `TemplateEngine.evaluateCondition(condition)`
   */
  def evaluateCondition(
    condition: String
  ): ZIO[TemplateEngine & RuntimeContext, Throwable, Boolean] =
    ZIO.serviceWithZIO[TemplateEngine](_.evaluateCondition(condition))

  /** Live implementation using Jinjava. */
  case class Live(jinjava: Jinjava) extends TemplateEngine:

    override def render(
      template: String
    ): ZIO[RuntimeContext, Throwable, String] =
      for
        context <- RuntimeContext.getTemplateContext
        result  <- ZIO
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
      yield result

    override def evaluateCondition(
      condition: String
    ): ZIO[RuntimeContext, Throwable, Boolean] =
      for
        context <- RuntimeContext.getTemplateContext
        result  <- ZIO
          .attempt {
            val javaContext = TemplateValue.contextToJava(context)
            // Wrap condition in {{ }} for evaluation
            val evalResult  = jinjava.render(s"{{ $condition }}", javaContext)
            // Parse result as boolean
            evalResult.toLowerCase match
              case "true"  => true
              case "false" => false
              case _       => evalResult.nonEmpty // Non-empty strings are truthy
          }
          .mapError(e =>
            YamlRuntimeError.TemplateError(
              condition,
              s"Failed to evaluate: ${e.getMessage}"
            )
          )
      yield result

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
