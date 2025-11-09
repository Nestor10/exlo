package exlo.yaml.template

import com.fasterxml.jackson.databind.JsonNode
import exlo.yaml.util.JsonUtils

import scala.jdk.CollectionConverters.*

/**
 * Represents values that can be used in template contexts.
 *
 * This ADT provides type-safe representation of all values that can appear in
 * Jinja2 template contexts. The sealed trait ensures exhaustive pattern
 * matching and compiler-enforced correctness.
 *
 * @see
 *   TemplateEngine for usage in template rendering
 */
sealed trait TemplateValue

object TemplateValue:

  /** String value */
  case class Str(value: String) extends TemplateValue

  /** Numeric value (integers) */
  case class Num(value: Int) extends TemplateValue

  /** Boolean value */
  case class Bool(value: Boolean) extends TemplateValue

  /** Object/Map value with nested TemplateValues */
  case class Obj(value: Map[String, Option[TemplateValue]]) extends TemplateValue

  /** Array/List value with nested TemplateValues */
  case class Arr(value: List[Option[TemplateValue]]) extends TemplateValue

  /**
   * Convert TemplateValue to Java Object for Jinjava compatibility.
   *
   * Jinjava requires Java types (java.util.Map, java.util.List, primitives)
   * for template rendering. This method recursively transforms the
   * TemplateValue ADT into the appropriate Java types.
   *
   * @param value
   *   The TemplateValue to convert (wrapped in Option)
   * @return
   *   Java Object compatible with Jinjava (null for None)
   */
  def toJava(value: Option[TemplateValue]): Object = value match
    case None            => null
    case Some(Str(s))    => s
    case Some(Num(n))    => java.lang.Integer.valueOf(n)
    case Some(Bool(b))   => java.lang.Boolean.valueOf(b)
    case Some(Obj(map))  =>
      map.view.mapValues(toJava).toMap.asJava
    case Some(Arr(list)) =>
      list.map(toJava).asJava

  /**
   * Convert a Map of TemplateValues to a Java Map.
   *
   * This is the primary conversion method used when passing context to
   * TemplateEngine. It transforms the entire context map into Java types
   * suitable for Jinjava.
   *
   * @param context
   *   Map of template variable names to TemplateValues
   * @return
   *   Java Map compatible with Jinjava
   */
  def contextToJava(
    context: Map[String, TemplateValue]
  ): java.util.Map[String, Object] =
    context.view.mapValues(v => toJava(Some(v))).toMap.asJava

  /**
   * Convert Jackson JsonNode to TemplateValue.
   *
   * This method handles the conversion from parsed JSON (Jackson's JsonNode)
   * into our type-safe TemplateValue ADT. It recursively processes nested
   * structures.
   *
   * Note: Uses JsonUtils.toAny internally which converts JsonNode to Java
   * types, then wraps in appropriate TemplateValue constructors.
   *
   * @param node
   *   Jackson JsonNode to convert
   * @return
   *   TemplateValue representing the JSON structure
   * @throws RuntimeException
   *   if the node represents a null value (caller should handle with Option)
   */
  def fromJsonNode(node: JsonNode): TemplateValue =
    val javaValue = JsonUtils.toAny(node)
    fromJavaObject(javaValue).getOrElse(
      throw new RuntimeException("JsonNode converted to null - use Option[TemplateValue] for nullable values")
    )

  /**
   * Convert Java Object (from JsonNode conversion) to TemplateValue.
   *
   * This handles the conversion from Jackson's Java types back into our ADT.
   * Used internally by fromJsonNode.
   *
   * @param obj
   *   Java Object (Map, List, String, Number, Boolean, or null)
   * @return
   *   Option[TemplateValue] wrapping the Java object (None for null)
   */
  private def fromJavaObject(obj: Object): Option[TemplateValue] = obj match
    case null                   => None
    case s: String              => Some(Str(s))
    case n: Number              => Some(Num(n.intValue()))
    case b: java.lang.Boolean   => Some(Bool(b.booleanValue()))
    case m: java.util.Map[?, ?] =>
      val scalaMap = m.asScala.toMap.asInstanceOf[Map[String, Object]]
      Some(Obj(scalaMap.view.mapValues(fromJavaObject).toMap))
    case l: java.util.List[?]   =>
      val scalaList = l.asScala.toList.asInstanceOf[List[Object]]
      Some(Arr(scalaList.map(fromJavaObject)))
    case other                  =>
      // Fallback for unexpected types - convert to string
      Some(Str(other.toString))
