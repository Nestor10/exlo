package exlo.yaml.util

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper

import scala.jdk.CollectionConverters.*

/**
 * Utilities for working with Jackson JsonNode.
 */
object JsonUtils:

  private val mapper = new ObjectMapper()

  /**
   * Convert JsonNode to Object for template engines.
   *
   * Jinjava cannot navigate JsonNode properties directly. This converts a
   * JsonNode into Java collections (Map/List) that Jinjava can traverse.
   *
   * Returns Java Object (will be Map, List, String, Number, or Boolean depending
   * on the JsonNode content). Do NOT convert to Scala collections - Jinjava
   * needs native Java types.
   *
   * @param node
   *   JsonNode to convert
   * @return
   *   Java Object (Map/List/String/Number/Boolean)
   */
  def toAny(node: JsonNode): Object =
    mapper.convertValue(node, classOf[Object])
