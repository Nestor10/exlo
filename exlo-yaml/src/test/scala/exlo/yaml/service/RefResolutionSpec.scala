package exlo.yaml.service

import exlo.yaml.spec.*
import zio.*
import zio.test.*
import zio.test.Assertion.*

/**
 * Tests for $ref resolution in YAML connector specifications.
 *
 * Verifies that JSON pointer references are resolved correctly before
 * parsing into ConnectorSpec ADTs.
 */
object RefResolutionSpec extends ZIOSpecDefault:

  def spec = suite("RefResolutionSpec")(
    test("resolve simple $ref in requester") {
      val yaml =
        """
          |version: "1.0.0"
          |
          |definitions:
          |  base_requester:
          |    url: "https://api.example.com/users"
          |    method: GET
          |    headers: {}
          |    params: {}
          |    auth:
          |      type: Bearer
          |      token: "secret-token"
          |
          |streams:
          |  - name: "users"
          |    requester:
          |      $ref: "#/definitions/base_requester"
          |    recordSelector:
          |      extractor:
          |        type: DpathExtractor
          |        field_path: ["data"]
          |      filter: null
          |    paginator:
          |      type: NoPagination
          |""".stripMargin

      for spec <- YamlSpecLoader.parseYamlString(yaml)
      yield assertTrue(
        spec.streams.length == 1,
        spec.streams.head.name == "users"
      )
    },
    test("resolve nested $ref references") {
      val yaml =
        """
          |version: "1.0.0"
          |
          |definitions:
          |  base_auth:
          |    type: Bearer
          |    token: "my-secret-token"
          |  
          |  base_requester:
          |    url: "https://api.example.com/posts"
          |    method: GET
          |    headers: {}
          |    params: {}
          |    auth:
          |      $ref: "#/definitions/base_auth"
          |
          |streams:
          |  - name: "posts"
          |    requester:
          |      $ref: "#/definitions/base_requester"
          |    recordSelector:
          |      extractor:
          |        type: DpathExtractor
          |        field_path: ["items"]
          |      filter: null
          |    paginator:
          |      type: NoPagination
          |""".stripMargin

      for spec <- YamlSpecLoader.parseYamlString(yaml)
      yield assertTrue(
        spec.streams.length == 1,
        spec.streams.head.name == "posts"
      )
    },
    test("resolve $ref in paginator") {
      val yaml =
        """
          |version: "1.0.0"
          |
          |definitions:
          |  cursor_paginator:
          |    type: CursorPagination
          |    cursorValue: "{{ response.next_page }}"
          |    stopCondition: "{{ not response.has_more }}"
          |
          |streams:
          |  - name: "items"
          |    requester:
          |      url: "https://api.example.com/items"
          |      method: GET
          |      headers: {}
          |      params: {}
          |      auth:
          |        type: NoAuth
          |    recordSelector:
          |      extractor:
          |        type: DpathExtractor
          |        field_path: ["data"]
          |      filter: null
          |    paginator:
          |      $ref: "#/definitions/cursor_paginator"
          |""".stripMargin

      for spec <- YamlSpecLoader.parseYamlString(yaml)
      yield assertTrue(
        spec.streams.length == 1,
        spec.streams.head.name == "items"
      )
    },
    test("resolve $ref in auth") {
      val yaml =
        """
          |version: "1.0.0"
          |
          |definitions:
          |  api_key_auth:
          |    type: ApiKey
          |    header: "X-API-Key"
          |    token: "test-key-123"
          |
          |streams:
          |  - name: "records"
          |    requester:
          |      url: "https://api.example.com/records"
          |      method: GET
          |      headers: {}
          |      params: {}
          |      auth:
          |        $ref: "#/definitions/api_key_auth"
          |    recordSelector:
          |      extractor:
          |        type: DpathExtractor
          |        field_path: ["results"]
          |      filter: null
          |    paginator:
          |      type: NoPagination
          |""".stripMargin

      for spec <- YamlSpecLoader.parseYamlString(yaml)
      yield assertTrue(
        spec.streams.length == 1,
        spec.streams.head.name == "records"
      )
    }
  )
