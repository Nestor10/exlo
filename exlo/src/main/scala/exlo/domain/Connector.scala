package exlo.domain

import zio.stream.ZStream

/**
 * Represents a fully configured connector with metadata and extraction logic.
 *
 * This trait solves the "phase mismatch" problem where metadata needs to be
 * loaded effectfully (file I/O, HTTP) but was previously required as strict vals.
 *
 * The connector is built inside a ZIO effect, allowing metadata to be loaded
 * from any source (files, config, HTTP endpoints) before the connector runs.
 *
 * Example:
 * {{{
 * val loadConnector: ZIO[Any, Throwable, Connector] =
 *   for
 *     spec <- loadYamlSpec("connector.yaml")
 *   yield new Connector:
 *     def id = "my-connector"
 *     def version = spec.version  // Loaded from spec
 *     def extract(state: String) = ZStream(...)
 * }}}
 */
trait Connector:

  /**
   * Unique identifier for this connector.
   *
   * Examples: "shopify-orders", "stripe-charges", "yaml-connector"
   */
  def id: String

  /**
   * Semantic version of connector logic.
   *
   * Used for reproducibility and debugging. Should follow semver.
   * Examples: "1.0.0", "2.1.0"
   */
  def version: String

  /**
   * Extract data from the source.
   *
   * @param state
   *   JSON string with extraction state (cursor, page, timestamp)
   * @return
   *   Stream of data records and checkpoints
   */
  type Env
  def extract(state: String): ZStream[Env, Throwable, StreamElement]

  /**
   * Environment layer providing dependencies for extraction.
   *
   * Framework layers (StorageConfig, IcebergCatalog, Table) are added automatically.
   */
  def environment: zio.ZLayer[Any, Any, Env]
