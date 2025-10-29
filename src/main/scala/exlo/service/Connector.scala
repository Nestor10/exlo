package exlo.service

import exlo.domain.ExloError
import exlo.domain.StreamElement
import zio.*
import zio.stream.*

/**
 * User-facing interface for extracting data from a source.
 *
 * Users implement this trait to define their extraction logic. The framework
 * calls `extract` with the current state and expects a stream of
 * StreamElements (Data and Checkpoints).
 *
 * Example:
 * {{{
 * case class ShopifyConnector(httpClient: HttpClient, config: ShopifyConfig) extends Connector:
 *   def connectorId = "shopify.orders"
 *
 *   def extract(state: String): ZStream[Any, ExloError, StreamElement] =
 *     val shopifyState = ShopifyState.fromJson(state).getOrElse(ShopifyState.empty)
 *     // ... extraction logic ...
 *     ZStream(...)
 * }}}
 */
trait Connector:

  /**
   * Unique identifier for this connector.
   *
   * Used in ExloRecord metadata and for logging/monitoring.
   */
  def connectorId: String

  /**
   * Extract data from source, emitting data records and state checkpoints.
   *
   * @param state
   *   Current state as JSON string. Empty string if first run or state version
   *   changed.
   * @return
   *   Stream of Data (user records) and Checkpoint (state) elements
   */
  def extract(state: String): ZStream[Any, ExloError, StreamElement]

object Connector:

  /**
   * Accessor for connectorId.
   *
   * Use: `Connector.connectorId`
   */
  def connectorId: ZIO[Connector, Nothing, String] =
    ZIO.serviceWith[Connector](_.connectorId)

  /**
   * Accessor for extract.
   *
   * Use: `Connector.extract(state)`
   */
  def extract(state: String): ZStream[Connector, ExloError, StreamElement] =
    ZStream.serviceWithStream[Connector](_.extract(state))
