package exlo.domain

import java.time.Instant
import java.util.UUID

/**
 * Framework's metadata wrapper for user data records written to Iceberg.
 *
 * This is NOT what users emit - users emit simple strings. The framework wraps
 * user strings with this metadata before writing to Iceberg tables.
 *
 * Enables:
 * - Traceability: Track which connector/sync produced each record
 * - Idempotency: Detect duplicate commits via commitId
 * - Lineage: Understand data provenance via metadata fields
 *
 * @param commitId
 *   Unique ID for this commit batch
 * @param connectorId
 *   User's connector identifier (e.g., "shopify.orders")
 * @param syncId
 *   Unique ID for this sync execution
 * @param committedAt
 *   When record was committed to Iceberg
 * @param recordedAt
 *   When record was extracted from source
 * @param connectorVersion
 *   User's connector version
 * @param connectorConfigHash
 *   Hash of connector configuration
 * @param streamConfigHash
 *   Hash of stream configuration
 * @param streamName
 *   Name of the stream this record belongs to (e.g., "orders", "customers")
 * @param stateVersion
 *   State version this record belongs to
 * @param payload
 *   User's record string - never deserialized by framework
 */
case class ExloRecord(
  commitId: UUID,
  connectorId: String,
  syncId: UUID,
  committedAt: Instant,
  recordedAt: Instant,
  connectorVersion: String,
  connectorConfigHash: String,
  streamConfigHash: String,
  streamName: String,
  stateVersion: Long,
  payload: String
)
