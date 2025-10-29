package exlo.domain

import java.time.Instant
import java.util.UUID

/**
 * Metadata about a sync execution.
 *
 * @param syncId
 *   Unique identifier for this sync run
 * @param startedAt
 *   When the sync started
 * @param connectorVersion
 *   Version of the connector being executed
 */
case class SyncMetadata(
  syncId: UUID,
  startedAt: Instant,
  connectorVersion: String
)
