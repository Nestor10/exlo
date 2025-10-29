package exlo.domain

import zio.*

/**
 * Configuration for state version management.
 *
 * Incrementing the version triggers a fresh start - framework ignores previous
 * state and passes empty string to connector.
 *
 * Use cases:
 * - User makes breaking changes to connector logic
 * - Source system had issues and needs full backfill
 * - Testing/development iterations
 *
 * @param version
 *   State version number. Increment by 1 to reset state.
 */
case class StateConfig(version: Long)

object StateConfig:

  /**
   * Layer for loading StateConfig from configuration.
   *
   * TODO: Implement with zio-config in Phase 3
   */
  val layer: ZLayer[Any, Throwable, StateConfig] =
    ZLayer.succeed(StateConfig(version = 1L))
