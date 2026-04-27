package exlo.domain

import exlo.runtime.ExloState
import zio.Tag
import zio.stream.ZStream

/**
 * A connector whose work is partitioned into independent `Slice`s. The framework enumerates
 * slices via [[slices]], then runs [[extract]] for each in parallel up to [[parallelism]],
 * with all of them emitting records via the shared `ExloState`. STM serializes concurrent
 * state updates and assigns monotonic record IDs across all slices.
 *
 * The model fits sources where the API supports filtering by a `(start, end)` range — time
 * windows, ID ranges, parent-child fanouts. Compared to cursor pagination (which forces
 * sequential execution), slicing unlocks parallel extraction with no contortion in the
 * connector author's mental model.
 *
 * State `S` is connector-defined. It typically includes a representation of which slices
 * have completed (a `Set[SliceId]`, a range tree, etc.) so [[slices]] can filter out
 * already-done slices on resume.
 *
 * To run a [[SlicedConnector]], call [[toConnector]] to produce a regular [[Connector]] and
 * pass it to [[exlo.Exlo.run]].
 */
trait SlicedConnector[Slice, S, -R, +E <: Throwable]:
  self =>

  def id: String
  def version: String

  /** Number of slices to run concurrently. Default 4; override for I/O-bound or rate-limited sources. */
  def parallelism: Int = 4

  /**
   * Enumerate the slices to run. Receives current state so the connector can skip slices it
   * has already marked done. May be lazy — slices can be discovered as the stream progresses.
   */
  def slices(state: S): ZStream[R, E, Slice]

  /**
   * Extract for one slice. Output (records and state advances) flows through `ExloState`.
   * Multiple slices run concurrently feeding the same shared sink; STM serializes
   * concurrent `ExloState.update` calls so no slice's mutation is lost.
   */
  def extract(slice: Slice): ZStream[R & ExloState[S], E, Unit]

  /**
   * Lift this [[SlicedConnector]] into a regular [[Connector]]. The lifted connector reads
   * current state, enumerates slices, then runs `parallelism` slice streams concurrently
   * via `flatMapPar`.
   */
  def toConnector(using Tag[S]): Connector[S, R, E] = new Connector[S, R, E]:
    def id: String                                = self.id
    def version: String                           = self.version
    def emit: ZStream[R & ExloState[S], E, Unit] =
      ZStream
        .unwrap(ExloState.current[S].map(self.slices))
        .flatMapPar(self.parallelism)(self.extract)
