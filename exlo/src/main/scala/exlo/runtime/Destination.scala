package exlo.runtime

import exlo.domain.ExloError
import zio.*

/**
 * Unified data + state destination. Replaces the earlier `DataStore` / `StateStore` split.
 *
 * Why a single trait: with atomic commits (Iceberg's snapshot-properties model), records
 * and state become durable in the same transaction. Splitting them at the API level fights
 * the substrate. Backends that don't support atomic commits (JSONL+gzip with state sidecar)
 * implement `commit` as ordered writes (data first, then state) and accept the at-least-once
 * trade — but the contract presented to the rest of the framework is the same.
 *
 * Lifecycle:
 *   1. The framework calls [[readState]] once at run start to load the resume point.
 *   2. As records arrive, the sink fiber calls [[writeRecords]] continuously to keep memory
 *      bounded — for atomic backends like Iceberg, this stages a Parquet `DataFile` against
 *      the table without committing it.
 *   3. When the sink's flush threshold trips (record count or interval), the sink calls
 *      [[commit]] with the current state value. The implementation commits all
 *      writeRecords-staged data + the state as one atomic transaction (where supported).
 */
trait Destination[S]:

  /**
   * Stage records for the next commit. Atomic backends should write Parquet/file artifacts
   * here without making them visible (so memory doesn't accumulate); ordered backends may
   * append to a file. Either way, the records are NOT durable until [[commit]] succeeds.
   */
  def writeRecords(records: Chunk[String]): IO[ExloError, Unit]

  /**
   * Commit all records staged since the last commit + the state value as one transaction
   * (atomic where the backend supports it; ordered-write at-least-once otherwise).
   */
  def commit(state: S): IO[ExloError, Unit]

  /** Latest committed state, or None on cold start. */
  def readState: IO[ExloError, Option[S]]

object Destination:

  /**
   * Test impl: buffers writeRecords in memory; commit moves the buffer into the snapshot
   * list along with the state value. Each commit is one snapshot — useful for asserting
   * commit boundaries in tests.
   */
  final class InMemory[S] private (
      pending: Ref[Chunk[String]],
      snapshotsRef: Ref[Chunk[InMemory.Snapshot[S]]]
  ) extends Destination[S]:

    def writeRecords(records: Chunk[String]): IO[ExloError, Unit] =
      pending.update(_ ++ records)

    def commit(state: S): IO[ExloError, Unit] =
      pending.getAndSet(Chunk.empty).flatMap { records =>
        snapshotsRef.update(_ :+ InMemory.Snapshot(records, state))
      }

    def readState: IO[ExloError, Option[S]] =
      snapshotsRef.get.map(_.lastOption.map(_.state))

    /** All snapshots in commit order. */
    def snapshots: UIO[Chunk[InMemory.Snapshot[S]]] = snapshotsRef.get

    /** All records ever committed, in order. */
    def allRecords: UIO[Chunk[String]] = snapshotsRef.get.map(_.flatMap(_.records))

    /** Number of commits. */
    def commitCount: UIO[Int] = snapshotsRef.get.map(_.length)

  object InMemory:

    final case class Snapshot[S](records: Chunk[String], state: S)

    def make[S]: UIO[InMemory[S]] =
      for
        pending   <- Ref.make(Chunk.empty[String])
        snapshots <- Ref.make(Chunk.empty[Snapshot[S]])
      yield new InMemory[S](pending, snapshots)

    /** Pre-seed the snapshot list, simulating a prior run's commit. Used to test resume. */
    def seeded[S](initialSnapshots: Chunk[Snapshot[S]]): UIO[InMemory[S]] =
      for
        pending   <- Ref.make(Chunk.empty[String])
        snapshots <- Ref.make(initialSnapshots)
      yield new InMemory[S](pending, snapshots)

    def layer[S: Tag]: ULayer[Destination[S] & InMemory[S]] =
      ZLayer.fromZIOEnvironment {
        make[S].map(impl => ZEnvironment[Destination[S]](impl).add[InMemory[S]](impl))
      }
