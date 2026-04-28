package exlo.runtime.iceberg

import exlo.domain.ExloError
import org.apache.iceberg.*
import org.apache.iceberg.catalog.{Catalog, Namespace, TableIdentifier}
import org.apache.iceberg.data.GenericRecord
import org.apache.iceberg.data.parquet.{GenericParquetReaders, GenericParquetWriter}
import org.apache.iceberg.expressions.Expressions
import org.apache.iceberg.parquet.Parquet
import org.apache.iceberg.types.Types
import zio.*

import java.time.{Instant, OffsetDateTime, ZoneOffset}
import java.util.UUID
import scala.jdk.CollectionConverters.*

/**
 * A single committed state checkpoint written to the sidecar table.
 *
 * @param syncId              ULID — lexicographically sortable by time, doubles as tiebreaker.
 * @param connector           Connector id (e.g. `zendesk`, `pokeapi`).
 * @param stream              Stream name within the connector (e.g. `tickets`).
 * @param stateVersion        Monotonic counter; currently always 0 (reserved for future use).
 * @param connectorConfigHash Hash of the connector config at commit time (reserved for future use).
 * @param streamConfigHash    Hash of the stream config at commit time (reserved for future use).
 * @param state               JSON-encoded connector state payload.
 * @param committedAt         Wall-clock time of the commit.
 */
final case class StateRow(
    syncId: String,
    connector: String,
    stream: String,
    stateVersion: Long,
    connectorConfigHash: String,
    streamConfigHash: String,
    state: String,
    committedAt: Instant
)

/**
 * Append-only sidecar state table for connector checkpointing.
 *
 * State is stored in a dedicated Iceberg table per connector:
 * `exlo_state.<connector>`, partitioned by `stream`.
 *
 * Semantics are **at-least-once**: a crash between the data-table commit
 * and the state append will cause the next run to re-read from the older
 * state, producing duplicate records in the data table. Duplicates are
 * a downstream concern.
 *
 * Manual correction: write a newer row via `append` with a later
 * `committedAt` to override the stale checkpoint. Table operators can
 * prune old rows with:
 * {{{
 *   DELETE FROM exlo_state.<connector> WHERE committed_at < '<cutoff>'
 * }}}
 */
trait StateStore:
  def readLatest(connector: String, stream: String): IO[ExloError, Option[StateRow]]
  def append(row: StateRow): IO[ExloError, Unit]

object StateStore:

  // ---- Accessor helpers -------------------------------------------------------------------

  def readLatest(connector: String, stream: String): ZIO[StateStore, ExloError, Option[StateRow]] =
    ZIO.serviceWithZIO[StateStore](_.readLatest(connector, stream))

  def append(row: StateRow): ZIO[StateStore, ExloError, Unit] =
    ZIO.serviceWithZIO[StateStore](_.append(row))

  // ---- Schema + partition spec ------------------------------------------------------------

  /**
   * Sidecar table schema. `connector` is NOT a column because the table is named after the
   * connector. `stream` is both a column and the partition key.
   */
  val schema: Schema = new Schema(
    Types.NestedField.required(1, "sync_id",               Types.StringType.get()),
    Types.NestedField.required(2, "stream",                Types.StringType.get()),
    Types.NestedField.required(3, "state_version",         Types.LongType.get()),
    Types.NestedField.required(4, "connector_config_hash", Types.StringType.get()),
    Types.NestedField.required(5, "stream_config_hash",    Types.StringType.get()),
    Types.NestedField.required(6, "state",                 Types.StringType.get()),
    Types.NestedField.required(7, "committed_at",          Types.TimestampType.withZone())
  )

  val partitionSpec: PartitionSpec =
    PartitionSpec.builderFor(schema).identity("stream").build()

  // ---- Live -------------------------------------------------------------------------------

  /**
   * Live implementation backed by a real Iceberg catalog.
   *
   * Sidecar tables are auto-created on first `append` (namespace `exlo_state` is also
   * auto-created if absent). The `readLatest` method never writes; if no table exists it
   * returns `None` immediately — the bootstrap migration is the caller's responsibility.
   */
  final class Live(catalog: Catalog) extends StateStore:

    private val stateNamespace = "exlo_state"

    def readLatest(connector: String, stream: String): IO[ExloError, Option[StateRow]] =
      ZIO
        .attemptBlocking {
          val identifier = tableId(connector)
          if !catalog.tableExists(identifier) then None
          else
            val table = catalog.loadTable(identifier)
            table.refresh()
            scanLatest(table, connector, stream)
        }
        .mapError(t =>
          ExloError.StorageError(s"StateStore.readLatest failed for $connector/$stream", t)
        )

    def append(row: StateRow): IO[ExloError, Unit] =
      ZIO
        .attemptBlocking {
          val table    = ensureTable(row.connector)
          val dataFile = writeRecord(table, row)
          val txn      = table.newAppend()
          txn.appendFile(dataFile)
          txn.commit()
        }
        .mapError(t =>
          ExloError.StorageError(s"StateStore.append failed for ${row.connector}/${row.stream}", t)
        )

    private def tableId(connector: String): TableIdentifier =
      TableIdentifier.of(Namespace.of(stateNamespace), connector)

    private def ensureTable(connector: String): Table =
      val ns         = Namespace.of(stateNamespace)
      val identifier = tableId(connector)
      catalog match
        case sn: org.apache.iceberg.catalog.SupportsNamespaces =>
          if !sn.namespaceExists(ns) then
            try sn.createNamespace(ns)
            catch
              case _: org.apache.iceberg.exceptions.AlreadyExistsException => ()
        case _ => ()
      if catalog.tableExists(identifier) then catalog.loadTable(identifier)
      else catalog.createTable(identifier, schema, partitionSpec)

    private def scanLatest(table: Table, connector: String, stream: String): Option[StateRow] =
      val filter = Expressions.equal("stream", stream)
      val tasks  = table.newScan().filter(filter).planFiles()
      try
        tasks.iterator().asScala
          .flatMap { task =>
            val inputFile = table.io().newInputFile(task.file().location())
            val reader = Parquet
              .read(inputFile)
              .project(table.schema())
              .createReaderFunc((fileSchema: org.apache.parquet.schema.MessageType) =>
                GenericParquetReaders.buildReader(table.schema(), fileSchema)
              )
              .filter(task.residual())
              .split(task.start(), task.length())
              .build()
            try reader.iterator().asScala.map(r => rowFromRecord(r.asInstanceOf[GenericRecord], connector)).toList
            finally reader.close()
          }
          .maxByOption(r => (r.committedAt.toEpochMilli, r.syncId))
      finally tasks.close()

    private def writeRecord(table: Table, row: StateRow): DataFile =
      val outFile = table
        .io()
        .newOutputFile(
          table.locationProvider().newDataLocation(s"state-${UUID.randomUUID()}.parquet")
        )

      // For a partitioned table the DataWriter must know which partition it's writing to.
      val partitionRecord = GenericRecord.create(table.spec().partitionType())
      partitionRecord.set(0, row.stream)

      val writer = Parquet
        .writeData(outFile)
        .schema(table.schema())
        .createWriterFunc((msg: org.apache.parquet.schema.MessageType) =>
          GenericParquetWriter.create(table.schema(), msg)
        )
        .overwrite()
        .withSpec(table.spec())
        .withPartition(partitionRecord)
        .build()

      try
        val gr = GenericRecord.create(table.schema())
        gr.setField("sync_id",               row.syncId)
        gr.setField("stream",                row.stream)
        gr.setField("state_version",         row.stateVersion)
        gr.setField("connector_config_hash", row.connectorConfigHash)
        gr.setField("stream_config_hash",    row.streamConfigHash)
        gr.setField("state",                 row.state)
        gr.setField("committed_at",          OffsetDateTime.ofInstant(row.committedAt, ZoneOffset.UTC))
        writer.write(gr)
      finally writer.close()

      writer.toDataFile()

    private def rowFromRecord(r: GenericRecord, connector: String): StateRow =
      StateRow(
        syncId              = r.getField("sync_id").asInstanceOf[String],
        connector           = connector,
        stream              = r.getField("stream").asInstanceOf[String],
        stateVersion        = r.getField("state_version").asInstanceOf[Long],
        connectorConfigHash = r.getField("connector_config_hash").asInstanceOf[String],
        streamConfigHash    = r.getField("stream_config_hash").asInstanceOf[String],
        state               = r.getField("state").asInstanceOf[String],
        committedAt         = r.getField("committed_at").asInstanceOf[OffsetDateTime].toInstant
      )

  /** ZLayer backed by a real Iceberg catalog. */
  val live: ZLayer[Catalog, Nothing, StateStore] =
    ZLayer.fromFunction(catalog => new Live(catalog))

  // ---- InMemory (test) --------------------------------------------------------------------

  /**
   * In-memory `StateStore` for tests. Stores rows in a plain `Ref`; no Iceberg I/O.
   * Use [[InMemory.make]] to construct and [[InMemory.layer]] as a `ZLayer`.
   */
  final class InMemory(ref: Ref[Map[(String, String), List[StateRow]]]) extends StateStore:

    def readLatest(connector: String, stream: String): IO[ExloError, Option[StateRow]] =
      ref.get.map(
        _.getOrElse((connector, stream), Nil)
          .maxByOption(r => (r.committedAt.toEpochMilli, r.syncId))
      )

    def append(row: StateRow): IO[ExloError, Unit] =
      ref.update { m =>
        val key = (row.connector, row.stream)
        m + (key -> (m.getOrElse(key, Nil) :+ row))
      }

    /** All rows ever appended (for test assertions). */
    def allRows: UIO[List[StateRow]] = ref.get.map(_.values.flatten.toList)

  object InMemory:
    def make: UIO[InMemory] =
      Ref.make(Map.empty[(String, String), List[StateRow]]).map(new InMemory(_))

    val layer: ULayer[StateStore] = ZLayer.fromZIO(make)

// ---- ULID utility -----------------------------------------------------------------------

/**
 * Minimal ULID generator (no external dependency).
 *
 * A ULID is a 26-character Crockford Base32 string:
 *   - 10 chars for a 48-bit millisecond timestamp (sortable)
 *   - 16 chars for 80 bits of cryptographic random
 *
 * Lexicographic ordering matches chronological ordering for the same process; ties within
 * the same millisecond break randomly (which is an acceptable tiebreaker for our read path
 * because `maxByOption` on `(committedAt.toEpochMilli, syncId)` already orders by wall
 * clock first).
 */
private[iceberg] object Ulid:

  private val alphabet = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
  private val rnd      = new java.security.SecureRandom()

  def generate(): String =
    val ts    = java.lang.System.currentTimeMillis()
    val rand  = new Array[Byte](10)
    rnd.nextBytes(rand)
    val chars = new Array[Char](26)

    // 10 chars for 48-bit timestamp (most-significant first, 5 bits per char)
    var t = ts
    for i <- (0 until 10).reverse do
      chars(i) = alphabet((t & 0x1f).toInt)
      t >>>= 5

    // 16 chars for 80-bit random: pack 10 bytes into two 40-bit halves
    var hi = 0L
    for i <- 0 until 5 do hi = (hi << 8) | (rand(i).toLong & 0xff)
    for i <- (0 until 8).reverse do
      chars(10 + i) = alphabet((hi & 0x1f).toInt)
      hi >>>= 5

    var lo = 0L
    for i <- 5 until 10 do lo = (lo << 8) | (rand(i).toLong & 0xff)
    for i <- (0 until 8).reverse do
      chars(18 + i) = alphabet((lo & 0x1f).toInt)
      lo >>>= 5

    new String(chars)
