package exlo.s3

import exlo.domain.ExloError
import exlo.runtime.{DataSink, RunContext, Sequenced}
import software.amazon.awssdk.core.async.AsyncRequestBody
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import zio.*
import zio.json.*
import zio.json.ast.Json

import java.io.{BufferedWriter, FileOutputStream, OutputStreamWriter}
import java.nio.file.{Files, Path}
import java.util.UUID
import java.util.zip.GZIPOutputStream

/**
 * S3-backed [[DataSink]]. Each `write` produces one JSONL+gzip object in S3,
 * named with a monotonic flush sequence number. Records are wrapped in the
 * exlo envelope:
 *
 *   {
 *     "_exlo_ab_id":     "<uuidv4>",
 *     "_exlo_emitted_at": <epoch millis>,
 *     "_exlo_data":       <record JSON>
 *   }
 *
 * Path layout:
 *   s3://{bucket}/{prefix}/data/connector={id}/stream={s}/sync_id={syncId}/part-NN.jsonl.gz
 *
 * Lifecycle: per `write` we acquire a tempfile + GZIP writer (scoped),
 * stream-write the batch, upload the tempfile via PutObject, and the scope
 * closes (deleting the tempfile). Per zionomicon ch.14/15: scoped resources
 * + acquireRelease guarantee cleanup even on interrupt.
 */
final class S3DataSink private (
    s3:       S3AsyncClient,
    config:   S3Config,
    flushSeq: Ref[Long]
) extends DataSink:

  /**
   * `connectorId`, `streamName`, and `syncId` are read from [[RunContext]]
   * FiberRefs at write time. The framework's entry point ([[exlo.Exlo.run]])
   * sets them via `RunContext.withRun` for the duration of the run; tests
   * must set them explicitly via `RunContext.withRun` and
   * `RunContext.streamName.locally` before calling `write`.
   */
  def write(batch: Chunk[Sequenced]): IO[ExloError, Long] =
    if batch.isEmpty then ZIO.succeed(0L)
    else
      for
        syncId      <- RunContext.syncId.get
        connectorId <- RunContext.connectorId.get
        streamName  <- RunContext.streamName.get
        n           <- flushSeq.updateAndGet(_ + 1L)
        result      <- ZIO.scoped {
                         for
                           tempFile <- acquireTempFile
                           _        <- writeBatch(tempFile, batch)
                           _        <- uploadFile(tempFile, pathFor(connectorId, streamName, syncId, n))
                         yield batch.map(_.seq).max
                       }
      yield result

  private def acquireTempFile: ZIO[Scope, ExloError, Path] =
    ZIO.acquireRelease(
      ZIO.attempt(Files.createTempFile("exlo-", ".jsonl.gz")).mapError(wrap)
    )(p => ZIO.attempt(Files.deleteIfExists(p)).ignoreLogged)

  private def writeBatch(path: Path, batch: Chunk[Sequenced]): IO[ExloError, Unit] =
    ZIO.attemptBlocking {
      val gz = new GZIPOutputStream(new FileOutputStream(path.toFile))
      val w  = new BufferedWriter(new OutputStreamWriter(gz, "UTF-8"))
      try
        batch.foreach { s =>
          w.write(envelope(s.value))
          w.newLine()
        }
        w.flush()
      finally
        w.close()
    }.mapError(wrap)

  private def envelope(rec: String): String =
    val data = rec.fromJson[Json].getOrElse(Json.Str(rec))
    Json.Obj(
      "_exlo_ab_id"      -> Json.Str(UUID.randomUUID.toString),
      "_exlo_emitted_at" -> Json.Num(java.lang.System.currentTimeMillis()),
      "_exlo_data"       -> data
    ).toJson

  private def uploadFile(path: Path, key: String): IO[ExloError, Unit] =
    ZIO.fromCompletableFuture(
      s3.putObject(
        PutObjectRequest.builder().bucket(config.bucket).key(key).build(),
        AsyncRequestBody.fromFile(path.toFile)
      )
    ).mapError(wrap).unit

  private def pathFor(connectorId: String, streamName: String, syncId: String, flushSeq: Long): String =
    f"${config.prefix.stripSuffix("/")}/data/connector=$connectorId/stream=$streamName/sync_id=$syncId/part-$flushSeq%08d.jsonl.gz"

  private def wrap(t: Throwable): ExloError =
    ExloError.StorageError(s"S3DataSink: ${t.getMessage}", t)


object S3DataSink:

  def make(config: S3Config): ZIO[S3AsyncClient, Nothing, S3DataSink] =
    for
      s3       <- ZIO.service[S3AsyncClient]
      flushSeq <- Ref.make(0L)
    yield new S3DataSink(s3, config, flushSeq)

  def layer(config: S3Config): ZLayer[S3AsyncClient, Nothing, DataSink] =
    ZLayer.fromZIO(make(config).map(s => s: DataSink))
