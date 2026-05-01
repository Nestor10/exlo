package exlo.s3

import exlo.domain.ExloError
import exlo.runtime.{Filter, StateRow, StateStore}
import software.amazon.awssdk.core.async.{AsyncRequestBody, AsyncResponseTransformer}
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.{
  GetObjectRequest, GetObjectResponse, ListObjectsV2Request,
  NoSuchKeyException, PutObjectRequest, S3Exception, S3Object
}
import zio.*
import zio.json.*
import zio.stream.ZStream

import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import java.time.Instant

/**
 * S3-backed [[StateStore]]. One JSON object per `(connector, stream, key)`:
 *
 *   s3://{bucket}/{prefix}/state/connector={c}/stream={s}/key={url-enc}.json
 *
 * Semantics:
 *   - readByKey: GET object; absent → `None`.
 *   - merge: read existing (with ETag), compare `(committedAt, syncId)`,
 *     PUT only if newer. The PUT uses S3 conditional writes — `If-Match`
 *     against the ETag we read, or `If-None-Match: *` when no object yet
 *     existed. On a 412 PreconditionFailed (someone else wrote first),
 *     the merge re-reads and retries up to 5 times. This makes the merge
 *     truly atomic single-writer-wins under contention; no documented
 *     race window.
 *   - scan(ByKey): one filtered list-and-get.
 *   - scan(Tail(n)): list objects, fetch each, sort by
 *     `(committedAt, syncId)` desc, take n. Acceptable memory-bound for
 *     typical state sizes (state per stream is normally <100 keys).
 */
final class S3StateStore(s3: S3AsyncClient, config: S3Config) extends StateStore:

  import S3StateStore.given

  def readByKey(connector: String, stream: String, key: String):
      IO[ExloError, Option[StateRow]] =
    val k = keyPath(connector, stream, key)
    getObjectAsString(k).flatMap {
      case None      => ZIO.none
      case Some(str) =>
        ZIO.fromEither(str.fromJson[StateRow])
          .mapError(msg => decodeError(k, msg))
          .map(Some(_))
    }

  def merge(row: StateRow): IO[ExloError, Unit] =
    val k = keyPath(row.connector, row.stream, row.key)

    val attempt: IO[ExloError, Unit] = getObjectWithEtag(k).flatMap {
      case None =>
        // No object yet — succeed only if no other writer beats us to it.
        putConditional(k, row.toJson, ifMatch = None, ifNoneMatch = Some("*"))
      case Some((bodyStr, etag)) =>
        ZIO.fromEither(bodyStr.fromJson[StateRow])
          .mapError(decodeError(k, _))
          .flatMap { prev =>
            if isNewer(row, prev) then
              // Succeed only if the object hasn't changed since we read it.
              putConditional(k, row.toJson, ifMatch = Some(etag), ifNoneMatch = None)
            else
              ZIO.unit  // we're not newer; nothing to do
          }
    }

    // On 412 PreconditionFailed (another writer beat us), re-read and retry.
    // Bounded to keep an unrelated bug from looping forever; in practice
    // contention should resolve in 1-2 retries.
    attempt.retry(
      Schedule.recurWhile[ExloError] {
        case ExloError.StorageError(_, cause: S3Exception) => cause.statusCode == 412
        case _                                              => false
      } && Schedule.recurs(5)
    )

  def scan(connector: String, stream: String, filter: Filter[String]):
      ZStream[Any, ExloError, StateRow] =
    val all = listAndDecode(streamPrefix(connector, stream))
    filter match
      case Filter.ByKey(k) =>
        all.filter(_.key == k)
      case Filter.Tail(n)  =>
        ZStream.unwrap(
          all.runCollect.map { rows =>
            val sorted = rows.sortBy(r => (r.committedAt, r.syncId)).reverse.take(n)
            ZStream.fromIterable(sorted)
          }
        )

  // ----- helpers -----------------------------------------------------------

  private def keyPath(connector: String, stream: String, key: String): String =
    s"${config.prefix.stripSuffix("/")}/state/connector=$connector/stream=$stream/key=${urlEncode(key)}.json"

  private def streamPrefix(connector: String, stream: String): String =
    s"${config.prefix.stripSuffix("/")}/state/connector=$connector/stream=$stream/"

  private def urlEncode(s: String): String =
    URLEncoder.encode(s, StandardCharsets.UTF_8)

  private def getObjectAsString(key: String): IO[ExloError, Option[String]] =
    ZIO.fromCompletableFuture(
      s3.getObject(
        GetObjectRequest.builder().bucket(config.bucket).key(key).build(),
        AsyncResponseTransformer.toBytes[GetObjectResponse]()
      )
    ).foldZIO(
      {
        case _: NoSuchKeyException => ZIO.none
        case t                     =>
          ZIO.fail(ExloError.StorageError(s"S3StateStore.get($key): ${t.getMessage}", t))
      },
      bytes => ZIO.succeed(Some(bytes.asUtf8String()))
    )

  /** GET the object body alongside its ETag. None if the object doesn't exist. */
  private def getObjectWithEtag(key: String): IO[ExloError, Option[(String, String)]] =
    ZIO.fromCompletableFuture(
      s3.getObject(
        GetObjectRequest.builder().bucket(config.bucket).key(key).build(),
        AsyncResponseTransformer.toBytes[GetObjectResponse]()
      )
    ).foldZIO(
      {
        case _: NoSuchKeyException => ZIO.none
        case t                     =>
          ZIO.fail(ExloError.StorageError(s"S3StateStore.get($key): ${t.getMessage}", t))
      },
      bytes => ZIO.succeed(Some((bytes.asUtf8String(), bytes.response.eTag)))
    )

  /** Conditional PUT. `ifMatch` requires the existing ETag to match;
   *  `ifNoneMatch = "*"` requires no object to exist at the key. Failures
   *  with status 412 (PreconditionFailed) bubble up so `merge`'s retry
   *  can detect the conflict and re-read. */
  private def putConditional(
      key:         String,
      body:        String,
      ifMatch:     Option[String],
      ifNoneMatch: Option[String]
  ): IO[ExloError, Unit] =
    ZIO.fromCompletableFuture {
      val builder0 = PutObjectRequest.builder().bucket(config.bucket).key(key)
      val builder1 = ifMatch.fold(builder0)(builder0.ifMatch)
      val builder2 = ifNoneMatch.fold(builder1)(builder1.ifNoneMatch)
      s3.putObject(builder2.build(), AsyncRequestBody.fromString(body))
    }.mapError(t =>
      ExloError.StorageError(s"S3StateStore.put($key): ${t.getMessage}", t)
    ).unit

  private def listObjects(prefix: String): ZStream[Any, ExloError, S3Object] =
    // Manual pagination via continuation tokens. None means "stop"; Some("") is the
    // sentinel for "first page, no token yet."
    ZStream.unfoldChunkZIO[Any, ExloError, S3Object, Option[String]](Some("")) {
      case None => ZIO.none
      case Some(token) =>
        val builder = ListObjectsV2Request.builder().bucket(config.bucket).prefix(prefix)
        val req     = if token.isEmpty then builder.build() else builder.continuationToken(token).build()
        ZIO.fromCompletableFuture(s3.listObjectsV2(req)).mapError(t =>
          ExloError.StorageError(s"S3StateStore.list($prefix): ${t.getMessage}", t)
        ).map { resp =>
          import scala.jdk.CollectionConverters.*
          val contents  = Chunk.fromIterable(resp.contents.asScala)
          val nextToken = if resp.isTruncated then Some(resp.nextContinuationToken) else None
          Some((contents, nextToken))
        }
    }

  private def listAndDecode(prefix: String): ZStream[Any, ExloError, StateRow] =
    listObjects(prefix).mapZIO { obj =>
      getObjectAsString(obj.key()).flatMap {
        case None       =>
          ZIO.fail(ExloError.StorageError(
            s"row vanished mid-scan: ${obj.key()}", new RuntimeException(obj.key())
          ))
        case Some(str)  =>
          ZIO.fromEither(str.fromJson[StateRow])
            .mapError(msg => decodeError(obj.key(), msg))
      }
    }

  private def decodeError(key: String, msg: String): ExloError =
    ExloError.StateError(s"could not decode StateRow at $key: $msg", new RuntimeException(msg))

  private def isNewer(a: StateRow, b: StateRow): Boolean =
    val c = a.committedAt.compareTo(b.committedAt)
    if c != 0 then c > 0 else a.syncId.compareTo(b.syncId) > 0


object S3StateStore:

  // Instant ↔ epoch millis for compact, sortable JSON.
  given JsonEncoder[Instant] = JsonEncoder[Long].contramap(_.toEpochMilli)
  given JsonDecoder[Instant] = JsonDecoder[Long].map(Instant.ofEpochMilli)
  given JsonEncoder[StateRow] = DeriveJsonEncoder.gen[StateRow]
  given JsonDecoder[StateRow] = DeriveJsonDecoder.gen[StateRow]

  def make(config: S3Config): ZIO[S3AsyncClient, Nothing, S3StateStore] =
    ZIO.service[S3AsyncClient].map(s3 => new S3StateStore(s3, config))

  def layer(config: S3Config): ZLayer[S3AsyncClient, Nothing, StateStore] =
    ZLayer.fromFunction((s3: S3AsyncClient) => new S3StateStore(s3, config): StateStore)
