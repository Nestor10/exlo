package exlo.runtime.s3

import exlo.runtime.Sequenced
import software.amazon.awssdk.core.async.AsyncResponseTransformer
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.{
  GetObjectRequest, GetObjectResponse, ListObjectsV2Request
}
import zio.*
import zio.json.*
import zio.json.ast.Json
import zio.test.*

import java.io.{BufferedReader, InputStreamReader}
import java.util.zip.GZIPInputStream

object S3DataSinkSpec extends ZIOSpecDefault:

  override def spec: Spec[TestEnvironment & Scope, Any] =
    suite("S3DataSink (MinIO)")(
      test("write produces a gzipped JSONL object containing the exlo envelope") {
        for
          s3        <- ZIO.service[S3AsyncClient]
          cfg       <- ZIO.service[S3Config]
          sink      <- S3DataSink.make(cfg, "c1", "s1", "sync-1").provideEnvironment(ZEnvironment(s3))
          batch      = Chunk(
                         Sequenced(1, """{"id":1,"name":"alice"}"""),
                         Sequenced(2, """{"id":2,"name":"bob"}""")
                       )
          durable   <- sink.write(batch)
          listed    <- ZIO.fromCompletableFuture(s3.listObjectsV2(
                         ListObjectsV2Request.builder()
                           .bucket(cfg.bucket)
                           .prefix(s"${cfg.prefix}/data/connector=c1/")
                           .build()
                       ))
          keys       = listed.contents.toArray.toList.map {
                         case o: software.amazon.awssdk.services.s3.model.S3Object => o.key()
                       }
          firstKey   = keys.head
          bytesResp <- ZIO.fromCompletableFuture(s3.getObject(
                         GetObjectRequest.builder().bucket(cfg.bucket).key(firstKey).build(),
                         AsyncResponseTransformer.toBytes[GetObjectResponse]()
                       ))
          decoded   <- ZIO.attempt {
                         val gz  = new GZIPInputStream(bytesResp.asInputStream)
                         val br  = new BufferedReader(new InputStreamReader(gz, "UTF-8"))
                         val ls  = Iterator.continually(br.readLine()).takeWhile(_ != null).toList
                         br.close()
                         ls
                       }
          parsed     = decoded.flatMap(_.fromJson[Json].toOption)
        yield assertTrue(
          durable == 2L,
          keys.size == 1,
          firstKey.endsWith("part-00000001.jsonl.gz"),
          firstKey.contains("connector=c1"),
          firstKey.contains("stream=s1"),
          firstKey.contains("sync_id=sync-1"),
          decoded.size == 2,
          parsed.size == 2,
          // Each parsed line is an object with our three envelope fields.
          parsed.forall(j => j.asObject.exists { obj =>
            obj.fields.exists(_._1 == "_exlo_ab_id") &&
            obj.fields.exists(_._1 == "_exlo_emitted_at") &&
            obj.fields.exists(_._1 == "_exlo_data")
          })
        )
      },
      test("two writes produce two distinct objects with monotonic flush sequence") {
        for
          s3      <- ZIO.service[S3AsyncClient]
          cfg     <- ZIO.service[S3Config]
          sink    <- S3DataSink.make(cfg, "c2", "s2", "sync-2").provideEnvironment(ZEnvironment(s3))
          _       <- sink.write(Chunk(Sequenced(1, """{"a":1}""")))
          _       <- sink.write(Chunk(Sequenced(2, """{"a":2}""")))
          listed  <- ZIO.fromCompletableFuture(s3.listObjectsV2(
                       ListObjectsV2Request.builder().bucket(cfg.bucket)
                         .prefix(s"${cfg.prefix}/data/connector=c2/").build()
                     ))
          keys     = listed.contents.toArray.toList.map {
                       case o: software.amazon.awssdk.services.s3.model.S3Object => o.key()
                     }.sorted
        yield assertTrue(
          keys.size == 2,
          keys.head.endsWith("part-00000001.jsonl.gz"),
          keys(1).endsWith("part-00000002.jsonl.gz")
        )
      }
    ).provideLayerShared(MinioFixture.layer.orDie) @@ TestAspect.withLiveClock
