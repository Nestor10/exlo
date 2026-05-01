package exlo.s3

import exlo.runtime.{Filter, StateRow, StateStore}
import software.amazon.awssdk.services.s3.S3AsyncClient
import zio.*
import zio.test.*

import java.time.Instant

object S3StateStoreSpec extends ZIOSpecDefault:

  private val t0 = Instant.parse("2026-01-01T00:00:00Z")
  private def at(seconds: Int): Instant = t0.plusSeconds(seconds.toLong)

  private def row(
      connector: String = "c1",
      stream:    String = "s1",
      key:       String = StateStore.WatermarkKey,
      value:     String = "v",
      committedAt: Instant = t0,
      syncId:    String = "sync-a"
  ): StateRow =
    StateRow(connector, stream, key, value, committedAt, syncId)

  override def spec: Spec[TestEnvironment & Scope, Any] =
    suite("S3StateStore (MinIO)")(
      test("readByKey on absent key returns None") {
        for
          s3   <- ZIO.service[S3AsyncClient]
          cfg  <- ZIO.service[S3Config]
          ss    = new S3StateStore(s3, cfg)
          got  <- ss.readByKey("missing-c", "missing-s", "missing-k")
        yield assertTrue(got.isEmpty)
      },
      test("merge then readByKey round-trips") {
        for
          s3  <- ZIO.service[S3AsyncClient]
          cfg <- ZIO.service[S3Config]
          ss   = new S3StateStore(s3, cfg)
          r    = row(connector = "rt", stream = "s", value = "v1")
          _   <- ss.merge(r)
          got <- ss.readByKey(r.connector, r.stream, r.key)
        yield assertTrue(got.exists(_.value == "v1"))
      },
      test("merge with newer committedAt wins; older is ignored") {
        for
          s3  <- ZIO.service[S3AsyncClient]
          cfg <- ZIO.service[S3Config]
          ss   = new S3StateStore(s3, cfg)
          r1   = row(connector = "newest", value = "v1", committedAt = at(0))
          r2   = row(connector = "newest", value = "v2", committedAt = at(10))
          r3   = row(connector = "newest", value = "v3", committedAt = at(5))   // older than r2
          _   <- ss.merge(r1)
          _   <- ss.merge(r2)
          _   <- ss.merge(r3)
          got <- ss.readByKey(r1.connector, r1.stream, r1.key)
        yield assertTrue(got.exists(_.value == "v2"))
      },
      test("scan ByKey returns the matching row only") {
        for
          s3  <- ZIO.service[S3AsyncClient]
          cfg <- ZIO.service[S3Config]
          ss   = new S3StateStore(s3, cfg)
          a    = row(connector = "scan", key = "k1", value = "1")
          b    = row(connector = "scan", key = "k2", value = "2")
          _   <- ss.merge(a) *> ss.merge(b)
          got <- ss.scan("scan", a.stream, Filter.ByKey("k1")).runCollect
        yield assertTrue(got.size == 1, got.head.value == "1")
      },
      test("scan Tail(n) returns last n by (committedAt, syncId), newest first") {
        for
          s3   <- ZIO.service[S3AsyncClient]
          cfg  <- ZIO.service[S3Config]
          ss    = new S3StateStore(s3, cfg)
          rows  = (1 to 5).map(i => row(connector = "tail", key = s"k$i", value = i.toString, committedAt = at(i)))
          _    <- ZIO.foreachDiscard(rows)(ss.merge)
          tail <- ss.scan("tail", "s1", Filter.Tail(3)).runCollect
        yield assertTrue(
          tail.size == 3,
          tail.map(_.value).toList == List("5", "4", "3")
        )
      }
    ).provideLayerShared(MinioFixture.layer.orDie) @@ TestAspect.withLiveClock
