package exlo.runtime

import zio.*
import zio.test.*

import java.time.Instant

object StateStoreSpec extends ZIOSpecDefault:

  private val t0 = Instant.parse("2026-04-30T00:00:00Z")
  private def at(seconds: Int): Instant = t0.plusSeconds(seconds.toLong)

  private def row(
      connector: String = "github_issues",
      stream:    String = "issues",
      key:       String = StateStore.WatermarkKey,
      value:     String = "v",
      committedAt: Instant = t0,
      syncId:    String = "sync-a"
  ): StateRow =
    StateRow(connector, stream, key, value, committedAt, syncId)

  def spec = suite("StateStore.InMemory")(
    test("readByKey on empty store returns None") {
      for
        s   <- StateStore.InMemory.make
        got <- s.readByKey("c", "s", "k")
      yield assertTrue(got.isEmpty)
    },
    test("merge then readByKey round-trips") {
      for
        s   <- StateStore.InMemory.make
        r    = row(value = "v1")
        _   <- s.merge(r)
        got <- s.readByKey(r.connector, r.stream, r.key)
      yield assertTrue(got.contains(r))
    },
    test("merge with a newer committedAt wins") {
      for
        s   <- StateStore.InMemory.make
        r1   = row(value = "v1", committedAt = at(0))
        r2   = row(value = "v2", committedAt = at(10))
        _   <- s.merge(r1)
        _   <- s.merge(r2)
        got <- s.readByKey(r1.connector, r1.stream, r1.key)
      yield assertTrue(got.exists(_.value == "v2"))
    },
    test("merge with an older committedAt is ignored") {
      for
        s   <- StateStore.InMemory.make
        r1   = row(value = "v1", committedAt = at(10))
        r2   = row(value = "v2", committedAt = at(0))
        _   <- s.merge(r1)
        _   <- s.merge(r2)
        got <- s.readByKey(r1.connector, r1.stream, r1.key)
      yield assertTrue(got.exists(_.value == "v1"))
    },
    test("merge with same committedAt resolves by syncId") {
      for
        s   <- StateStore.InMemory.make
        r1   = row(value = "v1", committedAt = at(0), syncId = "sync-a")
        r2   = row(value = "v2", committedAt = at(0), syncId = "sync-z")
        _   <- s.merge(r1)
        _   <- s.merge(r2)
        got <- s.readByKey(r1.connector, r1.stream, r1.key)
      yield assertTrue(got.exists(_.value == "v2"))
    },
    test("rows are isolated by (connector, stream, key)") {
      for
        s    <- StateStore.InMemory.make
        a     = row(connector = "a", value = "av")
        b     = row(connector = "b", value = "bv")
        _    <- s.merge(a) *> s.merge(b)
        gotA <- s.readByKey("a", a.stream, a.key)
        gotB <- s.readByKey("b", b.stream, b.key)
        gotC <- s.readByKey("c", a.stream, a.key)
      yield assertTrue(
        gotA.exists(_.value == "av"),
        gotB.exists(_.value == "bv"),
        gotC.isEmpty
      )
    },
    test("scan ByKey returns at most one matching row") {
      for
        s     <- StateStore.InMemory.make
        a      = row(key = "k1", value = "1")
        b      = row(key = "k2", value = "2")
        _     <- s.merge(a) *> s.merge(b)
        found <- s.scan(a.connector, a.stream, Filter.ByKey("k1")).runCollect
      yield assertTrue(found.size == 1, found.head.value == "1")
    },
    test("scan Tail(n) returns last n by (committedAt, syncId), newest first") {
      for
        s    <- StateStore.InMemory.make
        rows  = (1 to 5).map(i => row(key = s"k$i", value = i.toString, committedAt = at(i)))
        _    <- ZIO.foreachDiscard(rows)(s.merge)
        tail <- s.scan(rows.head.connector, rows.head.stream, Filter.Tail(3)).runCollect
      yield assertTrue(
        tail.size == 3,
        tail.map(_.value).toList == List("5", "4", "3")
      )
    },
    test("Tail rejects n <= 0") {
      val r = scala.util.Try(Filter.Tail(0))
      assertTrue(r.isFailure)
    }
  )
