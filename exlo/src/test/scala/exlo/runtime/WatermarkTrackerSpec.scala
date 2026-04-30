package exlo.runtime

import zio.*
import zio.test.*

object WatermarkTrackerSpec extends ZIOSpecDefault:

  def spec = suite("WatermarkTracker")(
    test("advance on empty tracker returns empty chunk") {
      for
        wt       <- WatermarkTracker.make[String]
        released <- wt.advance(100L)
      yield assertTrue(released.isEmpty)
    },
    test("admit one; advance below its seq returns empty; advance at its seq releases it") {
      for
        wt        <- WatermarkTracker.make[String]
        _         <- wt.admit(Pending(10L, "s1"))
        below     <- wt.advance(9L)
        atOrAbove <- wt.advance(10L)
      yield assertTrue(
        below.isEmpty,
        atOrAbove.toList == List(Pending(10L, "s1"))
      )
    },
    test("advance is idempotent: re-advancing past released entries returns empty") {
      for
        wt    <- WatermarkTracker.make[String]
        _     <- wt.admit(Pending(5L, "s1"))
        first <- wt.advance(5L)
        again <- wt.advance(100L)
      yield assertTrue(
        first.toList == List(Pending(5L, "s1")),
        again.isEmpty
      )
    },
    test("multiple admits, partial advance: releases only the prefix in seq order") {
      for
        wt       <- WatermarkTracker.make[String]
        _        <- wt.admit(Pending(1L, "a"))
        _        <- wt.admit(Pending(3L, "c"))
        _        <- wt.admit(Pending(5L, "e"))
        _        <- wt.admit(Pending(7L, "g"))
        released <- wt.advance(4L)
        rest     <- wt.advance(100L)
      yield assertTrue(
        released.toList == List(Pending(1L, "a"), Pending(3L, "c")),
        rest.toList == List(Pending(5L, "e"), Pending(7L, "g"))
      )
    },
    test("out-of-order admit: release order is by seq, not insertion") {
      for
        wt       <- WatermarkTracker.make[String]
        _        <- wt.admit(Pending(7L, "g"))
        _        <- wt.admit(Pending(3L, "c"))
        _        <- wt.admit(Pending(5L, "e"))
        _        <- wt.admit(Pending(1L, "a"))
        released <- wt.advance(100L)
      yield assertTrue(
        released.toList == List(
          Pending(1L, "a"),
          Pending(3L, "c"),
          Pending(5L, "e"),
          Pending(7L, "g")
        )
      )
    },
    test("admit is concurrent-safe under heavy load") {
      for
        wt       <- WatermarkTracker.make[Int]
        _        <- ZIO.foreachParDiscard(1 to 1000)(i => wt.admit(Pending(i.toLong, i)))
        released <- wt.advance(1000L)
      yield assertTrue(
        released.size == 1000,
        released.map(_.seq).toList == (1L to 1000L).toList
      )
    }
  )
