package exlo.runtime

import zio.*
import zio.test.*

object FlushPolicySpec extends ZIOSpecDefault:

  def spec = suite("FlushPolicy")(
    test("default has positive maxRows and positive maxInterval") {
      val p = FlushPolicy.default
      assertTrue(p.maxRows > 0, p.maxInterval.toNanos > 0L)
    },
    test("rejects non-positive maxRows") {
      val r = scala.util.Try(FlushPolicy(maxRows = 0, maxInterval = 1.second))
      assertTrue(r.isFailure)
    },
    test("rejects non-positive maxInterval") {
      val r = scala.util.Try(FlushPolicy(maxRows = 100, maxInterval = Duration.Zero))
      assertTrue(r.isFailure)
    }
  )
