package exlo.domain

import zio.test.*

object EmissionSpec extends ZIOSpecDefault:

  def spec = suite("Emission")(
    test("Record carries the literal string") {
      val r: Emission[Int] = Emission.Record("hello")
      r match
        case Emission.Record(v) => assertTrue(v == "hello")
        case other              => assertNever(s"expected Record, got $other")
    },
    test("Mark carries the state value") {
      val m: Emission[Int] = Emission.Mark(42)
      m match
        case Emission.Mark(s) => assertTrue(s == 42)
        case other            => assertNever(s"expected Mark, got $other")
    },
    test("Mark is covariant in S") {
      // Compile-time check: Emission[Nothing] is assignable to Emission[Int].
      val r: Emission[Nothing] = Emission.Record("x")
      val widened: Emission[Int] = r
      assertTrue(widened == Emission.Record("x"))
    }
  )
