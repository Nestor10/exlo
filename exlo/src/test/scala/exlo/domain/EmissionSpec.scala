package exlo.domain

import zio.test.*

object EmissionSpec extends ZIOSpecDefault:

  def spec = suite("Emission")(
    test("Record carries the typed value") {
      val r: Emission[String, Int] = Emission.Record("hello")
      r match
        case Emission.Record(v) => assertTrue(v == "hello")
        case other              => assertNever(s"expected Record, got $other")
    },
    test("Mark carries the state value") {
      val m: Emission[String, Int] = Emission.Mark(42)
      m match
        case Emission.Mark(s) => assertTrue(s == 42)
        case other            => assertNever(s"expected Mark, got $other")
    },
    test("Record is covariant in O; Mark is covariant in S") {
      // Compile-time check: Emission[String, Nothing] is assignable to Emission[String, Int].
      val r: Emission[String, Nothing] = Emission.Record("x")
      val widened: Emission[String, Int] = r
      assertTrue(widened == Emission.Record("x"))
    }
  )
