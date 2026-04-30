package exlo.runtime

import exlo.domain.ExloError
import zio.json.{DeriveJsonDecoder, DeriveJsonEncoder, JsonDecoder, JsonEncoder}
import zio.test.*

object CodecSpec extends ZIOSpecDefault:

  final case class Cursor(after: String, page: Int)
  object Cursor:
    given JsonEncoder[Cursor] = DeriveJsonEncoder.gen[Cursor]
    given JsonDecoder[Cursor] = DeriveJsonDecoder.gen[Cursor]

  def spec = suite("Codec")(
    suite("string")(
      test("encode/decode is identity") {
        val c = Codec.string
        assertTrue(
          c.encode("hello") == "hello",
          c.decode("hello") == Right("hello"),
          c.decode("") == Right("")
        )
      }
    ),
    suite("long")(
      test("round-trips") {
        val c = Codec.long
        assertTrue(
          c.encode(42L) == "42",
          c.decode("42") == Right(42L),
          c.decode(Long.MaxValue.toString) == Right(Long.MaxValue),
          c.decode(Long.MinValue.toString) == Right(Long.MinValue)
        )
      },
      test("decode fails as StateError on garbage") {
        val c = Codec.long
        c.decode("not a long") match
          case Left(_: ExloError.StateError) => assertCompletes
          case other                          => assertNever(s"expected StateError, got $other")
      }
    ),
    suite("fromJson")(
      test("round-trips a case class") {
        val c   = Codec.fromJson[Cursor]
        val v   = Cursor("abc123", 7)
        val enc = c.encode(v)
        assertTrue(c.decode(enc) == Right(v))
      },
      test("decode fails as StateError on malformed JSON") {
        val c = Codec.fromJson[Cursor]
        c.decode("{not json") match
          case Left(_: ExloError.StateError) => assertCompletes
          case other                          => assertNever(s"expected StateError, got $other")
      },
      test("decode fails as StateError on schema mismatch") {
        val c = Codec.fromJson[Cursor]
        c.decode("""{"after": "x"}""") match
          case Left(_: ExloError.StateError) => assertCompletes
          case other                          => assertNever(s"expected StateError, got $other")
      }
    )
  )
