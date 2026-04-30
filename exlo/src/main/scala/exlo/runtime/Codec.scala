package exlo.runtime

import exlo.domain.ExloError
import zio.json.{JsonDecoder, JsonEncoder}

/**
 * Encodes and decodes a connector's state to/from `String`. The StateStore
 * persists state as opaque text; every connector ships a `Codec[S]` to seal
 * the boundary.
 *
 * Encoding is total. Decoding can fail (corrupt or schema-evolved state) and
 * surfaces as `ExloError.StateError` so the framework's effect channel stays
 * `IO[ExloError, _]` end-to-end.
 */
trait Codec[A]:
  def encode(a: A): String
  def decode(s: String): Either[ExloError.StateError, A]

object Codec:

  def apply[A](using c: Codec[A]): Codec[A] = c

  given string: Codec[String] = new Codec[String]:
    def encode(a: String): String                              = a
    def decode(s: String): Either[ExloError.StateError, String] = Right(s)

  given long: Codec[Long] = new Codec[Long]:
    def encode(a: Long): String = a.toString
    def decode(s: String): Either[ExloError.StateError, Long] =
      s.toLongOption.toRight(
        ExloError.StateError(s"could not decode '$s' as Long", new RuntimeException(s))
      )

  /**
   * Adapter for any type with zio-json `JsonEncoder`/`JsonDecoder` instances.
   * Use for case-class state shapes; bring your own derivations.
   */
  def fromJson[A](using enc: JsonEncoder[A], dec: JsonDecoder[A]): Codec[A] =
    new Codec[A]:
      def encode(a: A): String = enc.encodeJson(a, None).toString
      def decode(s: String): Either[ExloError.StateError, A] =
        dec.decodeJson(s).left.map(msg =>
          ExloError.StateError(s"could not decode state JSON: $msg", new RuntimeException(msg))
        )
