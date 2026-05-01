package exlo.runtime

import exlo.domain.{ExloError, Tag}
import zio.*
import zio.stream.ZStream

/**
 * A stream of records exposed to a child connector via its env. Each parent
 * connector contributes one `Source[T <: Tag]`, where `T` is the parent's
 * `Connector.Out`. Multiple Sources of distinct `T` types coexist freely in
 * the same env — the type tag is the disambiguator.
 *
 * Live wiring is provided by [[FedBy]], a `ZLayer` producer that runs the
 * parent connector internally and pipes its records through a bounded queue.
 * Test impls ([[Source.fromIterable]], [[Source.empty]]) skip the runner.
 */
trait Source[T <: Tag]:
  def stream: ZStream[Any, ExloError, String]

object Source:

  /** Test impl: emit a fixed sequence of records, then complete. */
  def fromIterable[T <: Tag](records: Iterable[String]): Source[T] =
    new Source[T]:
      def stream: ZStream[Any, ExloError, String] = ZStream.fromIterable(records)

  /** Test impl: emit nothing. */
  def empty[T <: Tag]: Source[T] =
    new Source[T]:
      def stream: ZStream[Any, ExloError, String] = ZStream.empty
