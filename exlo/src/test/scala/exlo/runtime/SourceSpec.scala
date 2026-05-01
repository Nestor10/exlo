package exlo.runtime

import exlo.domain.{Connector, Emission, ExloError, Tag}
import zio.*
import zio.stream.ZStream
import zio.test.*

object SourceSpec extends ZIOSpecDefault:

  private trait UsersTag extends Tag

  def spec = suite("Source")(
    test("fromIterable emits the given records and completes") {
      val src: Source[UsersTag] = Source.fromIterable[UsersTag](List("a", "b", "c"))
      for got <- src.stream.runCollect
      yield assertTrue(got.toList == List("a", "b", "c"))
    },
    test("empty emits nothing and completes") {
      val src: Source[UsersTag] = Source.empty[UsersTag]
      for got <- src.stream.runCollect
      yield assertTrue(got.isEmpty)
    },
    test("FedBy wires parent records to a Source for child consumption") {
      val parent: Connector[UsersTag, Long, Any] =
        new Connector[UsersTag, Long, Any]:
          val id           = "users"
          val version      = "1.0.0"
          val initialState = 0L
          def reduce(a: Long, b: Long): Long = a max b
          val codec        = Codec.long
          def dataStream(resume: Long): ZStream[Any, ExloError, Emission[Long]] =
            ZStream.fromIterable(List(
              Emission.Record("u1"),
              Emission.Record("u2"),
              Emission.Record("u3")
            ))

      val collect: ZIO[Source[UsersTag], ExloError, Chunk[String]] =
        ZIO.serviceWithZIO[Source[UsersTag]](_.stream.runCollect)

      for got <- collect.provideLayer(FedBy(parent))
      yield assertTrue(got.toList == List("u1", "u2", "u3"))
    }
  )
