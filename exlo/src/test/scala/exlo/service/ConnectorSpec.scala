package exlo.service

import exlo.domain.{StreamElement, ExloError}
import zio.*
import zio.stream.*
import zio.test.*
import zio.test.Assertion.*

/** Tests for Connector service interface.
  *
  * These tests verify the basic contract of the Connector trait using a stub
  * implementation.
  */
object ConnectorSpec extends ZIOSpecDefault:

  def spec = suite("Connector")(
    test("stub connector emits data and checkpoint elements") {
      for
        connector <- ZIO.service[Connector]
        elements  <- connector.extract("").runCollect
      yield assertTrue(
        elements.size == 5,
        elements.count(_.isInstanceOf[StreamElement.Data]) == 3,
        elements.count(_.isInstanceOf[StreamElement.Checkpoint]) == 2
      )
    }.provide(StubConnector.layer),
    
    test("stub connector has correct connectorId") {
      for
        connector <- ZIO.service[Connector]
      yield assertTrue(connector.connectorId == "stub.test")
    }.provide(StubConnector.layer),
    
    test("elements are emitted in correct order") {
      for
        connector <- ZIO.service[Connector]
        elements  <- connector.extract("").runCollect
      yield assertTrue(
        elements(0).isInstanceOf[StreamElement.Data],
        elements(1).isInstanceOf[StreamElement.Data],
        elements(2).isInstanceOf[StreamElement.Checkpoint],
        elements(3).isInstanceOf[StreamElement.Data],
        elements(4).isInstanceOf[StreamElement.Checkpoint]
      )
    }.provide(StubConnector.layer)
  )

/** Stub connector for testing - emits static test data. */
case class StubConnector() extends Connector:

  def connectorId: String = "stub.test"

  def extract(state: String): ZStream[Any, ExloError, StreamElement] =
    ZStream(
      StreamElement.Data("""{"id":1,"name":"Alice"}"""),
      StreamElement.Data("""{"id":2,"name":"Bob"}"""),
      StreamElement.Checkpoint("""{"cursor":"page_2"}"""),
      StreamElement.Data("""{"id":3,"name":"Charlie"}"""),
      StreamElement.Checkpoint("""{"cursor":"page_3"}""")
    )

object StubConnector:
  val layer: ZLayer[Any, Nothing, Connector] =
    ZLayer.succeed(StubConnector())
