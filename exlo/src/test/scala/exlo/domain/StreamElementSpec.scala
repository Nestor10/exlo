package exlo.domain

import zio.test.*
import zio.test.Assertion.*

/** Tests for StreamElement domain model.
  *
  * Verifies the union type behavior and pattern matching.
  */
object StreamElementSpec extends ZIOSpecDefault:

  def spec = suite("StreamElement")(
    test("Data contains user record string") {
      val element = StreamElement.Data("""{"id":1}""")
      assertTrue(
        element.isInstanceOf[StreamElement.Data],
        element.asInstanceOf[StreamElement.Data].record == """{"id":1}"""
      )
    },
    
    test("Checkpoint contains user state string") {
      val element = StreamElement.Checkpoint("""{"cursor":"abc"}""")
      assertTrue(
        element.isInstanceOf[StreamElement.Checkpoint],
        element.asInstanceOf[StreamElement.Checkpoint].state == """{"cursor":"abc"}"""
      )
    },
    
    test("pattern matching on Data") {
      val element: StreamElement = StreamElement.Data("payload")
      val result = element match {
        case StreamElement.Data(record)       => s"got data: $record"
        case StreamElement.Checkpoint(state) => s"got checkpoint: $state"
      }
      assertTrue(result == "got data: payload")
    },
    
    test("pattern matching on Checkpoint") {
      val element: StreamElement = StreamElement.Checkpoint("state")
      val result = element match {
        case StreamElement.Data(record)       => s"got data: $record"
        case StreamElement.Checkpoint(state) => s"got checkpoint: $state"
      }
      assertTrue(result == "got checkpoint: state")
    }
  )
