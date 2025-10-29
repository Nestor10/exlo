package exlo.service

import exlo.domain.ExloError
import zio.*
import zio.test.*
import zio.test.Assertion.*

/** Tests for StateReader service.
  *
  * Verifies state reading behavior with version checking.
  */
object StateReaderSpec extends ZIOSpecDefault:

  def spec = suite("StateReader")(
    test("stub returns configured mock state") {
      for
        state <- StateReader.readState("test", "table", 1L)
      yield assertTrue(state == """{"cursor":"abc123"}""")
    }.provide(StateReader.Stub.withState("""{"cursor":"abc123"}""")),
    
    test("stub with empty state returns empty string") {
      for
        state <- StateReader.readState("test", "table", 1L)
      yield assertTrue(state == "")
    }.provide(StateReader.Stub.layer),
    
    test("live implementation fails with not implemented") {
      for
        result <- StateReader.readState("test", "table", 1L).exit
      yield assertTrue(
        result.isFailure,
        result match {
          case Exit.Failure(cause) =>
            cause.failureOrCause match {
              case Left(ExloError.StateReadError(_)) => true
              case _                                  => false
            }
          case _ => false
        }
      )
    }.provide(StateReader.live)
  )
