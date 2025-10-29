package exlo.service

import exlo.domain.{ExloError, ExloRecord}
import zio.*
import zio.test.*
import zio.test.Assertion.*
import java.util.UUID
import java.time.Instant

/** Tests for StateWriter service.
  *
  * Verifies state writing behavior with record commits.
  */
object StateWriterSpec extends ZIOSpecDefault:

  val sampleRecords = Chunk(
    ExloRecord(
      commitId = UUID.randomUUID(),
      connectorId = "test.connector",
      syncId = UUID.randomUUID(),
      committedAt = Instant.now(),
      recordedAt = Instant.now(),
      connectorVersion = "1.0.0",
      connectorConfigHash = "hash1",
      streamConfigHash = "hash2",
      stateVersion = 1L,
      payload = """{"id":1}"""
    )
  )

  def spec = suite("StateWriter")(
    test("stub tracks written state") {
      for
        writer <- ZIO.service[StateWriter]
        _      <- StateWriter.writeState("test", "table", 1L, sampleRecords, """{"cursor":"page_2"}""")
        stub = writer.asInstanceOf[StateWriter.Stub]
      yield assertTrue(
        stub.lastState == """{"cursor":"page_2"}""",
        stub.recordCount == 1
      )
    }.provide(StateWriter.Stub.layer),
    
    test("stub accumulates record count across writes") {
      for
        writer <- ZIO.service[StateWriter]
        _      <- StateWriter.writeState("test", "table", 1L, sampleRecords, """{"cursor":"page_2"}""")
        _      <- StateWriter.writeState("test", "table", 1L, sampleRecords ++ sampleRecords, """{"cursor":"page_3"}""")
        stub = writer.asInstanceOf[StateWriter.Stub]
      yield assertTrue(
        stub.lastState == """{"cursor":"page_3"}""",
        stub.recordCount == 3
      )
    }.provide(StateWriter.Stub.layer),
    
    test("live implementation fails with not implemented") {
      for
        result <- StateWriter.writeState("test", "table", 1L, sampleRecords, "{}").exit
      yield assertTrue(
        result.isFailure,
        result match {
          case Exit.Failure(cause) =>
            cause.failureOrCause match {
              case Left(ExloError.IcebergWriteError(_)) => true
              case _                                     => false
            }
          case _ => false
        }
      )
    }.provide(StateWriter.live)
  )
