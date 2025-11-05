package exlo.pipeline

import exlo.domain.DataFile
import exlo.domain.PipelineElement
import exlo.domain.StreamElement
import zio.*
import zio.stream.*

/**
 * Stream processing utilities for checkpoint-based grouping.
 *
 * Implements the core pipeline logic for:
 * 1. Batching records for file size control (groupedWithin)
 * 2. Writing Parquet files to cloud storage (not accumulating in memory)
 * 3. Tracking file paths until checkpoint (tiny memory footprint)
 * 4. Committing files atomically with state on checkpoint
 */
object StreamProcessor:

  /**
   * Groups data file paths between checkpoint markers.
   *
   * After records are written to Parquet files, this pipeline accumulates the
   * file paths (not records!) until a checkpoint arrives, then emits the batch
   * of files with the checkpoint state for atomic Iceberg commit.
   *
   * Memory characteristics:
   * - Accumulates file paths only (String + metadata): ~100 bytes per file
   * - Even 10,000 files between checkpoints = ~1MB memory
   * - Actual record data never held in memory (already in cloud storage)
   *
   * Behavior:
   * - DataFileWritten: accumulate file path
   * - StateCheckpoint: emit accumulated files with checkpoint state, reset
   * - Large data/few checkpoints: many files accumulated (OK - just paths)
   * - Little data/many checkpoints: few/zero files per checkpoint (OK)
   *
   * Example:
   * {{{
   * Input:  DataFileWritten(f1), DataFileWritten(f2), StateCheckpoint("s1"),
   *         DataFileWritten(f3), StateCheckpoint("s2")
   * Output: (Chunk(f1, f2), "s1"), (Chunk(f3), "s2")
   * }}}
   *
   * @return
   *   Pipeline that transforms PipelineElements into (files, state) batches
   */
  def groupByCheckpoint: ZPipeline[Any, Nothing, PipelineElement, (Chunk[DataFile], String)] =
    ZPipeline
      .mapAccum(Chunk.empty[DataFile]) { (accumulatedFiles, element) =>
        element match
          case PipelineElement.DataFileWritten(file) =>
            (accumulatedFiles :+ file, None)

          case PipelineElement.StateCheckpoint(state) =>
            (Chunk.empty, Some((accumulatedFiles, state)))
      }
      .collect { case Some(batch) => batch }
