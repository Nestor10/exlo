package exlo.domain

/** Internal pipeline elements after data files have been written.
  *
  * This is distinct from `StreamElement` which represents what users emit.
  * After writing ExloRecords to Parquet files, the pipeline works with:
  * - DataFileWritten: A file has been flushed to cloud storage
  * - Checkpoint: State marker (same as StreamElement.Checkpoint)
  *
  * This separation allows the pipeline to accumulate file paths (tiny memory)
  * instead of actual records (large memory).
  */
enum PipelineElement:
  /** A Parquet file has been written to cloud storage with ExloRecords. */
  case DataFileWritten(file: DataFile)
  
  /** State checkpoint marker - triggers Iceberg commit of accumulated files. */
  case StateCheckpoint(state: String)
