package exlo.domain

/**
 * Metadata about a Parquet data file written to cloud storage.
 *
 * Represents a file that has been written but not yet committed to Iceberg.
 * Files are written during stream processing and accumulated until a checkpoint
 * triggers an Iceberg commit.
 *
 * @param path
 *   Cloud storage path (e.g., "s3://bucket/namespace/table/data/file-001.parquet")
 * @param recordCount
 *   Number of ExloRecords in this file
 * @param sizeBytes
 *   File size in bytes for monitoring and optimization
 */
case class DataFile(
  path: String,
  recordCount: Long,
  sizeBytes: Long
)
