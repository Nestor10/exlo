package exlo.runtime.s3

/**
 * Configuration for the S3-backed [[exlo.runtime.s3.S3DataSink]] and
 * [[exlo.runtime.s3.S3StateStore]].
 *
 * Layout:
 *   - Data:  `s3://{bucket}/{prefix}/data/connector={id}/stream={s}/sync_id={syncId}/part-NN.jsonl.gz`
 *   - State: `s3://{bucket}/{prefix}/state/connector={id}/stream={s}/key={urlenc}.json`
 *
 * `endpoint` and `forcePathStyle` are for MinIO / non-AWS S3 implementations.
 * Leave them at the defaults for real AWS S3.
 */
final case class S3Config(
    bucket:         String,
    prefix:         String          = "exlo",
    region:         String          = "us-east-1",
    endpoint:       Option[String]  = None,
    forcePathStyle: Boolean         = false
):
  require(bucket.nonEmpty, "bucket must be non-empty")
