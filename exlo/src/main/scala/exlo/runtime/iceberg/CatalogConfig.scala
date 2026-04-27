package exlo.runtime.iceberg

import zio.*

/**
 * Catalog selection + connection params loaded from env. ZIO Config-driven so deployments
 * just set `EXLO_CATALOG_*` env vars; no code changes per environment.
 *
 * Env var schema:
 *   - `EXLO_CATALOG_TYPE` — `s3tables`, `glue`, or `hadoop`
 *   - `EXLO_CATALOG_WAREHOUSE`:
 *       * `s3tables`: table-bucket ARN (e.g. `arn:aws:s3tables:us-east-1:123:bucket/my-bucket`)
 *       * `glue`: S3 warehouse path (e.g. `s3://my-lake/warehouse`)
 *       * `hadoop`: filesystem path (e.g. `/tmp/exlo-warehouse`)
 *   - `EXLO_CATALOG_REGION` — required for `s3tables`; optional for `glue`; ignored for `hadoop`
 */
sealed trait CatalogConfig

object CatalogConfig:

  /**
   * AWS S3 Tables — managed Iceberg-as-a-service via the Iceberg REST Catalog spec.
   * Auto-handles compaction, Z-ordering, orphan cleanup, and snapshot expiration in the
   * background. The `warehouse` is the table-bucket ARN (NOT a `s3://` path).
   * Auth via AWS SDK v2 default credential chain + SigV4 signing for the `s3tables`
   * service. Region is required (S3 Tables endpoints are regional).
   *
   * Operational note: because S3 Tables auto-expires snapshots, child connectors using
   * incremental reads (`IcebergDataSource`) MUST run frequently enough to stay within the
   * retention window — otherwise their cursors will hit `ExloError.SnapshotExpired`.
   */
  final case class S3Tables(warehouse: String, region: String) extends CatalogConfig

  /**
   * AWS Glue Data Catalog. Authentication / region come from the AWS SDK v2 default
   * credential provider chain (IAM role, instance profile, `AWS_PROFILE`, `AWS_REGION`,
   * etc.). Set `region` only if you need to override the SDK's auto-detection.
   * Compaction / snapshot expiration are YOUR responsibility (orchestrated separately).
   */
  final case class Glue(warehouse: String, region: Option[String]) extends CatalogConfig

  /**
   * Hadoop catalog over a local directory or S3 path. No catalog server required;
   * directory layout doubles as the metastore.
   */
  final case class Hadoop(warehouse: String) extends CatalogConfig

  // ---- ZIO Config descriptor -------------------------------------------------------------

  /** Reads from the `exlo.catalog.*` namespace (env vars `EXLO_CATALOG_*`). */
  val config: Config[CatalogConfig] = {
    val tpe       = Config.string("type")
    val warehouse = Config.string("warehouse")
    val region    = Config.string("region").optional
    (tpe ++ warehouse ++ region)
      .nested("catalog")
      .nested("exlo")
      .mapOrFail {
        case ("s3tables", w, Some(r)) => Right(S3Tables(w, r))
        case ("s3tables", _, None) =>
          Left(Config.Error.InvalidData(
            message = "EXLO_CATALOG_REGION is required for catalog type 's3tables'"
          ))
        case ("glue", w, r)   => Right(Glue(w, r))
        case ("hadoop", w, _) => Right(Hadoop(w))
        case (other, _, _)    =>
          Left(Config.Error.InvalidData(
            message = s"Unknown catalog type '$other' (expected 's3tables', 'glue', or 'hadoop')"
          ))
      }
  }

  /** Load from the ambient `ConfigProvider` (env vars by default). */
  def fromEnv: ZIO[Any, Config.Error, CatalogConfig] = ZIO.config(config)
