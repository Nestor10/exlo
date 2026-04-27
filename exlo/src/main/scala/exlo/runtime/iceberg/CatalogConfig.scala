package exlo.runtime.iceberg

import zio.*

/**
 * Catalog selection + connection params loaded from env. ZIO Config-driven so deployments
 * just set `EXLO_CATALOG_*` env vars; no code changes per environment.
 *
 * Env var schema:
 *   - `EXLO_CATALOG_TYPE` — `glue` or `hadoop`
 *   - `EXLO_CATALOG_WAREHOUSE` — warehouse location (e.g. `s3://my-lake/warehouse`,
 *     `/tmp/exlo-warehouse`)
 *   - `EXLO_CATALOG_REGION` — AWS region for Glue (optional; falls back to SDK default
 *     credential chain / `AWS_REGION`)
 */
sealed trait CatalogConfig

object CatalogConfig:

  /**
   * AWS Glue Data Catalog. Authentication / region come from the AWS SDK v2 default
   * credential provider chain (IAM role, instance profile, `AWS_PROFILE`, `AWS_REGION`,
   * etc.). Set `region` only if you need to override the SDK's auto-detection.
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
        case ("glue", w, r)   => Right(Glue(w, r))
        case ("hadoop", w, _) => Right(Hadoop(w))
        case (other, _, _)    =>
          Left(Config.Error.InvalidData(message = s"Unknown catalog type '$other' (expected 'glue' or 'hadoop')"))
      }
  }

  /** Load from the ambient `ConfigProvider` (env vars by default). */
  def fromEnv: ZIO[Any, Config.Error, CatalogConfig] = ZIO.config(config)
