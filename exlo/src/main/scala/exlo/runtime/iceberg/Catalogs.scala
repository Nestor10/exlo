package exlo.runtime.iceberg

import org.apache.hadoop.conf.Configuration
import org.apache.iceberg.{CatalogProperties, PartitionSpec, Table}
import org.apache.iceberg.aws.glue.GlueCatalog
import org.apache.iceberg.catalog.{Catalog, TableIdentifier}
import org.apache.iceberg.hadoop.HadoopCatalog
import org.apache.iceberg.rest.RESTCatalog
import zio.*

import scala.jdk.CollectionConverters.*

/**
 * Catalog factories. Each returns a Scoped `Catalog` — the catalog's `close()` is wired
 * into the scope's release, so `RESTCatalog`/`GlueCatalog` HTTP connection pools and the
 * like get torn down cleanly on framework shutdown.
 *
 * Glue: AWS SDK v2 default credential chain. IAM role / instance profile / `AWS_PROFILE` /
 * `AWS_ACCESS_KEY_ID` env vars are all picked up automatically. No catalog properties
 * for credentials — keep it pristine.
 *
 * Hadoop: directory-based; no server. Works with local FS, S3, etc. For S3, additional
 * `s3.*` properties may be needed (defer until requested).
 */
object Catalogs:

  /** Make a Scoped `Catalog` from a [[CatalogConfig]]. */
  def make(config: CatalogConfig): RIO[Scope, Catalog] = config match
    case CatalogConfig.S3Tables(warehouse, region) => makeS3Tables(warehouse, region)
    case CatalogConfig.Glue(warehouse, region)     => makeGlue(warehouse, region)
    case CatalogConfig.Hadoop(warehouse)           => makeHadoop(warehouse)

  /** ZLayer that resolves the `Catalog` from `CatalogConfig` in the env. */
  val layer: ZLayer[CatalogConfig, Throwable, Catalog] = ZLayer.scoped {
    ZIO.serviceWithZIO[CatalogConfig](make)
  }

  /** ZLayer that loads the config from env AND builds the catalog. */
  val live: ZLayer[Any, Throwable, Catalog] =
    ZLayer.fromZIO(CatalogConfig.fromEnv) >>> layer

  // ---- factories -------------------------------------------------------------------------

  /**
   * Amazon S3 Tables via the Iceberg REST Catalog spec. The catalog endpoint is regional
   * (`https://s3tables.<region>.amazonaws.com/iceberg`); the warehouse is the table-bucket
   * ARN. Auth is AWS SigV4 signed for the `s3tables` service — the SDK v2 default
   * credential chain (IAM role / IRSA / AWS_PROFILE / etc.) supplies the credentials.
   *
   * `RESTCatalog` ships in `iceberg-core`; no extra deps needed for this path.
   */
  private def makeS3Tables(warehouse: String, region: String): RIO[Scope, Catalog] =
    ZIO.fromAutoCloseable {
      ZIO.attemptBlocking {
        val catalog = new RESTCatalog()
        val props = Map(
          CatalogProperties.URI                -> s"https://s3tables.$region.amazonaws.com/iceberg",
          CatalogProperties.WAREHOUSE_LOCATION -> warehouse,
          CatalogProperties.FILE_IO_IMPL       -> "org.apache.iceberg.aws.s3.S3FileIO",
          "client.region"                      -> region,
          // Iceberg 1.10's REST catalog AWS SigV4 integration: signing-name targets the
          // `s3tables` service rather than the default `execute-api`.
          "rest.auth.type"                     -> "sigv4",
          "rest.sigv4-signer-region"           -> region,
          "rest.signing-name"                  -> "s3tables"
        )
        catalog.initialize("exlo-s3tables", props.asJava)
        catalog
      }
    }

  private def makeGlue(warehouse: String, region: Option[String]): RIO[Scope, Catalog] =
    ZIO.fromAutoCloseable {
      ZIO.attemptBlocking {
        val catalog = new GlueCatalog()
        val baseProps = Map(
          CatalogProperties.WAREHOUSE_LOCATION -> warehouse,
          CatalogProperties.FILE_IO_IMPL       -> "org.apache.iceberg.aws.s3.S3FileIO"
        )
        // Region is optional — if not set, the AWS SDK v2 default chain (`AWS_REGION`,
        // `AWS_DEFAULT_REGION`, IMDS metadata, etc.) supplies it. Override only when you
        // need a specific region different from what the chain reports.
        // NOTE: the canonical Iceberg property key for region is `client.region`. If this
        // doesn't take effect for you, try also setting `glue.region`.
        val props = region.fold(baseProps)(r => baseProps + ("client.region" -> r))
        catalog.initialize("exlo-glue", props.asJava)
        catalog
      }
    }

  private def makeHadoop(warehouse: String): RIO[Scope, Catalog] =
    ZIO.fromAutoCloseable {
      ZIO.attemptBlocking {
        // HadoopCatalog manages namespaces and table directories under the warehouse path.
        // Constructor takes (Configuration, warehousePath) directly.
        new HadoopCatalog(new Configuration(), warehouse)
      }
    }

  /**
   * Load (or create) the table identified by `(namespace, name)` against the catalog.
   * If the table doesn't exist, it's created with the [[IcebergDestination]] schema.
   */
  def loadOrCreateTable(
      catalog: Catalog,
      namespace: String,
      name: String
  ): Task[Table] =
    ZIO.attemptBlocking {
      val ns         = org.apache.iceberg.catalog.Namespace.of(namespace)
      val identifier = TableIdentifier.of(ns, name)

      // Best-effort namespace creation; HadoopCatalog auto-creates, GlueCatalog needs it
      // to exist. SupportsNamespaces is the interface that exposes createNamespace.
      catalog match
        case sn: org.apache.iceberg.catalog.SupportsNamespaces =>
          if !sn.namespaceExists(ns) then
            try sn.createNamespace(ns)
            catch
              // Race-friendly: another fiber/process may have created it concurrently.
              case _: org.apache.iceberg.exceptions.AlreadyExistsException => ()
        case _ => ()

      if catalog.tableExists(identifier) then catalog.loadTable(identifier)
      else
        catalog.createTable(
          identifier,
          IcebergDestination.schema,
          IcebergDestination.partitionSpec
        )
    }
