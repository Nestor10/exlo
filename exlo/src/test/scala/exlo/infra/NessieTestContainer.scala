package exlo.infra

import com.dimafeng.testcontainers.GenericContainer
import exlo.domain.*
import org.testcontainers.containers.wait.strategy.Wait
import zio.*

/**
 * Test infrastructure for running integration tests with real Nessie and MinIO.
 *
 * Provides:
 * - Nessie catalog server (Git-like version control for Iceberg)
 * - MinIO (S3-compatible object storage)
 * - Helper method to create catalog layers for specific tables
 *
 * Pattern: Each test creates its own catalog layer with namespace/tableName using catalogLayer().
 */
object NessieTestContainer:

  private val nessiePort       = 19120
  private val minioPort        = 9000
  private val minioConsolePort = 9001

  /**
   * Create a layer providing IcebergCatalog for a specific table.
   *
   * Lifecycle:
   * 1. Starts containers (scoped to this layer)
   * 2. Creates ConfigProvider with container URLs + namespace/tableName
   * 3. Loads StorageConfig from the test ConfigProvider
   * 4. Creates IcebergCatalog using the loaded config
   *
   * Each test should call this to get a fresh catalog instance.
   */
  def catalogLayer(namespace: String, tableName: String): ZLayer[Any, Throwable, IcebergCatalog] =
    ZLayer.scoped {
      for {
        // Start Nessie container
        nessie <- startNessie
        nessieUri = s"http://localhost:${nessie.mappedPort(nessiePort)}/api/v1"

        // Start MinIO container
        minio <- startMinIO
        minioEndpoint = s"http://localhost:${minio.mappedPort(minioPort)}"

        // MinIO default credentials
        accessKey = "minioadmin"
        secretKey = "minioadmin"

        // Create test bucket in MinIO
        _ <- createBucket(minio, "test-bucket", accessKey, secretKey)

        // Build config map with storage + stream config
        testConfig = Map(
          "EXLO_STORAGE_WAREHOUSE_PATH"               -> "s3://test-bucket/warehouse",
          "EXLO_STORAGE_BACKEND_S3_REGION"            -> "us-east-1",
          "EXLO_STORAGE_BACKEND_S3_ENDPOINT"          -> minioEndpoint,
          "EXLO_STORAGE_BACKEND_S3_ACCESS_KEY_ID"     -> accessKey,
          "EXLO_STORAGE_BACKEND_S3_SECRET_ACCESS_KEY" -> secretKey,
          "EXLO_STORAGE_BACKEND_S3_PATH_STYLE_ACCESS" -> "true",
          "EXLO_STORAGE_CATALOG_NESSIE_URI"           -> nessieUri,
          "EXLO_STORAGE_CATALOG_NESSIE_BRANCH"        -> "main",
          "EXLO_STREAM_NAMESPACE"                     -> namespace,
          "EXLO_STREAM_TABLE_NAME"                    -> tableName
        )

        configProvider = exlo.config.ExloConfigProvider.fromMap(testConfig)

        // Load config and create catalog layers with the test config provider
        storageConfig <- StorageConfig.load.withConfigProvider(configProvider)

        // Create CatalogOps layer
        catalogOpsLayer = storageConfig.catalog match {
          case nessie: CatalogConfig.Nessie =>
            catalog.NessieCatalogOps.layer(
              storageConfig.warehousePath,
              storageConfig.storage,
              nessie
            )
          case _                            =>
            ZLayer.die(new IllegalStateException("Test containers only support Nessie catalog"))
        }

        // Compose with IcebergWriter and build IcebergCatalog
        catalog <- ZIO
          .service[IcebergCatalog]
          .provideLayer(catalogOpsLayer ++ IcebergWriter.Live.layer >>> IcebergCatalog.Live.layer)

      } yield catalog
    }

  /**
   * Start Nessie catalog container.
   *
   * Nessie provides Git-like version control for Iceberg tables.
   * Using the official ghcr.io/projectnessie/nessie image.
   */
  private def startNessie: ZIO[Scope, Throwable, GenericContainer] =
    ZIO.acquireRelease {
      ZIO.attempt {
        val container = GenericContainer(
          dockerImage = "ghcr.io/projectnessie/nessie:0.105.6",
          exposedPorts = Seq(nessiePort),
          waitStrategy = Wait.forHttp("/api/v1/trees").forStatusCode(200)
        )
        container.start()
        container
      }
    }(container => ZIO.attempt(container.stop()).orDie)

  /**
   * Start MinIO container for S3-compatible storage.
   *
   * MinIO is a lightweight, S3-compatible object store perfect for testing.
   */
  private def startMinIO: ZIO[Scope, Throwable, GenericContainer] =
    ZIO.acquireRelease {
      ZIO.attempt {
        val container = GenericContainer(
          dockerImage = "minio/minio:RELEASE.2024-10-02T17-50-41Z",
          exposedPorts = Seq(minioPort, minioConsolePort),
          command = Seq("server", "/data", "--console-address", s":$minioConsolePort"),
          waitStrategy = Wait.forHttp("/minio/health/ready").forPort(minioPort).forStatusCode(200)
        )
        container.start()
        container
      }
    }(container => ZIO.attempt(container.stop()).orDie)

  /**
   * Create a bucket in MinIO using mc (MinIO Client).
   *
   * MinIO containers come with mc pre-installed.
   */
  private def createBucket(
    minio: GenericContainer,
    bucketName: String,
    accessKey: String,
    secretKey: String
  ): Task[Unit] =
    ZIO.attempt {
      // Configure mc alias
      minio.container.execInContainer(
        "mc",
        "alias",
        "set",
        "testminio",
        s"http://localhost:$minioPort",
        accessKey,
        secretKey
      )

      // Create bucket
      val result = minio.container.execInContainer(
        "mc",
        "mb",
        s"testminio/$bucketName",
        "--ignore-existing"
      )

      if (result.getExitCode != 0) {
        throw new RuntimeException(s"Failed to create bucket: ${result.getStderr}")
      }
    }
