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
 * - StorageConfig configured to connect to both
 *
 * Pattern from Zionomicon Ch 44: Shared layer for expensive resources.
 */
object NessieTestContainer:

  private val nessiePort       = 19120
  private val minioPort        = 9000
  private val minioConsolePort = 9001

  /**
   * ZLayer that provides StorageConfig configured for test containers.
   *
   * Lifecycle:
   * 1. Starts Nessie container
   * 2. Starts MinIO container
   * 3. Creates StorageConfig with connection details
   * 4. Acquires once per suite when using provideLayerShared
   * 5. Releases containers after all tests complete
   */
  val layer: ZLayer[Any, Throwable, StorageConfig] =
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

        // Create StorageConfig pointing to test containers
        config = StorageConfig(
          warehousePath = "s3://test-bucket/warehouse",
          nessie = Some(
            NessieConfig(
              uri = nessieUri,
              defaultBranch = "main",
              authToken = None,
              properties = Map(
                // Iceberg S3FileIO configuration
                "io-impl"              -> "org.apache.iceberg.aws.s3.S3FileIO",
                "s3.endpoint"          -> minioEndpoint,
                "s3.access-key-id"     -> accessKey,
                "s3.secret-access-key" -> secretKey,
                "s3.path-style-access" -> "true",
                "client.region"        -> "us-east-1" // MinIO doesn't care about region but S3FileIO requires it
              )
            )
          )
        )

      } yield config
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
