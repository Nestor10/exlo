package exlo.runtime.s3

import org.testcontainers.containers.MinIOContainer
import org.testcontainers.utility.DockerImageName
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.CreateBucketRequest
import zio.*

import java.net.URI

/**
 * Shared MinIO testcontainer + S3AsyncClient + bucket setup. Provided as a
 * single `ZLayer` so test specs can wire it via `provideLayerShared`.
 *
 * Container start is the slow part (~3-5 s). The shared layer ensures it
 * happens once per spec, not once per test.
 */
object MinioFixture:

  /** Bucket name fixed across tests; created on layer build. */
  val bucket: String = "exlo-test"

  private val image = DockerImageName.parse("minio/minio:RELEASE.2024-12-13T22-19-12Z")

  /** ZLayer producing a started container, a configured S3AsyncClient, and an
   *  S3Config pointing at the test bucket. The bucket is created on build. */
  val layer: ZLayer[Any, Throwable, S3AsyncClient & S3Config] =
    ZLayer.scopedEnvironment {
      for
        container <- ZIO.acquireRelease(
                       ZIO.attemptBlocking {
                         val c = new MinIOContainer(image)
                         c.start()
                         c
                       }
                     )(c => ZIO.attemptBlocking(c.stop()).ignoreLogged)

        client    <- ZIO.acquireRelease(
                       ZIO.attempt(
                         S3AsyncClient.builder()
                           .endpointOverride(URI.create(container.getS3URL))
                           .credentialsProvider(
                             StaticCredentialsProvider.create(
                               AwsBasicCredentials.create(container.getUserName, container.getPassword)
                             )
                           )
                           .region(Region.US_EAST_1)
                           .forcePathStyle(true)
                           .build()
                       )
                     )(c => ZIO.attempt(c.close()).ignoreLogged)

        _ <- ZIO.fromCompletableFuture(
               client.createBucket(CreateBucketRequest.builder().bucket(bucket).build())
             )

        config = S3Config(
                   bucket         = bucket,
                   prefix         = "test",
                   region         = "us-east-1",
                   endpoint       = Some(container.getS3URL),
                   forcePathStyle = true
                 )
      yield ZEnvironment[S3AsyncClient](client).add[S3Config](config)
    }
