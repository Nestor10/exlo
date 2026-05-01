package exlo.s3

import software.amazon.awssdk.auth.credentials.{
  AwsBasicCredentials, DefaultCredentialsProvider, StaticCredentialsProvider
}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3AsyncClient
import zio.*

import java.net.URI

/**
 * `S3AsyncClient` ZLayers. Use [[layer]] for production (default credential
 * provider chain) and [[layerWithCredentials]] for tests pointing at MinIO.
 */
object S3:

  def layer(config: S3Config): TaskLayer[S3AsyncClient] =
    ZLayer.scoped {
      ZIO.acquireRelease(
        buildClient(config, creds = None)
      )(c => ZIO.attempt(c.close()).ignoreLogged)
    }

  def layerWithCredentials(
      config:    S3Config,
      accessKey: String,
      secretKey: String
  ): TaskLayer[S3AsyncClient] =
    ZLayer.scoped {
      ZIO.acquireRelease(
        buildClient(config, creds = Some((accessKey, secretKey)))
      )(c => ZIO.attempt(c.close()).ignoreLogged)
    }

  private def buildClient(
      config: S3Config,
      creds:  Option[(String, String)]
  ): Task[S3AsyncClient] =
    ZIO.attempt {
      val builder = S3AsyncClient.builder().region(Region.of(config.region))

      val withCreds = creds match
        case Some((ak, sk)) =>
          builder.credentialsProvider(
            StaticCredentialsProvider.create(AwsBasicCredentials.create(ak, sk))
          )
        case None =>
          builder.credentialsProvider(DefaultCredentialsProvider.create())

      val withEndpoint = config.endpoint.fold(withCreds)(ep =>
        withCreds.endpointOverride(URI.create(ep))
      )

      val withPathStyle =
        if config.forcePathStyle then withEndpoint.forcePathStyle(true) else withEndpoint

      withPathStyle.build()
    }
