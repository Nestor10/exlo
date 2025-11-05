package exlo.config

import zio.*
import zio.config.*
import zio.config.magnolia.*
import zio.test.*
import zio.test.Assertion.*

/**
 * Tests to understand how zio-config handles environment variable naming
 * with sealed traits and nested case classes.
 */
object EnvVarNamingSpec extends ZIOSpecDefault:

  final case class TestConfig(
    warehousePath: String,
    maxRetries: Int
  )

  object TestConfig:

    val config: Config[TestConfig] =
      deriveConfig[TestConfig].nested("exlo", "storage")

  sealed trait Backend

  object Backend:
    case class S3(region: String)      extends Backend
    case class Local(basePath: String) extends Backend

  final case class ConfigWithSealed(
    backend: Backend
  )

  object ConfigWithSealed:

    val config: Config[ConfigWithSealed] =
      deriveConfig[ConfigWithSealed].nested("exlo", "storage")

  def spec = suite("Environment Variable Naming")(
    test("fromMap uses path separator with exact camelCase names") {
      // fromMap uses path-style keys with "/" separator
      val provider = ConfigProvider.fromMap(
        Map(
          "exlo/storage/warehousePath" -> "s3://test/path",
          "exlo/storage/maxRetries"    -> "5"
        ),
        "/"
      )

      assertZIO(ZIO.config(TestConfig.config).provideLayer(Runtime.setConfigProvider(provider)))(
        equalTo(TestConfig("s3://test/path", 5))
      )
    },
    test("environment variables use camelCase with underscore separator") {
      // envProvider expects: PREFIX_path_to_fieldName (exact case preserved!)
      val provider = ConfigProvider.fromMap(
        Map(
          "exlo_storage_warehousePath" -> "s3://test/path",
          "exlo_storage_maxRetries"    -> "5"
        ),
        "_"
      )

      assertZIO(ZIO.config(TestConfig.config).provideLayer(Runtime.setConfigProvider(provider)))(
        equalTo(TestConfig("s3://test/path", 5))
      )
    },
    test("snakeCase + upperCase = SCREAMING_SNAKE_CASE") {
      // Test the built-in transformation
      val provider = ConfigProvider
        .fromMap(
          Map(
            "EXLO_STORAGE_WAREHOUSE_PATH" -> "s3://test/path",
            "EXLO_STORAGE_MAX_RETRIES"    -> "5"
          ),
          "_"
        )
        .snakeCase
        .upperCase

      assertZIO(ZIO.config(TestConfig.config).provideLayer(Runtime.setConfigProvider(provider)))(
        equalTo(TestConfig("s3://test/path", 5))
      )
    },
    test("ExloConfigProvider handles S3 acronym correctly") {
      // Our custom provider should preserve S3 as S3, not S_3
      val provider = ExloConfigProvider.fromMap(
        Map(
          "EXLO_STORAGE_BACKEND_S3_REGION" -> "us-east-1"
        )
      )

      assertZIO(ZIO.config(ConfigWithSealed.config).provideLayer(Runtime.setConfigProvider(provider)))(
        equalTo(ConfigWithSealed(Backend.S3("us-east-1")))
      )
    },
    test("ExloConfigProvider handles all standard fields") {
      val provider = ExloConfigProvider.fromMap(
        Map(
          "EXLO_STORAGE_WAREHOUSE_PATH" -> "s3://bucket/warehouse",
          "EXLO_STORAGE_MAX_RETRIES"    -> "10"
        )
      )

      assertZIO(ZIO.config(TestConfig.config).provideLayer(Runtime.setConfigProvider(provider)))(
        equalTo(TestConfig("s3://bucket/warehouse", 10))
      )
    }
  )
