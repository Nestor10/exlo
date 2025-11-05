package exlo.config

import zio.*
import zio.test.*
import zio.test.Assertion.*

/** Tests for ExloConfigProvider with real StorageConfigData structures. */
object ExloConfigProviderSpec extends ZIOSpecDefault:

  def spec = suite("ExloConfigProvider")(
    test("loads S3 backend with Nessie catalog from env vars") {
      val provider = ExloConfigProvider.fromMap(
        Map(
          "EXLO_STORAGE_WAREHOUSE_PATH" -> "s3://test-bucket/warehouse",
          "EXLO_STORAGE_BACKEND_S3_REGION" -> "us-east-1",
          "EXLO_STORAGE_CATALOG_NESSIE_URI" -> "http://localhost:19120/api/v1"
        )
      )

      assertZIO(
        ZIO
          .config(StorageConfigData.config)
          .provideLayer(Runtime.setConfigProvider(provider))
      )(
        equalTo(
          StorageConfigData(
            warehousePath = "s3://test-bucket/warehouse",
            backend = StorageBackendConfig.S3("us-east-1"),
            catalog = CatalogConfigData.Nessie(
              uri = "http://localhost:19120/api/v1",
              branch = "main",
              authToken = None
            )
          )
        )
      )
    },
    test("loads GCS backend with Glue catalog") {
      val provider = ExloConfigProvider.fromMap(
        Map(
          "EXLO_STORAGE_WAREHOUSE_PATH" -> "gs://test-bucket/warehouse",
          "EXLO_STORAGE_BACKEND_GCS_PROJECT_ID" -> "my-project",
          "EXLO_STORAGE_CATALOG_GLUE_REGION" -> "us-east-1"
        )
      )

      val result = ZIO
        .config(StorageConfigData.config)
        .provideLayer(Runtime.setConfigProvider(provider))

      // This should fail validation (Glue requires S3)
      assertZIO(result.exit)(
        fails(
          isSubtype[Config.Error](
            hasField(
              "message",
              _.getMessage,
              containsString("AWS Glue catalog requires S3 backend")
            )
          )
        )
      )
    },
    test("loads Azure backend with JDBC catalog") {
      val provider = ExloConfigProvider.fromMap(
        Map(
          "EXLO_STORAGE_WAREHOUSE_PATH" -> "abfss://container@account.dfs.core.windows.net/warehouse",
          "EXLO_STORAGE_BACKEND_AZURE_ACCOUNT_NAME" -> "mystorageaccount",
          "EXLO_STORAGE_CATALOG_JDBC_URI" -> "jdbc:postgresql://localhost:5432/catalog",
          "EXLO_STORAGE_CATALOG_JDBC_USERNAME" -> "user",
          "EXLO_STORAGE_CATALOG_JDBC_PASSWORD" -> "pass"
        )
      )

      assertZIO(
        ZIO
          .config(StorageConfigData.config)
          .provideLayer(Runtime.setConfigProvider(provider))
      )(
        equalTo(
          StorageConfigData(
            warehousePath =
              "abfss://container@account.dfs.core.windows.net/warehouse",
            backend = StorageBackendConfig.Azure("mystorageaccount"),
            catalog = CatalogConfigData.Jdbc(
              uri = "jdbc:postgresql://localhost:5432/catalog",
              username = "user",
              password = "pass"
            )
          )
        )
      )
    },
    test("loads Local backend with Hive catalog") {
      val provider = ExloConfigProvider.fromMap(
        Map(
          "EXLO_STORAGE_WAREHOUSE_PATH" -> "/tmp/warehouse",
          "EXLO_STORAGE_BACKEND_LOCAL_BASE_PATH" -> "/tmp/data",
          "EXLO_STORAGE_CATALOG_HIVE_URI" -> "thrift://localhost:9083"
        )
      )

      assertZIO(
        ZIO
          .config(StorageConfigData.config)
          .provideLayer(Runtime.setConfigProvider(provider))
      )(
        equalTo(
          StorageConfigData(
            warehousePath = "/tmp/warehouse",
            backend = StorageBackendConfig.Local("/tmp/data"),
            catalog = CatalogConfigData.Hive("thrift://localhost:9083")
          )
        )
      )
    },
    test("validates region matching between Glue and S3") {
      val provider = ExloConfigProvider.fromMap(
        Map(
          "EXLO_STORAGE_WAREHOUSE_PATH" -> "s3://bucket/warehouse",
          "EXLO_STORAGE_BACKEND_S3_REGION" -> "us-east-1",
          "EXLO_STORAGE_CATALOG_GLUE_REGION" -> "us-west-2"
        )
      )

      val result = ZIO
        .config(StorageConfigData.config)
        .provideLayer(Runtime.setConfigProvider(provider))

      assertZIO(result.exit)(
        fails(
          isSubtype[Config.Error](
            hasField(
              "message",
              _.getMessage,
              containsString("must match S3 region")
            )
          )
        )
      )
    },
    test("validates HTTPS Nessie requires auth token") {
      val provider = ExloConfigProvider.fromMap(
        Map(
          "EXLO_STORAGE_WAREHOUSE_PATH" -> "s3://bucket/warehouse",
          "EXLO_STORAGE_BACKEND_S3_REGION" -> "us-east-1",
          "EXLO_STORAGE_CATALOG_NESSIE_URI" -> "https://nessie.prod.com/api/v1"
        )
      )

      val result = ZIO
        .config(StorageConfigData.config)
        .provideLayer(Runtime.setConfigProvider(provider))

      assertZIO(result.exit)(
        fails(
          isSubtype[Config.Error](
            hasField(
              "message",
              _.getMessage,
              containsString("require auth-token")
            )
          )
        )
      )
    }
  )
