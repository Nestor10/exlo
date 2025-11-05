package exlo.config

import zio.*
import zio.config.*
import zio.config.typesafe.TypesafeConfigProvider
import zio.test.*
import zio.test.Assertion.*

object StorageConfigDataSpec extends ZIOSpecDefault:

  def spec = suite("StorageConfigData")(
    test("parses storage config data with S3 backend and Nessie catalog") {
      assertZIO(ZIO.config(StorageConfigData.config))(
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
    }.provide(
      Runtime.setConfigProvider(
        TypesafeConfigProvider
          .fromHoconString("""
          exlo {
            storage {
              warehousePath = "s3://test-bucket/warehouse"
              backend {
                S3 {
                  region = "us-east-1"
                }
              }
              catalog {
                Nessie {
                  uri = "http://localhost:19120/api/v1"
                }
              }
            }
          }
        """)
      )
    ),
    test("validates Glue requires S3 backend") {
      val result = ZIO
        .config(StorageConfigData.config)
        .provide(
          Runtime.setConfigProvider(
            TypesafeConfigProvider
              .fromHoconString("""
              exlo {
                storage {
                  warehousePath = "gs://test-bucket/warehouse"
                  backend {
                    Gcs {
                      projectId = "my-project"
                    }
                  }
                  catalog {
                    Glue {
                      region = "us-east-1"
                    }
                  }
                }
              }
            """)
          )
        )

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
    test("validates Glue and S3 region match") {
      val result = ZIO
        .config(StorageConfigData.config)
        .provide(
          Runtime.setConfigProvider(
            TypesafeConfigProvider
              .fromHoconString("""
              exlo {
                storage {
                  warehousePath = "s3://test-bucket/warehouse"
                  backend {
                    S3 {
                      region = "us-east-1"
                    }
                  }
                  catalog {
                    Glue {
                      region = "us-west-2"
                    }
                  }
                }
              }
            """)
          )
        )

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
    test("validates Nessie HTTPS requires auth token") {
      val result = ZIO
        .config(StorageConfigData.config)
        .provide(
          Runtime.setConfigProvider(
            TypesafeConfigProvider
              .fromHoconString("""
              exlo {
                storage {
                  warehousePath = "s3://test-bucket/warehouse"
                  backend {
                    S3 {
                      region = "us-east-1"
                    }
                  }
                  catalog {
                    Nessie {
                      uri = "https://nessie.prod.com/api/v1"
                    }
                  }
                }
              }
            """)
          )
        )

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
