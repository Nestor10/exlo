val scala3Version    = "3.7.3"
val zioVersion       = "2.1.22"
val zioConfigVersion = "4.0.5"
val icebergVersion   = "1.10.0"
val awsVersion       = "2.37.2"
val nessieVersion    = "0.105.6"

lazy val root = (project in file("."))
  .aggregate(exlo, examples)
  .settings(
    name           := "exlo-root",
    publish / skip := true
  )

lazy val exlo = project
  .in(file("exlo"))
  .settings(
    name         := "exlo",
    version      := "0.1.0-SNAPSHOT",
    scalaVersion := scala3Version,
    libraryDependencies ++= Seq(
      // ZIO Core
      "dev.zio" %% "zio"         % zioVersion,
      "dev.zio" %% "zio-streams" % zioVersion,

      // ZIO Config
      "dev.zio" %% "zio-config"          % zioConfigVersion,
      "dev.zio" %% "zio-config-magnolia" % zioConfigVersion,
      "dev.zio" %% "zio-config-typesafe" % zioConfigVersion,

      // Apache Iceberg
      "org.apache.iceberg" % "iceberg-core"    % icebergVersion,
      "org.apache.iceberg" % "iceberg-parquet" % icebergVersion,
      "org.apache.iceberg" % "iceberg-nessie"  % icebergVersion,
      "org.apache.iceberg" % "iceberg-aws"     % icebergVersion, // S3FileIO (bypasses Hadoop)

      // AWS SDK v2 (required by iceberg-aws, not transitive)
      "software.amazon.awssdk" % "s3"       % awsVersion,
      "software.amazon.awssdk" % "glue"     % awsVersion,
      "software.amazon.awssdk" % "dynamodb" % awsVersion,
      "software.amazon.awssdk" % "kms"      % awsVersion,
      "software.amazon.awssdk" % "sts"      % awsVersion,

      // Nessie Catalog
      "org.projectnessie.nessie" % "nessie-client" % nessieVersion,

      // Parquet (already included transitively, but explicit for clarity)
      "org.apache.parquet" % "parquet-avro" % "1.14.3",

      // Hadoop (Parquet writer needs Configuration class)
      "org.apache.hadoop" % "hadoop-common" % "3.4.1" % "runtime",

      // Testing
      "dev.zio"       %% "zio-test"     % zioVersion % Test,
      "dev.zio"       %% "zio-test-sbt" % zioVersion % Test,
      "org.scalameta" %% "munit"        % "1.0.0"    % Test,

      // Testcontainers for integration tests
      "com.dimafeng"      %% "testcontainers-scala-core" % "0.41.4" % Test,
      "org.testcontainers" % "testcontainers"            % "1.20.4" % Test,
      "org.testcontainers" % "minio"                     % "1.20.4" % Test
    ),
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")
  )

lazy val examples = project
  .in(file("examples"))
  .dependsOn(exlo)
  .settings(
    name         := "exlo-examples",
    scalaVersion := scala3Version,
    libraryDependencies ++= Seq(
      "dev.zio" %% "zio"         % zioVersion,
      "dev.zio" %% "zio-streams" % zioVersion,
      "dev.zio" %% "zio-http"    % "3.0.1",
      "dev.zio" %% "zio-json"    % "0.7.3"
    )
  )
