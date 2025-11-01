val scala3Version  = "3.7.3"
val zioVersion     = "2.1.14"
val icebergVersion = "1.7.0"
val awsVersion     = "2.29.29"
val nessieVersion  = "0.99.0"

lazy val root = project
  .in(file("."))
  .settings(
    name         := "exlo",
    version      := "0.1.0-SNAPSHOT",
    scalaVersion := scala3Version,
    libraryDependencies ++= Seq(
      // ZIO Core
      "dev.zio" %% "zio"         % zioVersion,
      "dev.zio" %% "zio-streams" % zioVersion,

      // Apache Iceberg
      "org.apache.iceberg" % "iceberg-core"    % icebergVersion,
      "org.apache.iceberg" % "iceberg-parquet" % icebergVersion,
      "org.apache.iceberg" % "iceberg-nessie"  % icebergVersion,

      // Nessie Catalog
      "org.projectnessie.nessie" % "nessie-client" % nessieVersion,

      // AWS SDK for S3
      "software.amazon.awssdk" % "s3"  % awsVersion,
      "software.amazon.awssdk" % "sts" % awsVersion,

      // Parquet (already included transitively, but explicit for clarity)
      "org.apache.parquet" % "parquet-avro" % "1.14.3",

      // Testing
      "dev.zio"       %% "zio-test"     % zioVersion % Test,
      "dev.zio"       %% "zio-test-sbt" % zioVersion % Test,
      "org.scalameta" %% "munit"        % "1.0.0"    % Test
    ),
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")
  )
