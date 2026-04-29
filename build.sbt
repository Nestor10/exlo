val scala3Version    = "3.7.3"
val zioVersion       = "2.1.25"
val zioConfigVersion = "4.0.5"
val zioJsonVersion   = "0.9.0"
val zioHttpVersion   = "3.11.0"
val zioLoggingVersion = "2.5.0"
val zioOtelVersion   = "4.0.0-RC11"
val otelSdkVersion   = "1.61.0"
val otelInstrVersion = "2.20.0-alpha"
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
    version      := "0.2.0-SNAPSHOT",
    scalaVersion := scala3Version,
    libraryDependencies ++= Seq(
      // ZIO Core
      "dev.zio" %% "zio"         % zioVersion,
      "dev.zio" %% "zio-streams" % zioVersion,

      // ZIO JSON (state serialization)
      "dev.zio" %% "zio-json" % zioJsonVersion,

      // ZIO HTTP (HttpExtract builder)
      "dev.zio" %% "zio-http" % zioHttpVersion,

      // ZIO Config
      "dev.zio" %% "zio-config"          % zioConfigVersion,
      "dev.zio" %% "zio-config-magnolia" % zioConfigVersion,
      "dev.zio" %% "zio-config-typesafe" % zioConfigVersion,

      // ZIO Logging — JSON formatter with trace_id / span_id labels
      "dev.zio" %% "zio-logging" % zioLoggingVersion,

      // OpenTelemetry — pure-ZIO context (FiberRef storage). SDK is provided explicitly
      // per zio-opentelemetry 4.x. Trace/span IDs reach JSON logs via `logAnnotated = true`
      // on `OpenTelemetry.custom`, so the `zio-opentelemetry-zio-logging` LogRecord bridge
      // is unnecessary for stdout JSON.
      "dev.zio"           %% "zio-opentelemetry"             % zioOtelVersion,
      "io.opentelemetry"   % "opentelemetry-sdk"             % otelSdkVersion,
      "io.opentelemetry"   % "opentelemetry-exporter-otlp"   % otelSdkVersion,
      "io.opentelemetry"   % "opentelemetry-sdk-extension-autoconfigure" % otelSdkVersion,
      "io.opentelemetry.instrumentation" % "opentelemetry-runtime-telemetry-java17" % otelInstrVersion,

      // Apache Iceberg — the platform substrate for atomic commits + multi-connector DAGs
      "org.apache.iceberg" % "iceberg-core"    % icebergVersion,
      "org.apache.iceberg" % "iceberg-parquet" % icebergVersion,
      "org.apache.iceberg" % "iceberg-nessie"  % icebergVersion,
      "org.apache.iceberg" % "iceberg-aws"     % icebergVersion,

      // AWS SDK v2 — required by iceberg-aws (S3FileIO)
      "software.amazon.awssdk" % "s3"       % awsVersion,
      "software.amazon.awssdk" % "glue"     % awsVersion,
      "software.amazon.awssdk" % "dynamodb" % awsVersion,
      "software.amazon.awssdk" % "kms"      % awsVersion,
      "software.amazon.awssdk" % "sts"      % awsVersion,
      "software.amazon.awssdk" % "sso"     % awsVersion,
      "software.amazon.awssdk" % "ssooidc" % awsVersion,

      // Nessie catalog client
      "org.projectnessie.nessie" % "nessie-client" % nessieVersion,

      // Parquet (transitive via iceberg-parquet but explicit for clarity)
      "org.apache.parquet" % "parquet-avro" % "1.14.3",

      // Hadoop. hadoop-common is on the compile classpath because we reference
      // `org.apache.hadoop.conf.Configuration` directly when building HadoopCatalog.
      // hadoop-mapreduce-client-core is needed at read-time for Parquet's HadoopReadOptions.
      "org.apache.hadoop" % "hadoop-common"                % "3.4.1",
      "org.apache.hadoop" % "hadoop-mapreduce-client-core" % "3.4.1" % "runtime",

      // Testing
      "dev.zio" %% "zio-test"          % zioVersion     % Test,
      "dev.zio" %% "zio-test-sbt"      % zioVersion     % Test,
      "dev.zio" %% "zio-http-testkit"  % zioHttpVersion % Test,

      // Testcontainers for Iceberg integration tests (Nessie + MinIO)
      "com.dimafeng"      %% "testcontainers-scala-core" % "0.41.4" % Test,
      "org.testcontainers" % "testcontainers"            % "1.20.4" % Test,
      "org.testcontainers" % "minio"                     % "1.20.4" % Test
    ),
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework"),

    // Hadoop 3.x calls Subject.getSubject which is removed in Java 17+ unless the security
    // manager is allowed. Required for HadoopTables-based Iceberg integration tests.
    Test / fork := true,
    Test / javaOptions ++= Seq("-Djava.security.manager=allow")
  )

lazy val examples = project
  .in(file("examples"))
  .dependsOn(exlo % "compile->compile;test->test")
  .settings(
    name         := "exlo-examples",
    version      := "0.2.0-SNAPSHOT",
    scalaVersion := scala3Version,
    publish / skip := true,
    libraryDependencies ++= Seq(
      "dev.zio" %% "zio-test"         % zioVersion     % Test,
      "dev.zio" %% "zio-test-sbt"     % zioVersion     % Test,
      "dev.zio" %% "zio-http-testkit" % zioHttpVersion % Test
    ),
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")
  )
