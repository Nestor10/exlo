val scala3Version    = "3.7.3"
val zioVersion       = "2.1.25"
val zioConfigVersion = "4.0.5"
val zioJsonVersion   = "0.9.0"
val zioHttpVersion   = "3.11.0"
val zioLoggingVersion = "2.5.0"
val zioOtelVersion   = "4.0.0-RC11"
val otelSdkVersion   = "1.61.0"
val otelInstrVersion = "2.20.0-alpha"
val awsVersion       = "2.37.2"
val testcontainersVersion = "1.20.4"

lazy val root = (project in file("."))
  .aggregate(exlo, exloIt)
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

      // ZIO JSON (state serialization, exlo envelope)
      "dev.zio" %% "zio-json" % zioJsonVersion,

      // ZIO HTTP (connector authoring; framework itself does not depend on it)
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

      // AWS SDK v2 — S3 data sink + S3 state store. STS/SSO included so the default
      // credential provider chain works with assume-role and SSO profiles in dev.
      "software.amazon.awssdk" % "s3"      % awsVersion,
      "software.amazon.awssdk" % "sts"     % awsVersion,
      "software.amazon.awssdk" % "sso"     % awsVersion,
      "software.amazon.awssdk" % "ssooidc" % awsVersion,

      // Testing
      "dev.zio" %% "zio-test"          % zioVersion     % Test,
      "dev.zio" %% "zio-test-sbt"      % zioVersion     % Test,
      "dev.zio" %% "zio-http-testkit"  % zioHttpVersion % Test
    ),
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework"),

    Test / fork := true,

    Compile / doc / sources := Seq.empty
  )

/**
 * Integration tests. Anything that needs Docker (testcontainers — MinIO,
 * etc.) lives here so `sbt exlo/test` stays fast and dependency-free for
 * the inner loop. Run integration tests via `sbt exloIt/test` or
 * `sbt test` (which aggregates both).
 */
lazy val exloIt = project
  .in(file("exlo-it"))
  .dependsOn(exlo % "compile->compile;test->test")
  .settings(
    name           := "exlo-it",
    version        := "0.2.0-SNAPSHOT",
    scalaVersion   := scala3Version,
    publish / skip := true,
    libraryDependencies ++= Seq(
      "dev.zio"           %% "zio-test"                   % zioVersion              % Test,
      "dev.zio"           %% "zio-test-sbt"               % zioVersion              % Test,
      "com.dimafeng"      %% "testcontainers-scala-core"  % "0.41.4"                % Test,
      "org.testcontainers" % "testcontainers"             % testcontainersVersion   % Test,
      "org.testcontainers" % "minio"                      % testcontainersVersion   % Test
    ),
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework"),

    Test / fork := true
  )
