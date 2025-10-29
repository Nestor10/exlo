val scala3Version = "3.7.3"
val zioVersion    = "2.1.14"

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

      // Testing
      "dev.zio"       %% "zio-test"     % zioVersion % Test,
      "dev.zio"       %% "zio-test-sbt" % zioVersion % Test,
      "org.scalameta" %% "munit"        % "1.0.0"    % Test
    ),
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")
  )
