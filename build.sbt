import com.scalapenos.sbt.prompt.SbtPrompt.autoImport._
import com.scalapenos.sbt.prompt._
import Dependencies._

ThisBuild / name := "effredis"
ThisBuild / crossScalaVersions := Seq("2.12.12", "2.13.4")

inThisBuild(List(
  organization := "io.github.debasishg",
  homepage := Some(url("https://github.com/debasishg/effRedis")),
  licenses := List("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0")),
  developers := List(
    Developer(
      "debasishg",
      "Debasish Ghosh",
      "dghosh@acm.org",
      url("https://debasishg.blogspot.com")
    )
  )
))

// thanks https://github.com/profunktor/redis4cats/blob/master/build.sbt
promptTheme := PromptTheme(
  List(
    text("[sbt] ", fg(105)),
    text(_ => "effredis", fg(15)).padRight(" λ ")
  )
)

def pred[A](p: Boolean, t: => Seq[A], f: => Seq[A]): Seq[A] =
  if (p) t else f

def version(strVersion: String): Option[(Long, Long)] = CrossVersion.partialVersion(strVersion)

val commonSettings = Seq(
  organizationName := "Redis client for Scala",
  startYear := Some(2020),
  licenses += ("Apache-2.0", new URL("https://www.apache.org/licenses/LICENSE-2.0.txt")),
  headerLicense := Some(HeaderLicense.ALv2("2020", "Debasish Ghosh")),
  testFrameworks += new TestFramework("munit.Framework"),
  libraryDependencies ++= Seq(
        CompilerPlugins.betterMonadicFor,
        CompilerPlugins.contextApplied,
        CompilerPlugins.kindProjector,
        Libraries.catsEffect,
        Libraries.shapeless,
        Libraries.kittens,
        Libraries.enumeratum,
        Libraries.Cormorant.core,
        Libraries.Cormorant.generic,
        Libraries.Cormorant.parser,
        Libraries.slf4jLog4j,
        Libraries.slf4jApi,
        Libraries.log4j,
        Libraries.catsLaws         % Test,
        Libraries.catsTestKit      % Test,
        Libraries.munitCore        % Test,
        Libraries.munitScalacheckEffect  % Test,
        Libraries.munitCatsEffect % Test
      ),
  resolvers += "Apache public" at "https://repository.apache.org/content/groups/public/",
  scalacOptions ++= pred(
        version(scalaVersion.value) == Some(2, 12),
        t = Seq("-Xmax-classfile-name", "80"),
        f = Seq.empty
      ),
  sources in (Compile, doc) := (sources in (Compile, doc)).value,
  scalacOptions in (Compile, doc) ++= Seq("-groups", "-implicits"),
  autoAPIMappings := true,
  scalafmtOnCompile := true
)

lazy val noPublish = Seq(
  publish := {},
  publishLocal := {},
  publishArtifact := false,
  skip in publish := true
)

lazy val `effredis-root` = project
  .in(file("."))
  .aggregate(
    `effredis-core`,
    `effredis-log4cats`,
    `effredis-examples`,
    `effredis-tests`
  )
  .settings(noPublish)

lazy val `effredis-core` = project
  .in(file("modules/core"))
  .settings(commonSettings: _*)
  .settings(libraryDependencies += Libraries.keypool)
  .settings(parallelExecution in Test := false)
  .enablePlugins(AutomateHeaderPlugin)

lazy val `effredis-log4cats` = project
  .in(file("modules/log4cats"))
  .settings(commonSettings: _*)
  .settings(libraryDependencies += Libraries.log4CatsCore)
  .settings(parallelExecution in Test := false)
  .enablePlugins(AutomateHeaderPlugin)
  .dependsOn(`effredis-core`)

lazy val `effredis-examples` = project
  .in(file("modules/examples"))
  .settings(commonSettings: _*)
  .settings(libraryDependencies += Libraries.log4CatsSlf4j)
  .settings(libraryDependencies += Libraries.keypool)
  .settings(libraryDependencies += Libraries.logback % "runtime")
  .settings(parallelExecution in Test := false)
  .enablePlugins(AutomateHeaderPlugin)
  .dependsOn(`effredis-core`)
  .dependsOn(`effredis-log4cats`)

lazy val `effredis-tests` = project
  .in(file("modules/tests"))
  .settings(commonSettings: _*)
  .settings(libraryDependencies += Libraries.log4CatsSlf4j)
  .settings(libraryDependencies += Libraries.logback % "runtime")
  .settings(noPublish)
  .settings(parallelExecution in Test := false)
  .enablePlugins(AutomateHeaderPlugin)
  .dependsOn(`effredis-core`)
  .dependsOn(`effredis-log4cats`)