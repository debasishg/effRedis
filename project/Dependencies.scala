import sbt._
import Keys._

import sbt._

object Dependencies {

  object V {
    val cats       = "2.7.0"
    val catsEffect = "2.5.4"
    val circe      = "0.13.0"
    val fs2        = "2.4.2"
    val log4cats   = "2.2.0"
    val shapeless  = "2.4.0-M1"

    val logback = "1.2.10"

    val betterMonadicFor = "0.3.1"
    val contextApplied   = "0.1.4"
    val kindProjector    = "0.13.2"

    val munit                 = "0.7.29"
    val enumeratum            = "1.7.0"
    val cormorant             = "0.3.0"
    val keypool               = "0.2.0"
    val kittens               = "2.3.2"
    val munitCatsEffect       = "1.0.7"
    val munitScalacheckEffect = "1.0.3"
  }

  object Libraries {
    def cats(artifact: String): ModuleID     = "org.typelevel"     %% s"cats-$artifact"     % V.cats
    def log4cats(artifact: String): ModuleID = "io.chrisdavenport" %% s"log4cats-$artifact" % V.log4cats

    val catsEffect = "org.typelevel"     %% "cats-effect" % V.catsEffect
    val fs2Core    = "co.fs2"            %% "fs2-core"    % V.fs2
    val shapeless  = "com.chuusai"       %% "shapeless"   % V.shapeless
    val enumeratum = "com.beachape"      %% "enumeratum"  % V.enumeratum
    val keypool    = "io.chrisdavenport" %% "keypool"     % V.keypool
    val kittens    = "org.typelevel"     %% "kittens"     % V.kittens

    object Cormorant {
      val core    = "io.chrisdavenport" %% "cormorant-core"    % V.cormorant
      val generic = "io.chrisdavenport" %% "cormorant-generic" % V.cormorant
      val parser  = "io.chrisdavenport" %% "cormorant-parser"  % V.cormorant
    }

    val slf4jApi   = "org.slf4j" % "slf4j-api"     % "1.7.35"
    val slf4jLog4j = "org.slf4j" % "slf4j-log4j12" % "1.7.35" % "provided"
    val log4j      = "log4j"     % "log4j"         % "1.2.17" % "provided"

    val log4CatsCore = log4cats("core")

    // Examples libraries
    val circeCore     = "io.circe" %% "circe-core" % V.circe
    val circeGeneric  = "io.circe" %% "circe-generic" % V.circe
    val circeParser   = "io.circe" %% "circe-parser" % V.circe
    val log4CatsSlf4j = log4cats("slf4j")
    val logback       = "ch.qos.logback" % "logback-classic" % V.logback

    // Testing libraries
    val catsLaws              = cats("core")
    val catsTestKit           = cats("testkit")
    val munitCore             = "org.scalameta" %% "munit" % V.munit
    val munitScalacheckEffect = "org.typelevel" %% "scalacheck-effect-munit" % V.munitScalacheckEffect
    val munitCatsEffect       = "org.typelevel" %% "munit-cats-effect-2" % V.munitCatsEffect
  }

  object CompilerPlugins {
    val betterMonadicFor = compilerPlugin("com.olegpy"     %% "better-monadic-for" % V.betterMonadicFor)
    val contextApplied   = compilerPlugin("org.augustjune" %% "context-applied"    % V.contextApplied)
    val kindProjector = compilerPlugin(
      "org.typelevel" % "kind-projector" % V.kindProjector cross CrossVersion.full
    )
  }

}
