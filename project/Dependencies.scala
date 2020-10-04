import sbt._
import Keys._

import sbt._

object Dependencies {

  object V {
    val cats       = "2.2.0"
    val catsEffect = "2.2.0"
    val circe      = "0.13.0"
    val fs2        = "2.4.2"
    val log4cats   = "1.1.1"
    val shapeless  = "2.4.0-M1"

    val logback = "1.2.3"

    val betterMonadicFor = "0.3.1"
    val contextApplied   = "0.1.4"
    val kindProjector    = "0.11.0"

    val munit = "0.7.13"
    val enumeratum = "1.6.1"
    val cormorant = "0.3.0"
    val keypool = "0.2.0"
    val kittens = "2.1.0"
    val munitScalacheckEffect = "0.2.0"
    val munitCatsEffect = "0.3.0"
  }

  object Libraries {
    def cats(artifact: String): ModuleID     = "org.typelevel"     %% s"cats-$artifact"     % V.cats
    def log4cats(artifact: String): ModuleID = "io.chrisdavenport" %% s"log4cats-$artifact" % V.log4cats

    val catsEffect = "org.typelevel"     %% "cats-effect"        % V.catsEffect
    val fs2Core    = "co.fs2"            %% "fs2-core"           % V.fs2
    val shapeless  = "com.chuusai"       %% "shapeless"          % V.shapeless
    val enumeratum = "com.beachape"      %% "enumeratum"         % V.enumeratum
    val keypool    = "io.chrisdavenport" %% "keypool"            % V.keypool
    val kittens    = "org.typelevel"     %% "kittens"            % V.kittens


    object Cormorant {
      val core              = "io.chrisdavenport" %% "cormorant-core"     % V.cormorant
      val generic           = "io.chrisdavenport" %% "cormorant-generic"  % V.cormorant
      val parser            = "io.chrisdavenport" %% "cormorant-parser"   % V.cormorant
    }

    val slf4jApi   = "org.slf4j"      %  "slf4j-api"         % "1.7.30"
    val slf4jLog4j = "org.slf4j"      %  "slf4j-log4j12"     % "1.7.30"      % "provided"
    val log4j      = "log4j"          %  "log4j"             % "1.2.17"      % "provided"

    val log4CatsCore = log4cats("core")

    // Examples libraries
    val circeCore     = "io.circe" %% "circe-core" % V.circe
    val circeGeneric  = "io.circe" %% "circe-generic" % V.circe
    val circeParser   = "io.circe" %% "circe-parser" % V.circe
    val log4CatsSlf4j = log4cats("slf4j")
    val logback       = "ch.qos.logback" % "logback-classic" % V.logback

    // Testing libraries
    val catsLaws         = cats("core")
    val catsTestKit      = cats("testkit")
    val munitCore        = "org.scalameta" %% "munit" % V.munit
    val munitScalacheckEffect = "org.typelevel" %% "scalacheck-effect-munit" % V.munitScalacheckEffect
    val munitCatsEffect = "org.typelevel" %% "munit-cats-effect" % V.munitCatsEffect
  }

  object CompilerPlugins {
    val betterMonadicFor = compilerPlugin("com.olegpy"     %% "better-monadic-for" % V.betterMonadicFor)
    val contextApplied   = compilerPlugin("org.augustjune" %% "context-applied"    % V.contextApplied)
    val kindProjector = compilerPlugin(
      "org.typelevel" % "kind-projector" % V.kindProjector cross CrossVersion.full
    )
  }

}
