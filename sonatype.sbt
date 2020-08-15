// Your profile name of the sonatype account. The default is the same with the organization value
ThisBuild / sonatypeProfileName := "io.github.debasishg"

// To sync with Maven central, you need to supply the following information:
ThisBuild / publishMavenStyle := true

// Open-source license of your choice
ThisBuild / licenses := Seq("APL2" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt"))

// Where is the source code hosted: GitHub or GitLab?
import xerial.sbt.Sonatype._
ThisBuild / sonatypeProjectHosting := Some(GitHubHosting("debasishg", "effRedis", "dghosh@acm.org"))

ThisBuild / scmInfo := Some(
  ScmInfo(
    url("https://github.com/debasishg/effRedis"),
    "scm:git@github.com:debasishg/effRedis.git"
  )
)

ThisBuild / pomExtra := (
  <url>https://github.com/debasishg/effRedis</url>
)

ThisBuild / developers := List(
  Developer(id="debasishg", name="Debasish Ghosh", email="dghosh@acm.org", url=url("http://lightbend.com"))
)

// Remove all additional repository other than Maven Central from POM
ThisBuild / pomIncludeRepository := { _ => false }
ThisBuild / publishTo := sonatypePublishToBundle.value

ThisBuild / publishConfiguration := publishConfiguration.value.withOverwrite(true)
ThisBuild / publishLocalConfiguration := publishLocalConfiguration.value.withOverwrite(true)

