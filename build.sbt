import app.softnetwork.*

/////////////////////////////////
// Defaults
/////////////////////////////////

ThisBuild / organization := "app.softnetwork"

name := "elastic"

ThisBuild / version := Versions.elasticSearch

ThisBuild / scalaVersion := "2.12.18"

ThisBuild / scalacOptions ++= Seq("-deprecation", "-feature", "-target:jvm-1.8", "-Ypartial-unification")

ThisBuild / javacOptions ++= Seq("-source", "1.8", "-target", "1.8")

ThisBuild / resolvers ++= Seq(
  "Softnetwork Server" at "https://softnetwork.jfrog.io/artifactory/releases/",
  "Maven Central Server" at "https://repo1.maven.org/maven2",
  "Typesafe Server" at "https://repo.typesafe.com/typesafe/releases"
)

val logging = Seq(
  "com.typesafe.scala-logging" %% "scala-logging" % Versions.scalaLogging,
  "org.log4s"                  %% "log4s"         % Versions.log4s,
  "org.slf4j"                  % "slf4j-api"      % Versions.slf4j,
  "org.slf4j"                  % "jcl-over-slf4j" % Versions.slf4j,
  "org.slf4j"                  % "jul-to-slf4j"   % Versions.slf4j
)

val jacksonExclusions = Seq(
  ExclusionRule(organization = "com.fasterxml.jackson.core"),
  ExclusionRule(organization = "org.codehaus.jackson")
)

val json4s = Seq(
  "org.json4s" %% "json4s-jackson" % Versions.json4s,
  "org.json4s" %% "json4s-ext"     % Versions.json4s
).map(_.excludeAll(jacksonExclusions: _*))

ThisBuild / libraryDependencies ++= Seq(
  "org.scala-lang.modules" %% "scala-parser-combinators" % "1.1.1"
)// ++ configDependencies ++ json4s ++ logging

ThisBuild / libraryDependencySchemes += "org.scala-lang.modules" %% "scala-xml" % VersionScheme.Always

Test / parallelExecution := false

lazy val sql = project.in(file("sql"))
  .configs(IntegrationTest)
  .settings(Defaults.itSettings)

lazy val client = project.in(file("client"))
  .configs(IntegrationTest)
  .settings(Defaults.itSettings)
  .dependsOn(
    sql % "compile->compile;test->test;it->it"
  )

lazy val persistence = project.in(file("persistence"))
  .configs(IntegrationTest)
  .settings(Defaults.itSettings)
  .dependsOn(
    client % "compile->compile;test->test;it->it"
  )

lazy val rest = project.in(file("rest"))
  .configs(IntegrationTest)
  .settings(Defaults.itSettings)
  .dependsOn(
    client % "compile->compile;test->test;it->it"
  )

lazy val restPersistence = project.in(file("rest/persistence"))
  .configs(IntegrationTest)
  .settings(Defaults.itSettings)
  .dependsOn(
    rest % "compile->compile;test->test;it->it",
  )
  .dependsOn(
    persistence % "compile->compile;test->test;it->it"
  )

lazy val jest = project.in(file("jest"))
  .configs(IntegrationTest)
  .settings(Defaults.itSettings)
  .dependsOn(
    client % "compile->compile;test->test;it->it"
  )

lazy val jestPersistence = project.in(file("jest/persistence"))
  .configs(IntegrationTest)
  .settings(Defaults.itSettings)
  .dependsOn(
    jest % "compile->compile;test->test;it->it",
  )
  .dependsOn(
    persistence % "compile->compile;test->test;it->it"
  )

lazy val testKit = project.in(file("testkit"))
  .configs(IntegrationTest)
  .settings(
    Defaults.itSettings,
    app.softnetwork.Info.infoSettings
  )
  .enablePlugins(BuildInfoPlugin)
  .dependsOn(
    restPersistence % "compile->compile;test->test;it->it"
  )
  .dependsOn(
    jestPersistence % "compile->compile;test->test;it->it"
  )

lazy val root = project.in(file("."))
  .configs(IntegrationTest)
  .settings(Defaults.itSettings, Publish.noPublishSettings)
  .aggregate(sql, client, rest, jest, persistence, restPersistence, jestPersistence, testKit)
