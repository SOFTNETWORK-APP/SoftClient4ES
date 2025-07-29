import SoftClient4es.*
import app.softnetwork.*

/////////////////////////////////
// Defaults
/////////////////////////////////

lazy val scala212 = "2.12.20"
lazy val scala213 = "2.13.16"
lazy val scalacCompilerOptions = Seq(
  "-deprecation",
  "-feature",
  "-target:jvm-1.8"
)

ThisBuild / organization := "app.softnetwork"

name := "softclient4es"

ThisBuild / version := "0.1-SNAPSHOT"

ThisBuild / scalaVersion := scala213

ThisBuild / dependencyOverrides ++= Seq(
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % Versions.jackson,
  "com.github.jnr" % "jnr-ffi" % "2.2.17",
  "com.github.jnr" % "jffi"    % "1.3.13" classifier "native",
  "org.lmdbjava" % "lmdbjava" % "0.9.1" exclude("org.slf4j", "slf4j-api"),
)

lazy val moduleSettings = Seq(
  crossScalaVersions := Seq(scala212, scala213),
  scalacOptions ++= {
    CrossVersion.partialVersion(scalaVersion.value) match {
      case Some((2, 12)) => scalacCompilerOptions :+ "-Ypartial-unification"
      case Some((2, 13)) => scalacCompilerOptions
      case _             => Seq.empty
    }
  }
)

ThisBuild / javacOptions ++= Seq("-source", "1.8", "-target", "1.8")

ThisBuild / resolvers ++= Seq(
  "Softnetwork Server" at "https://softnetwork.jfrog.io/artifactory/releases/",
  "Softnetwork Snapshots" at "https://softnetwork.jfrog.io/artifactory/snapshots/",
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

val json4s = Seq(
  "org.json4s" %% "json4s-jackson" % Versions.json4s,
  "org.json4s" %% "json4s-ext"     % Versions.json4s
).map(_.excludeAll(jacksonExclusions: _*))

ThisBuild / libraryDependencies ++= Seq(
  "org.scala-lang.modules" %% "scala-parser-combinators" % "1.1.2"
)// ++ configDependencies ++ json4s ++ logging

ThisBuild / libraryDependencySchemes += "org.scala-lang.modules" %% "scala-xml" % VersionScheme.Always

Test / parallelExecution := false

lazy val sql = project.in(file("sql"))
  .configs(IntegrationTest)
  .settings(
    Defaults.itSettings,
    moduleSettings,
  )

lazy val core = project.in(file("core"))
  .configs(IntegrationTest)
  .settings(
    Defaults.itSettings,
    moduleSettings
  )
  .dependsOn(
    sql % "compile->compile;test->test;it->it"
  )

lazy val persistence = project.in(file("persistence"))
  .configs(IntegrationTest)
  .settings(
    Defaults.itSettings,
    moduleSettings
  )
  .dependsOn(
    core % "compile->compile;test->test;it->it"
  )

lazy val es8java = project.in(file("es8/java"))
  .configs(IntegrationTest)
  .settings(
    Defaults.itSettings,
    moduleSettings,
    elasticSearchVersion := Versions.es8,
  )
  .dependsOn(
    core % "compile->compile;test->test;it->it"
  )

lazy val es8javap = project.in(file("es8/java/persistence"))
  .configs(IntegrationTest)
  .settings(
    Defaults.itSettings,
    moduleSettings,
    elasticSearchVersion := Versions.es8,
  )
  .dependsOn(
    persistence % "compile->compile;test->test;it->it"
  )
  .dependsOn(
    es8java % "compile->compile;test->test;it->it"
  )

lazy val es8testkit = project.in(file("es8/testkit"))
  .configs(IntegrationTest)
  .settings(
    Defaults.itSettings,
    app.softnetwork.Info.infoSettings,
    moduleSettings,
    elasticSearchVersion := Versions.es8,
    buildInfoKeys += BuildInfoKey("elasticVersion" -> elasticSearchVersion.value)
  )
  .enablePlugins(BuildInfoPlugin)
  .dependsOn(
    es8javap % "compile->compile;test->test;it->it"
  )

lazy val es8 = project.in(file("es8"))
  .configs(IntegrationTest)
  .settings(
    Defaults.itSettings,
    Publish.noPublishSettings,
    crossScalaVersions := Nil,
    elasticSearchVersion := Versions.es8
  )
  .aggregate(
    es8java,
    es8javap,
    es8testkit
  )

lazy val es9java = project.in(file("es9/java"))
  .configs(IntegrationTest)
  .settings(
    Defaults.itSettings,
    moduleSettings,
    scalaVersion := scala213,
    crossScalaVersions := Seq(scala213),
    elasticSearchVersion := Versions.es9,
    javacOptions ++= Seq("-source", "17", "-target", "17")
  )
  .dependsOn(
    core % "compile->compile;test->test;it->it"
  )

lazy val es9javap = project.in(file("es9/java/persistence"))
  .configs(IntegrationTest)
  .settings(
    Defaults.itSettings,
    moduleSettings,
    scalaVersion := scala213,
    crossScalaVersions := Seq(scala213),
    elasticSearchVersion := Versions.es9,
    javacOptions ++= Seq("-source", "17", "-target", "17")
  )
  .dependsOn(
    persistence % "compile->compile;test->test;it->it"
  )
  .dependsOn(
    es9java % "compile->compile;test->test;it->it"
  )

lazy val es9testkit = project.in(file("es9/testkit"))
  .configs(IntegrationTest)
  .settings(
    Defaults.itSettings,
    app.softnetwork.Info.infoSettings,
    moduleSettings,
    scalaVersion := scala213,
    crossScalaVersions := Seq(scala213),
    elasticSearchVersion := Versions.es9,
    javacOptions ++= Seq("-source", "17", "-target", "17"),
    buildInfoKeys += BuildInfoKey("elasticVersion" -> elasticSearchVersion.value)
  )
  .enablePlugins(BuildInfoPlugin)
  .dependsOn(
    es9javap % "compile->compile;test->test;it->it"
  )

lazy val es9 = project.in(file("es9"))
  .configs(IntegrationTest)
  .settings(
    Defaults.itSettings,
    Publish.noPublishSettings,
    crossScalaVersions := Nil,
    elasticSearchVersion := Versions.es9
  )
  .aggregate(
    es9java,
    es9javap,
    es9testkit
  )

lazy val root = project.in(file("."))
  .configs(IntegrationTest)
  .settings(
    Defaults.itSettings,
    Publish.noPublishSettings,
    crossScalaVersions := Nil
  )
  .aggregate(
    sql,
    core,
    persistence,
    es8,
    es9
  )
