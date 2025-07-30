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

ThisBuild / version := "0.1.0"

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

lazy val es6bridge = project.in(file("es6/sql-bridge"))
  .configs(IntegrationTest)
  .settings(
    Defaults.itSettings,
    moduleSettings,
    elasticSearchVersion := Versions.es6,
  )
  .dependsOn(
    sql % "compile->compile;test->test;it->it"
  )

lazy val es6rest = project.in(file("es6/rest"))
  .configs(IntegrationTest)
  .settings(
    Defaults.itSettings,
    moduleSettings,
    elasticSearchVersion := Versions.es6,
  )
  .dependsOn(
    core % "compile->compile;test->test;it->it"
  )
  .dependsOn(
    es6bridge % "compile->compile;test->test;it->it"
  )

lazy val es6restp = project.in(file("es6/rest/persistence"))
  .configs(IntegrationTest)
  .settings(
    Defaults.itSettings,
    moduleSettings,
    elasticSearchVersion := Versions.es6,
  )
  .dependsOn(
    persistence % "compile->compile;test->test;it->it"
  )
  .dependsOn(
    es6rest % "compile->compile;test->test;it->it"
  )

lazy val es6jest = project.in(file("es6/jest"))
  .configs(IntegrationTest)
  .settings(
    Defaults.itSettings,
    moduleSettings,
    elasticSearchVersion := Versions.es6,
  )
  .dependsOn(
    core % "compile->compile;test->test;it->it"
  )
  .dependsOn(
    es6bridge % "compile->compile;test->test;it->it"
  )

lazy val es6jestp = project.in(file("es6/jest/persistence"))
  .configs(IntegrationTest)
  .settings(
    Defaults.itSettings,
    moduleSettings,
    elasticSearchVersion := Versions.es6,
  )
  .dependsOn(
    persistence % "compile->compile;test->test;it->it"
  )
  .dependsOn(
    es6jest % "compile->compile;test->test;it->it"
  )

lazy val es6testkit = project.in(file("es6/testkit"))
  .configs(IntegrationTest)
  .settings(
    Defaults.itSettings,
    app.softnetwork.Info.infoSettings,
    moduleSettings,
    elasticSearchVersion := Versions.es6,
    buildInfoKeys += BuildInfoKey("elasticVersion" -> elasticSearchVersion.value)
  )
  .enablePlugins(BuildInfoPlugin)
  .dependsOn(
    es6restp % "compile->compile;test->test;it->it"
  )
  .dependsOn(
    es6jestp % "compile->compile;test->test;it->it"
  )

lazy val es6 = project.in(file("es6"))
  .configs(IntegrationTest)
  .settings(
    Defaults.itSettings,
    Publish.noPublishSettings,
    crossScalaVersions := Nil,
    elasticSearchVersion := Versions.es6
  )
  .aggregate(
    es6bridge,
    es6rest,
    es6restp,
    es6jest,
    es6jestp,
    es6testkit
  )

lazy val es7bridge = project.in(file("sql/bridge"))
  .configs(IntegrationTest)
  .settings(
    Defaults.itSettings,
    moduleSettings,
    elasticSearchVersion := Versions.es7,
  )
  .dependsOn(
    sql % "compile->compile;test->test;it->it"
  )

lazy val es7rest = project.in(file("es7/rest"))
  .configs(IntegrationTest)
  .settings(
    Defaults.itSettings,
    moduleSettings,
    elasticSearchVersion := Versions.es7,
  )
  .dependsOn(
    core % "compile->compile;test->test;it->it"
  )
  .dependsOn(
    es7bridge % "compile->compile;test->test;it->it"
  )

lazy val es7restp = project.in(file("es7/rest/persistence"))
  .configs(IntegrationTest)
  .settings(
    Defaults.itSettings,
    moduleSettings,
    elasticSearchVersion := Versions.es7,
  )
  .dependsOn(
    persistence % "compile->compile;test->test;it->it"
  )
  .dependsOn(
    es7rest % "compile->compile;test->test;it->it"
  )

lazy val es7testkit = project.in(file("es7/testkit"))
  .configs(IntegrationTest)
  .settings(
    Defaults.itSettings,
    app.softnetwork.Info.infoSettings,
    moduleSettings,
    elasticSearchVersion := Versions.es7,
    buildInfoKeys += BuildInfoKey("elasticVersion" -> elasticSearchVersion.value)
  )
  .enablePlugins(BuildInfoPlugin)
  .dependsOn(
    es7restp % "compile->compile;test->test;it->it"
  )

lazy val es7 = project.in(file("es7"))
  .configs(IntegrationTest)
  .settings(
    Defaults.itSettings,
    Publish.noPublishSettings,
    crossScalaVersions := Nil,
    elasticSearchVersion := Versions.es7
  )
  .aggregate(
    es7bridge,
    es7rest,
    es7restp,
    es7testkit
  )

lazy val es8bridge = project.in(file("sql/bridge"))
  .configs(IntegrationTest)
  .settings(
    Defaults.itSettings,
    moduleSettings,
    elasticSearchVersion := Versions.es8,
  )
  .dependsOn(
    sql % "compile->compile;test->test;it->it"
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
  .dependsOn(
    es8bridge % "compile->compile;test->test;it->it"
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
    es8bridge,
    es8java,
    es8javap,
    es8testkit
  )

lazy val es9bridge = project.in(file("sql/bridge"))
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
    sql % "compile->compile;test->test;it->it"
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
  .dependsOn(
    es9bridge % "compile->compile;test->test;it->it"
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
    es9bridge,
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
    es6,
    es7,
    es8,
    es9
  )
