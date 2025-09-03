import SoftClient4es.*
import app.softnetwork.*
import sbt.Def
import sbtbuildinfo.BuildInfoKeys.buildInfoObject

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

ThisBuild / version := "0.4.0"

ThisBuild / scalaVersion := scala213

ThisBuild / dependencyOverrides ++= Seq(
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % Versions.jackson,
  "com.github.jnr" % "jnr-ffi" % "2.2.17",
  "com.github.jnr" % "jffi" % "1.3.13" classifier "native",
  "org.lmdbjava" % "lmdbjava" % "0.9.1" exclude ("org.slf4j", "slf4j-api")
)

lazy val moduleSettings = Seq(
  crossScalaVersions := Seq(scala212, scala213),
  scalacOptions ++= {
    CrossVersion.partialVersion(scalaVersion.value) match {
      case Some((2, 12)) => scalacCompilerOptions :+ "-Ypartial-unification"
      case Some((2, 13)) => scalacCompilerOptions
      case _             => Seq.empty
    }
  },
  dependencyOverrides ++= {
    CrossVersion.partialVersion(scalaVersion.value) match {
      case Some((2, 12)) => Seq("com.github.kxbmap" %% "configs" % Versions.kxbmap_scala2_12)
      case Some((2, 13)) => Seq("com.github.kxbmap" %% "configs" % Versions.kxbmap)
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
  "org.log4s" %% "log4s" % Versions.log4s,
  "org.slf4j" % "slf4j-api" % Versions.slf4j,
  "org.slf4j" % "jcl-over-slf4j" % Versions.slf4j,
  "org.slf4j" % "jul-to-slf4j" % Versions.slf4j
)

val json4s = Seq(
  "org.json4s" %% "json4s-jackson" % Versions.json4s,
  "org.json4s" %% "json4s-ext" % Versions.json4s
).map(_.excludeAll(jacksonExclusions: _*))

ThisBuild / libraryDependencies ++= Seq(
  "org.scala-lang.modules" %% "scala-parser-combinators" % "1.1.2"
) // ++ configDependencies ++ json4s ++ logging

ThisBuild / libraryDependencySchemes += "org.scala-lang.modules" %% "scala-xml" % VersionScheme.Always

Test / parallelExecution := false

lazy val sql = project
  .in(file("sql"))
  .configs(IntegrationTest)
  .settings(
    Defaults.itSettings,
    moduleSettings
  )

lazy val core = project
  .in(file("core"))
  .configs(IntegrationTest)
  .settings(
    Defaults.itSettings,
    moduleSettings
  )
  .dependsOn(
    sql % "compile->compile;test->test;it->it"
  )

def copyTestkit(esVersion: String): Def.Initialize[Task[Unit]] = Def.task {
  val src = file("core/testkit")
  val target = baseDirectory.value
  streams.value.log.info(
    s"Copying testkit template sources for ES ${elasticSearchMajorVersion(esVersion)}..."
  )
  IO.copyDirectory(src / "src", target / "src")
}

def testkitProject(esVersion: String, ss: Def.SettingsDefinition*): Project = {
  val projectId = s"softclient4es${elasticSearchMajorVersion(esVersion)}-core-testkit"
  Project(id = projectId, base = file(s"es${elasticSearchMajorVersion(esVersion)}/core/testkit"))
    .configs(IntegrationTest)
    .settings(
      Defaults.itSettings,
      app.softnetwork.Info.infoSettings,
      moduleSettings,
      elasticSearchVersion := esVersion,
      buildInfoKeys += BuildInfoKey("elasticVersion" -> elasticSearchVersion.value),
      buildInfoObject := "SoftClient4esCoreTestkitBuildInfo",
      organization := "app.softnetwork.elastic",
      name := projectId,
      libraryDependencies ++= elasticDependencies(elasticSearchVersion.value) ++
      elastic4sTestkitDependencies(elasticSearchVersion.value) ++ Seq(
        "org.apache.logging.log4j" % "log4j-api" % Versions.log4j,
        //  "org.apache.logging.log4j" % "log4j-slf4j-impl"  % Versions.log4j,
        "org.apache.logging.log4j" % "log4j-core" % Versions.log4j,
        "app.softnetwork.persistence" %% "persistence-core-testkit" % Versions.genericPersistence,
        "org.testcontainers" % "elasticsearch" % Versions.testContainers excludeAll (jacksonExclusions: _*)
      ),
      Compile / compile := (Compile / compile).dependsOn(copyTestkit(esVersion)).value
    )
    .settings(ss: _*)
    .enablePlugins(BuildInfoPlugin)
    .dependsOn(
      core % "compile->compile;test->test;it->it"
    )
}

def copyBridge(esVersion: String): Def.Initialize[Task[Unit]] = Def.task {
  val src = file("sql/bridge")
  val target = baseDirectory.value
  streams.value.log.info(
    s"Copying bridge template sources for ES ${elasticSearchMajorVersion(esVersion)}..."
  )
  IO.copyDirectory(src / "src", target / "src")
}

def bridgeProject(esVersion: String, ss: Def.SettingsDefinition*): Project = {
  val projectId = s"softclient4es${elasticSearchMajorVersion(esVersion)}-sql-bridge"
  Project(id = projectId, base = file(s"es${elasticSearchMajorVersion(esVersion)}/sql/bridge"))
    .configs(IntegrationTest)
    .settings(
      Defaults.itSettings,
      moduleSettings,
      elasticSearchVersion := esVersion,
      organization := "app.softnetwork.elastic",
      name := projectId,
      libraryDependencies ++= elasticDependencies(elasticSearchVersion.value) ++
      elastic4sDependencies(elasticSearchVersion.value),
      Compile / compile := (Compile / compile).dependsOn(copyBridge(esVersion)).value
    )
    .settings(ss: _*)
    .dependsOn(
      sql % "compile->compile;test->test;it->it"
    )
}

lazy val es6bridge = project
  .in(file("es6/sql-bridge"))
  .configs(IntegrationTest)
  .settings(
    Defaults.itSettings,
    moduleSettings,
    elasticSearchVersion := Versions.es6
  )
  .dependsOn(
    sql % "compile->compile;test->test;it->it"
  )

lazy val es6testkit = testkitProject(Versions.es6)

lazy val es6embeddedtestkit = project
  .in(file("es6/testkit"))
  .configs(IntegrationTest)
  .settings(
    Defaults.itSettings,
    moduleSettings,
    elasticSearchVersion := Versions.es6
  )
  .dependsOn(
    es6testkit % "compile->compile;test->test;it->it"
  )

lazy val es6rest = project
  .in(file("es6/rest"))
  .configs(IntegrationTest)
  .settings(
    Defaults.itSettings,
    moduleSettings,
    elasticSearchVersion := Versions.es6
  )
  .dependsOn(
    core % "compile->compile;test->test;it->it"
  )
  .dependsOn(
    es6bridge % "compile->compile;test->test;it->it"
  )
  .dependsOn(
    es6embeddedtestkit % "test->test;it->it"
  )

lazy val es6jest = project
  .in(file("es6/jest"))
  .configs(IntegrationTest)
  .settings(
    Defaults.itSettings,
    moduleSettings,
    elasticSearchVersion := Versions.es6
  )
  .dependsOn(
    core % "compile->compile;test->test;it->it"
  )
  .dependsOn(
    es6bridge % "compile->compile;test->test;it->it"
  )
  .dependsOn(
    es6embeddedtestkit % "test->test;it->it"
  )

lazy val es6 = project
  .in(file("es6"))
  .configs(IntegrationTest)
  .settings(
    Defaults.itSettings,
    Publish.noPublishSettings,
    crossScalaVersions := Nil,
    elasticSearchVersion := Versions.es6
  )
  .aggregate(
    es6bridge,
    es6testkit,
    es6embeddedtestkit,
    es6rest,
    es6jest
  )

lazy val es7bridge = bridgeProject(Versions.es7)

lazy val es7testkit = testkitProject(Versions.es7)

lazy val es7rest = project
  .in(file("es7/rest"))
  .configs(IntegrationTest)
  .settings(
    Defaults.itSettings,
    moduleSettings,
    elasticSearchVersion := Versions.es7
  )
  .dependsOn(
    core % "compile->compile;test->test;it->it"
  )
  .dependsOn(
    es7bridge % "compile->compile;test->test;it->it"
  )
  .dependsOn(
    es7testkit % "test->test;it->it"
  )

lazy val es7 = project
  .in(file("es7"))
  .configs(IntegrationTest)
  .settings(
    Defaults.itSettings,
    Publish.noPublishSettings,
    crossScalaVersions := Nil,
    elasticSearchVersion := Versions.es7
  )
  .aggregate(
    es7bridge,
    es7testkit,
    es7rest
  )

lazy val es8bridge = bridgeProject(Versions.es8)

lazy val es8testkit = testkitProject(Versions.es8)

lazy val es8java = project
  .in(file("es8/java"))
  .configs(IntegrationTest)
  .settings(
    Defaults.itSettings,
    moduleSettings,
    elasticSearchVersion := Versions.es8
  )
  .dependsOn(
    core % "compile->compile;test->test;it->it"
  )
  .dependsOn(
    es8bridge % "compile->compile;test->test;it->it"
  )
  .dependsOn(
    es8testkit % "test->test;it->it"
  )

lazy val es8 = project
  .in(file("es8"))
  .configs(IntegrationTest)
  .settings(
    Defaults.itSettings,
    Publish.noPublishSettings,
    crossScalaVersions := Nil,
    elasticSearchVersion := Versions.es8
  )
  .aggregate(
    es8bridge,
    es8testkit,
    es8java
  )

lazy val es9bridge = bridgeProject(
  Versions.es9,
  scalaVersion := scala213,
  crossScalaVersions := Seq(scala213),
  javacOptions ++= Seq("-source", "17", "-target", "17")
)

lazy val es9testkit = testkitProject(
  Versions.es9,
  scalaVersion := scala213,
  crossScalaVersions := Seq(scala213),
  javacOptions ++= Seq("-source", "17", "-target", "17")
)

lazy val es9java = project
  .in(file("es9/java"))
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
  .dependsOn(
    es9testkit % "test->test;it->it"
  )

lazy val es9 = project
  .in(file("es9"))
  .configs(IntegrationTest)
  .settings(
    Defaults.itSettings,
    Publish.noPublishSettings,
    crossScalaVersions := Nil,
    elasticSearchVersion := Versions.es9
  )
  .aggregate(
    es9bridge,
    es9testkit,
    es9java
  )

lazy val root = project
  .in(file("."))
  .configs(IntegrationTest)
  .settings(
    Defaults.itSettings,
    Publish.noPublishSettings,
    crossScalaVersions := Nil
  )
  .aggregate(
    sql,
    core,
    es6,
    es7,
    es8,
    es9
  )
