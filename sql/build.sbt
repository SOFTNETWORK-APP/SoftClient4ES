import SoftClient4es.*
import sbt.Keys.scalaVersion

organization := "app.softnetwork.elastic"

elasticSearchVersion := Versions.es9

name := s"softclient4es-sql"

val typesafeConfig = Seq(
  "com.typesafe" % "config" % Versions.typesafeConfig,
  "com.github.kxbmap" %% "configs" % Versions.kxbmap
)

val scalatest = Seq(
  "org.scalatest" %% "scalatest" % Versions.scalatest  % Test
)

libraryDependencies ++= jacksonDependencies(elasticSearchVersion.value) ++
//  elastic4sDependencies(elasticSearchVersion.value) ++
  typesafeConfig ++
  scalatest ++
  Seq(
    "javax.activation" % "activation" % "1.1.1" % Test
  ) :+
  "com.typesafe.scala-logging" %% "scala-logging" % Versions.scalaLogging :+
  "com.github.alonsodomin.cron4s" %% "cron4s-core" % Versions.cron4s :+
  "org.scala-lang" % "scala-reflect" % scalaVersion.value :+
  "com.google.code.gson" % "gson" % Versions.gson % Test
