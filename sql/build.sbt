import SoftClient4es._

organization := "app.softnetwork.elastic"

elasticSearchVersion := Versions.es9

name := s"softclient4es-sql"

val scalatest = Seq(
  "org.scalatest" %% "scalatest" % Versions.scalatest  % Test
)

libraryDependencies ++= jacksonDependencies(elasticSearchVersion.value) ++
//  elastic4sDependencies(elasticSearchVersion.value) ++
  scalatest ++
  Seq(
    "javax.activation" % "activation" % "1.1.1" % Test
  ) :+
//  ("app.softnetwork.persistence" %% "persistence-core" % Versions.genericPersistence excludeAll(jacksonExclusions: _*)) :+
  "org.scala-lang" % "scala-reflect" % "2.13.16" :+
  "com.google.code.gson" % "gson" % Versions.gson % Test


