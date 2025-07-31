import SoftClient4es.*

organization := "app.softnetwork.elastic"

name := "softclient4es-core"

val configDependencies = Seq(
  "com.typesafe" % "config" % Versions.typesafeConfig
)

val json4s = Seq(
  "org.json4s" %% "json4s-jackson" % Versions.json4s,
  "org.json4s" %% "json4s-ext" % Versions.json4s
).map(_.excludeAll(jacksonExclusions *))

libraryDependencies ++= configDependencies ++
json4s :+ "com.google.code.gson" % "gson" % Versions.gson :+
("app.softnetwork.persistence" %% "persistence-core" % Versions.genericPersistence excludeAll (jacksonExclusions *))
