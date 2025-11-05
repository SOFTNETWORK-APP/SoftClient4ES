import SoftClient4es.*

organization := "app.softnetwork.elastic"

name := "softclient4es-core"

val akka = Seq(
  "com.typesafe.akka" %% "akka-actor" % Versions.akka,
  "com.typesafe.akka" %% "akka-cluster-sharding-typed" % Versions.akka,
  "com.typesafe.akka" %% "akka-slf4j" % Versions.akka,
  "com.typesafe.akka" %% "akka-discovery" % Versions.akka,
  "com.typesafe.akka" %% "akka-stream" % Versions.akka
)

val typesafeConfig = Seq(
  "com.typesafe" % "config" % Versions.typesafeConfig,
  "com.github.kxbmap" %% "configs" % Versions.kxbmap
)

val http = Seq(
  "org.apache.httpcomponents" % "httpcore" % "4.4.12" % "provided"
)

val json4s = Seq(
  "org.json4s" %% "json4s-jackson" % Versions.json4s,
  "org.json4s" %% "json4s-ext" % Versions.json4s
).map(_.excludeAll(jacksonExclusions *))

val mockito = Seq(
  "org.mockito" %% "mockito-scala" % "1.17.12" % Test
)

libraryDependencies ++= akka ++ typesafeConfig ++ http ++
json4s ++ mockito :+ "com.google.code.gson" % "gson" % Versions.gson :+
  "com.typesafe.scala-logging" %% "scala-logging" % Versions.scalaLogging
