organization := "app.softnetwork.elastic"

name := "softclient4es-macros"

libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-reflect" % scalaVersion.value,
  "org.json4s" %% "json4s-native" % Versions.json4s
)

scalacOptions ++= Seq(
  "-language:experimental.macros",
  "-Ymacro-debug-lite"              // Debug macros
)
