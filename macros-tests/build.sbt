organization := "app.softnetwork.elastic"

name := "softclient4es-macros-tests"

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % Versions.scalatest % Test
)

scalacOptions ++= Seq(
  "-language:experimental.macros",
  "-Ymacro-debug-lite"
)

Test / scalacOptions += "-Xlog-free-terms"
