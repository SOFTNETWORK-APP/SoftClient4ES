organization := "app.softnetwork.elastic"

name := "softclient4es-licensing"

libraryDependencies ++= Seq(
  "com.typesafe"                % "config"          % Versions.typesafeConfig,
  "com.typesafe.scala-logging" %% "scala-logging"   % Versions.scalaLogging,
  "org.scalatest"              %% "scalatest"       % Versions.scalatest % Test
)
