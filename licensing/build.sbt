organization := "app.softnetwork.elastic"

name := "softclient4es-licensing"

libraryDependencies ++= Seq(
  "com.nimbusds"                % "nimbus-jose-jwt" % Versions.nimbusJoseJwt,
  "org.bouncycastle"            % "bcprov-jdk18on"  % Versions.bouncyCastle,
  "com.google.crypto.tink"      % "tink"            % Versions.tink,
  "com.typesafe"                % "config"          % Versions.typesafeConfig,
  "com.typesafe.scala-logging" %% "scala-logging"   % Versions.scalaLogging,
  "com.fasterxml.jackson.core"  % "jackson-databind" % Versions.jackson,
  "org.scalatest"              %% "scalatest"       % Versions.scalatest % Test
)

