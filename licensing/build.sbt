organization := "app.softnetwork.elastic"

name := "softclient4es-licensing"

libraryDependencies ++= Seq(
  "com.typesafe"                % "config"          % Versions.typesafeConfig,
  "com.typesafe.scala-logging" %% "scala-logging"   % Versions.scalaLogging,
  "org.scalatest"              %% "scalatest"       % Versions.scalatest % Test,
  // #258: the isolation specs capture the empty-provider-list WARN through logback's ListAppender
  // (house pattern: SlicedScrollCompletenessSpec). Test scope only - the published module carries
  // no logging backend. Test logging is configured by TestLoggingConfigurator (a logback
  // Configurator SPI, root WARN) - deliberately NOT a logback-test.xml, which would override the
  // persistence-core-testkit logback.xml on every es{N} test classpath via test->test.
  "ch.qos.logback"              % "logback-classic" % Versions.logback   % Test
)
