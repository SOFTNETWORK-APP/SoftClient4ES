object Versions {

  val akka = "2.6.20" // TODO 2.6.20 -> 2.8.3

  val scalatest = "3.2.19"

  val typesafeConfig = "1.4.3"

  val kxbmap_scala2_12 = "0.4.4"

  val kxbmap = "0.6.1"

  val jackson = "2.19.0" // 2.13.3 -> 2.19.0

  val json4s = "4.0.7" // 4.0.6 -> 4.0.7

  val scalaLogging = "3.9.2"

  val logback = "1.5.32"

  val slf4j = "1.7.36"

  val log4s = "1.8.2"

  val es6 = "6.8.23"

  val elastic64s = "6.7.8"

  val jest = "6.3.1"

  val es7 = "7.17.29"

  val elastic74s = "7.17.4"

  val es8 = "8.18.3"

  val elastic84s = "8.18.2"

  val es9 = "9.0.3"

  val elastic94s = "9.0.0"

  // Fallback for an Elasticsearch major with no entry in SoftClient4es.log4jVersion — the ONLY
  // consumer of this val is log4jVersion's `case _` arm. Client closures do NOT use it: they
  // inherit log4j-api transitively from `org.elasticsearch:elasticsearch`, whose own POM pins the
  // version (2.17.1 on ES 6/7, 2.19.0 on ES 8/9) — see elasticDependencies (#168 / jdbc#33 /
  // arrow#167). The core-testkits' deliberate log4j implementation goes through log4jVersion.
  val log4j = "2.17.1"

  val testContainers = "2.0.2"

  val genericPersistence = "0.9.0"

  val gson = "2.8.9"

  val delta = "3.3.2"

  val cron4s = "0.8.2"

  val jline = "3.30.6"

  val fansi = "0.5.1"

  // Cloud storage connectors (provided scope — must be on classpath at runtime)
  val hadoop = "3.4.3" // must match hadoop-client in core/build.sbt

  val gcsConnector = "hadoop3-2.2.24"
}
