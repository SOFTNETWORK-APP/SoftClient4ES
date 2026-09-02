import sbt.*

trait SoftClient4es {

  lazy val elasticSearchVersion =
    settingKey[String]("The version of Elasticsearch used for this module")

  def elasticSearchMajorVersion(esVersion: String): Int = esVersion.split("\\.").head.toInt

  /** The log4j version the given Elasticsearch major's own POM declares for log4j-api / log4j-core.
    *
    * The source of truth is `org.elasticsearch:elasticsearch:<esVersion>`'s POM — re-read it
    * whenever `Versions.es{6,7,8,9}` moves. Client closures never use this (they inherit the API
    * transitively — see `elasticDependencies`); it exists so the ONE artifact class that
    * deliberately ships a log4j implementation, `softclient4es{N}-core-testkit`, cannot drift away
    * from the API version its Elasticsearch supplies.
    */
  def log4jVersion(esVersion: String): String =
    elasticSearchMajorVersion(esVersion) match {
      case 6 | 7 => "2.17.1" // ES 6.8.23 / 7.17.29 declare 2.17.1
      case 8 | 9 => "2.19.0" // ES 8.18.3 / 9.0.3 declare 2.19.0
      case _     => Versions.log4j
    }

  lazy val jacksonExclusions: Seq[ExclusionRule] = Seq(
    ExclusionRule(organization = "com.fasterxml.jackson.core"),
    ExclusionRule(organization = "com.fasterxml.jackson.dataformat"),
    ExclusionRule(organization = "com.fasterxml.jackson.datatype"),
    ExclusionRule(organization = "com.fasterxml.jackson.module"),
    ExclusionRule(organization = "org.codehaus.jackson")
  )

  lazy val guavaExclusion = ExclusionRule(organization = "com.google.guava", name = "guava")

  lazy val httpComponentsExclusions: Seq[ExclusionRule] = Seq(
    ExclusionRule(
      organization = "org.apache.httpcomponents",
      name = "httpclient",
      artifact = "*",
      configurations = Vector(ConfigRef("test")),
      crossVersion = CrossVersion.disabled
    )
  )

  lazy val excludeSlf4jAndLog4j: Seq[ExclusionRule] = Seq(
    ExclusionRule(organization = "org.slf4j", name = "slf4j-log4j12"),
    ExclusionRule(organization = "org.slf4j", name = "slf4j-reload4j"),
    ExclusionRule(organization = "log4j", name = "log4j"),
    // ch.qos.reload4j is the maintained log4j 1.x fork Hadoop 3.4 actually logs through — a helper
    // named "exclude log4j" that lets the drop-in fork through ships a logging backend anyway.
    // Found by checkLog4jClosure on its first run (story 20.2): hadoop-client → hadoop-auth/-common
    // delivered reload4j-1.2.22.jar onto EVERY client closure despite this excludeAll.
    ExclusionRule(organization = "ch.qos.reload4j"),
    ExclusionRule(organization = "org.apache.logging.log4j")
  )

  def jacksonDependencies(esVersion: String): Seq[ModuleID] = {
    val jackson2_19 = "2.19.0"
    val jackson2_13 = "2.13.3"
    val jackson2_12 = "2.12.7"
    (elasticSearchMajorVersion(esVersion) match {
      case 6 =>
        Some(jackson2_12)
      case 7 =>
        Some(jackson2_13)
      case 8 | 9 =>
        Some(jackson2_19)
      case _ => None
    }) match {
      case Some(version) =>
        Seq(
          "com.fasterxml.jackson.core" % "jackson-databind" % version,
          "com.fasterxml.jackson.core" % "jackson-core" % version,
          "com.fasterxml.jackson.core" % "jackson-annotations" % version,
          "com.fasterxml.jackson.dataformat" % "jackson-dataformat-cbor" % version,
          "com.fasterxml.jackson.dataformat" % "jackson-dataformat-yaml" % version,
          "com.fasterxml.jackson.datatype" % "jackson-datatype-jdk8" % version,
          "com.fasterxml.jackson.datatype" % "jackson-datatype-jsr310" % version,
          "com.fasterxml.jackson.module" % "jackson-module-parameter-names" % version,
          "com.fasterxml.jackson.module" %% "jackson-module-scala" % version
        )
      case None => Seq.empty
    }
  }

  def elastic4sDependencies(esVersion: String): Seq[ModuleID] = {
    elasticSearchMajorVersion(esVersion) match {
      case 6 =>
        Seq(
          "com.sksamuel.elastic4s" %% "elastic4s-core" % Versions.elastic64s exclude ("org.elasticsearch", "elasticsearch") exclude ("org.slf4j", "slf4j-api"),
          "com.sksamuel.elastic4s" %% "elastic4s-http" % Versions.elastic64s exclude ("org.elasticsearch", "elasticsearch")
          // (#168) The explicit log4j-api re-add that used to live here is gone: elasticDependencies
          // no longer excludes log4j-api from `org.elasticsearch:elasticsearch`, so every major gets
          // the API transitively at the version its own ES release declares. NB the comment this
          // replaces blamed `excludeSlf4jAndLog4j` — that helper is never applied to an ES
          // dependency (only to parquet/avro/hadoop/gcs in core/build.sbt); the culprit was always
          // the log4j-api exclusion in elasticDependencies.
        )
      case 7 =>
        Seq(
          "com.sksamuel.elastic4s" %% "elastic4s-core" % Versions.elastic74s exclude ("org.elasticsearch", "elasticsearch") exclude ("org.slf4j", "slf4j-api")
          // (#168 / jdbc#33 / arrow#167) log4j-api arrives transitively — see elasticDependencies.
        )
      case 8 =>
        Seq(
          "nl.gn0s1s" %% "elastic4s-core" % Versions.elastic84s exclude ("org.elasticsearch", "elasticsearch") exclude ("org.slf4j", "slf4j-api")
        )
      case 9 =>
        Seq(
          "nl.gn0s1s" %% "elastic4s-core" % Versions.elastic94s exclude ("org.elasticsearch", "elasticsearch") exclude ("org.slf4j", "slf4j-api")
        )
      case _ => Seq.empty
    }
  }

  def elastic4sTestkitDependencies(esVersion: String): Seq[ModuleID] = {
    elastic4sDependencies(esVersion) ++
    (elasticSearchMajorVersion(esVersion) match {
      case 6 =>
        Seq(
          "com.sksamuel.elastic4s" %% "elastic4s-testkit" % Versions.elastic64s exclude ("org.elasticsearch", "elasticsearch") exclude ("org.slf4j", "slf4j-api"),
          "com.sksamuel.elastic4s" %% "elastic4s-embedded" % Versions.elastic64s exclude ("org.elasticsearch", "elasticsearch"),
          "pl.allegro.tech" % "embedded-elasticsearch" % "2.10.0" excludeAll (jacksonExclusions *)
        )
      case 7 =>
        Seq(
          "com.sksamuel.elastic4s" %% "elastic4s-testkit" % Versions.elastic74s exclude ("org.elasticsearch", "elasticsearch") exclude ("org.slf4j", "slf4j-api")
        )
      case 8 =>
        Seq(
          "nl.gn0s1s" %% "elastic4s-testkit" % Versions.elastic84s exclude ("org.elasticsearch", "elasticsearch") exclude ("org.slf4j", "slf4j-api")
        )
      case 9 =>
        Seq(
          "nl.gn0s1s" %% "elastic4s-testkit" % Versions.elastic94s exclude ("org.elasticsearch", "elasticsearch") exclude ("org.slf4j", "slf4j-api")
        )
      case _ => Seq.empty
    })
  }

  def elasticDependencies(esVersion: String): Seq[ModuleID] = {
    elasticSearchMajorVersion(esVersion) match {
      case 6 | 7 | 8 | 9 =>
        Seq(
          // ONE RULE, ALL FOUR MAJORS (#168 / jdbc#33 / arrow#167): an Elasticsearch client closure
          // carries the log4j *API* and never a log4j *implementation*.
          //   - The API is REQUIRED: Elasticsearch's own classes hard-reference
          //     org.apache.logging.log4j.LogManager in <clinit>. Excluding it — which this line used
          //     to do — is what made the es6 REPL (#168) and then the es8/es9 drivers (jdbc#33,
          //     arrow#167) die with NoClassDefFoundError. Its version is NOT ours to pick: it is
          //     whatever `org.elasticsearch:elasticsearch:<esVersion>` declares (2.17.1 on ES 6.8 /
          //     7.17, 2.19.0 on ES 8.18 / 9.0), so it is right by construction on the next ES bump.
          //   - The IMPLEMENTATION is excluded: nothing on a client path needs a logging backend,
          //     and shipping one inside an in-process JDBC/ADBC driver installs a log4j provider in
          //     the HOST's JVM (the es8 fat jar shipped log4j-core plus a dangling
          //     META-INF/services/org.apache.logging.log4j.spi.Provider — Tableau error 1CA83880).
          //     log4j2-ecs-layout is a log4j-core plugin and goes with it — and so does
          //     ecs-logging-core, which ES 8/9 declare as a SEPARATE direct dependency (it exists
          //     only to serve the layout; with the layout gone it is dead weight in every fat jar).
          // log4j-api with no provider falls back to SimpleLogger — exactly what ES 6 has always
          // shipped (its log4j-core is <optional>true</optional> upstream and never resolved).
          // Guarded by `checkLog4jClosure` (build.sbt) — a dependencyTree read is not evidence.
          "org.elasticsearch" % "elasticsearch" % esVersion exclude ("org.apache.logging.log4j", "log4j-core") exclude ("co.elastic.logging", "log4j2-ecs-layout") exclude ("co.elastic.logging", "ecs-logging-core") exclude ("org.slf4j", "slf4j-api") excludeAll (jacksonExclusions *)
        ).map(_.excludeAll(jacksonExclusions *))
      case _ => Seq.empty
    }
  }

  def elasticClientDependencies(esVersion: String): Seq[ModuleID] = {
    elasticDependencies(esVersion) ++
    (elasticSearchMajorVersion(esVersion) match {
      case 6 | 7 | 8 | 9 =>
        Seq(
          "org.elasticsearch.client" % "elasticsearch-rest-client" % esVersion
        ).map(_.excludeAll(jacksonExclusions *))
      case _ => Seq.empty
    })
  }

  def javaClientDependencies(esVersion: String): Seq[ModuleID] = {
    elasticClientDependencies(esVersion) ++
    (elasticSearchMajorVersion(esVersion) match {
      case 8 | 9 =>
        Seq(
          "co.elastic.clients" % "elasticsearch-java" % esVersion exclude ("org.elasticsearch", "elasticsearch")
        ).map(_.excludeAll(jacksonExclusions *))
      case _ => Seq.empty
    })
  }

  def restClientDependencies(esVersion: String): Seq[ModuleID] = {
    elasticClientDependencies(esVersion) ++
    (elasticSearchMajorVersion(esVersion) match {
      case 6 | 7 =>
        Seq(
          "org.elasticsearch.client" % "elasticsearch-rest-high-level-client" % esVersion exclude ("org.elasticsearch", "elasticsearch")
        ).map(_.excludeAll(jacksonExclusions *))
      case _ => Seq.empty
    })
  }

  def jestClientDependencies(esVersion: String): Seq[ModuleID] = {
    elasticClientDependencies(esVersion) ++
    (elasticSearchMajorVersion(esVersion) match {
      case 6 =>
        Seq(
          "io.searchbox" % "jest" % Versions.jest,
          "com.google.guava" % "guava" % "33.5.0-jre"
        ).map(_.excludeAll(httpComponentsExclusions /*++ Seq(guavaExclusion)*/ *))
      case _ => Seq.empty
    })
  }

}

object SoftClient4es extends SoftClient4es
