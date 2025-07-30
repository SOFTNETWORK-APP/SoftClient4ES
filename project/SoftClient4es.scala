import sbt.*

trait SoftClient4es {

  lazy val elasticSearchVersion =
    settingKey[String]("The version of Elasticsearch used for this module")

  def elasticSearchMajorVersion(esVersion: String): Int = esVersion.split("\\.").head.toInt

  lazy val jacksonExclusions: Seq[ExclusionRule] = Seq(
    ExclusionRule(organization = "com.fasterxml.jackson.core"),
    ExclusionRule(organization = "com.fasterxml.jackson.dataformat"),
    ExclusionRule(organization = "com.fasterxml.jackson.datatype"),
    ExclusionRule(organization = "com.fasterxml.jackson.module"),
    ExclusionRule(organization = "org.codehaus.jackson")
  )

  def jacksonDependencies(esVersion: String): Seq[ModuleID] = {
    val jackson2_19 = "2.19.0"
    val jackson2_13 = "2.13.3"
    (elasticSearchMajorVersion(esVersion) match {
      case 6 | 7 =>
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
      case 7 =>
        Seq(
          "com.sksamuel.elastic4s" %% "elastic4s-core" % Versions.elastic74s exclude ("org.elasticsearch", "elasticsearch") exclude ("org.slf4j", "slf4j-api")
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
      case 7 | 8 | 9 =>
        Seq(
          "org.elasticsearch" % "elasticsearch" % esVersion exclude ("org.apache.logging.log4j", "log4j-api") exclude ("org.slf4j", "slf4j-api") excludeAll (jacksonExclusions: _*),
        ).map(_.excludeAll(jacksonExclusions: _*))
      case _ => Seq.empty
    }
  }

  def elasticClientDependencies(esVersion: String): Seq[ModuleID] = {
    elasticDependencies(esVersion) ++
      (elasticSearchMajorVersion(esVersion) match {
      case 7 | 8 | 9 =>
        Seq(
          "org.elasticsearch.client" % "elasticsearch-rest-client" % esVersion
        ).map(_.excludeAll(jacksonExclusions: _*))
      case _ => Seq.empty
    })
  }

  def javaClientDependencies(esVersion: String): Seq[ModuleID] = {
    elasticClientDependencies(esVersion) ++
    (elasticSearchMajorVersion(esVersion) match {
      case 8 | 9 =>
        Seq(
          "co.elastic.clients" % "elasticsearch-java" % esVersion exclude ("org.elasticsearch", "elasticsearch")
        ).map(_.excludeAll(jacksonExclusions: _*))
      case _ => Seq.empty
    })
  }

  def restClientDependencies(esVersion: String): Seq[ModuleID] = {
    elasticClientDependencies(esVersion) ++
    (elasticSearchMajorVersion(esVersion) match {
      case 6 | 7 =>
        Seq(
          "org.elasticsearch.client" % "elasticsearch-rest-high-level-client" % esVersion exclude ("org.elasticsearch", "elasticsearch")
        ).map(_.excludeAll(jacksonExclusions: _*))
      case _ => Seq.empty
    })
  }

}

object SoftClient4es extends SoftClient4es
