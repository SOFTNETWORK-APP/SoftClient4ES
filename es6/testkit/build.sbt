import SoftClient4es.*

organization := "app.softnetwork.elastic"

name := s"softclient4es${elasticSearchMajorVersion(elasticSearchVersion.value)}-core-testkit"

libraryDependencies ++= elasticClientDependencies(elasticSearchVersion.value) ++
elastic4sTestkitDependencies(elasticSearchVersion.value) ++ Seq(
  "org.apache.logging.log4j" % "log4j-api" % Versions.log4j,
  //  "org.apache.logging.log4j" % "log4j-slf4j-impl"  % Versions.log4j,
  "org.apache.logging.log4j" % "log4j-core" % Versions.log4j,
  "app.softnetwork.persistence" %% "persistence-core-testkit" % Versions.genericPersistence,
  "org.testcontainers" % "testcontainers-elasticsearch" % Versions.testContainers excludeAll (jacksonExclusions *)
)
