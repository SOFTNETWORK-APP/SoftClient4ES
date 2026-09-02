import SoftClient4es.*

organization := "app.softnetwork.elastic"

name := s"softclient4es${elasticSearchMajorVersion(elasticSearchVersion.value)}-core-testkit"

target := baseDirectory.value / s"target-es${elasticSearchMajorVersion(elasticSearchVersion.value)}"

libraryDependencies ++= elasticClientDependencies(elasticSearchVersion.value) ++ Seq(
  // AD-3 / #168 — see build.sbt's testkitProject: the testkit is the one artifact class that
  // deliberately carries a log4j implementation; pin it to the version THIS ES major declares
  // (log4jVersion). log4j-api is NOT re-declared — it arrives transitively from
  // `org.elasticsearch:elasticsearch` at that same version. Keep in step with testkitProject.
  "org.apache.logging.log4j" % "log4j-core" % log4jVersion(elasticSearchVersion.value),
  //  "org.apache.logging.log4j" % "log4j-slf4j-impl"  % log4jVersion(elasticSearchVersion.value),
  "app.softnetwork.persistence" %% "persistence-core-testkit" % Versions.genericPersistence,
  "org.testcontainers" % "testcontainers-elasticsearch" % Versions.testContainers excludeAll (jacksonExclusions: _*),
  "org.testcontainers" % "testcontainers-minio"         % Versions.testContainers
)
