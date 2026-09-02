import SoftClient4es.*

organization := "app.softnetwork.elastic"

name := s"softclient4es${elasticSearchMajorVersion(elasticSearchVersion.value)}-core-testkit"

libraryDependencies ++= elasticClientDependencies(elasticSearchVersion.value) ++
elastic4sTestkitDependencies(elasticSearchVersion.value) ++ Seq(
  // AD-3 / #168: no log4j lines here — the API arrives transitively from
  // `org.elasticsearch:elasticsearch`, and THIS project's deliberate log4j-core impl is declared
  // once, by build.sbt's testkitProject (at log4jVersion) — these are EXTRA settings appended to
  // softclient4es6-core-testkit. Re-declaring either half here is exactly how the api/impl pair
  // drifts. (The separate template project, testkit/build.sbt, declares its own — it does not go
  // through testkitProject.)
  "app.softnetwork.persistence" %% "persistence-core-testkit" % Versions.genericPersistence,
  "org.testcontainers" % "testcontainers-elasticsearch" % Versions.testContainers excludeAll (jacksonExclusions *)
)
