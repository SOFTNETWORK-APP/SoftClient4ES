import SoftClient4es.*

organization := "app.softnetwork.elastic"

name := s"softclient4es${elasticSearchMajorVersion(elasticSearchVersion.value)}-sql-bridge"

target := baseDirectory.value / s"target-es${elasticSearchMajorVersion(elasticSearchVersion.value)}"

libraryDependencies ++= elasticDependencies(elasticSearchVersion.value) ++
  elastic4sDependencies(elasticSearchVersion.value)
