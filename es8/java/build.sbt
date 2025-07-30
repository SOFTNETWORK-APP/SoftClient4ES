import SoftClient4es.*

organization := "app.softnetwork.elastic"

name := s"softclient4es${elasticSearchMajorVersion(elasticSearchVersion.value)}-java-client"

libraryDependencies ++= javaClientDependencies(elasticSearchVersion.value) ++
  elastic4sDependencies(elasticSearchVersion.value)
