import SoftClient4es.*

organization := "app.softnetwork.elastic"

name := s"softclient4es${elasticSearchMajorVersion(elasticSearchVersion.value)}-rest-client"

libraryDependencies ++= restClientDependencies(elasticSearchVersion.value) ++
  elastic4sDependencies(elasticSearchVersion.value)
