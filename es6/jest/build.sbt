import SoftClient4es.*

organization := "app.softnetwork.elastic"

name := s"softclient4es${elasticSearchMajorVersion(elasticSearchVersion.value)}-jest-client"

libraryDependencies ++= jestClientDependencies(elasticSearchVersion.value) ++
  elastic4sDependencies(elasticSearchVersion.value)
