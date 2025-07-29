import SoftClient4es.*
ThisBuild / elasticSearchVersion := Versions.es9
organization := "app.softnetwork.elastic"
name := s"softclient4es${elasticSearchMajorVersion(elasticSearchVersion.value)}"
publish / skip := true
Compile / sources := Nil
Test / sources := Nil

