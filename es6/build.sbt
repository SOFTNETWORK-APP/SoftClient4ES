import SoftClient4es.*
organization := "app.softnetwork.elastic"
name := s"softclient4es${elasticSearchMajorVersion(elasticSearchVersion.value)}"
publish / skip := true
Compile / sources := Nil
Test / sources := Nil

