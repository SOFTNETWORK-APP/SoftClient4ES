import SoftClient4es._

organization := "app.softnetwork.elastic"

name := "softclient4es-core-testkit"

libraryDependencies += "org.testcontainers" % "elasticsearch" % Versions.testContainers excludeAll (jacksonExclusions: _*)
