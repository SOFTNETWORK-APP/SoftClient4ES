import SoftClient4es.*

organization := "app.softnetwork.elastic"

name := "softclient4es-persistence"

libraryDependencies ++= Seq(
  "app.softnetwork.persistence" %% "persistence-core" % Versions.genericPersistence excludeAll (jacksonExclusions *)
)