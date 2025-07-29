package app.softnetwork.elastic.model

import app.softnetwork.persistence.model.Timestamped

import java.time.Instant

/** Created by smanciot on 12/04/2020.
  */
case class Sample(
  uuid: String,
  var createdDate: Instant = Instant.now(),
  var lastUpdated: Instant = Instant.now()
) extends Timestamped
