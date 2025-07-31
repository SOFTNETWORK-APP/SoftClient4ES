package app.softnetwork.elastic.model

import app.softnetwork.persistence.model.Timestamped

import java.time.Instant

case class Binary(
  uuid: String,
  var createdDate: Instant = Instant.now(),
  var lastUpdated: Instant = Instant.now(),
  content: String,
  md5: String
) extends Timestamped
