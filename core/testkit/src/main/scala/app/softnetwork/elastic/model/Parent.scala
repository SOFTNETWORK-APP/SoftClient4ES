package app.softnetwork.elastic.model

import app.softnetwork.persistence.{generateUUID, now}
import app.softnetwork.persistence.model.Timestamped
import app.softnetwork.time._

import java.time.{Instant, LocalDate}

case class Parent(
  uuid: String,
  name: String,
  birthDate: LocalDate,
  children: Seq[Child] = Seq.empty[Child]
) extends Timestamped {
  def addChild(child: Child): Parent = copy(children = children :+ child)
  lazy val createdDate: Instant = Instant.now()
  lazy val lastUpdated: Instant = Instant.now()
}

case class Child(name: String, birthDate: LocalDate, parentId: String)

object Parent {
  def apply(name: String, birthDate: LocalDate): Parent =
    apply(
      generateUUID(),
      name,
      birthDate
    )

  def apply(uuid: String, name: String, birthDate: LocalDate): Parent =
    apply(
      uuid,
      name,
      birthDate,
      Seq.empty[Child]
    )

  def apply(uuid: String, name: String, birthDate: LocalDate, children: Seq[Child]): Parent = {
    Parent(
      uuid = uuid,
      name = name,
      birthDate = birthDate,
      children = children
    )
  }

}
