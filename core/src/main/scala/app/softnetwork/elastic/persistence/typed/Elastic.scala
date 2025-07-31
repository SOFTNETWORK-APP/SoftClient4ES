package app.softnetwork.elastic.persistence.typed

import app.softnetwork.persistence._

import app.softnetwork.persistence.model.Timestamped

import scala.language.implicitConversions

import app.softnetwork.persistence._

/** Created by smanciot on 10/04/2020.
  */
object Elastic {

  def index(_type: String): String = {
    s"${_type}s-$environment".toLowerCase
  }

  def alias(_type: String): String = {
    s"${_type}s-$environment-v$version".toLowerCase
  }

  def getAlias[T <: Timestamped](implicit m: Manifest[T]): String = {
    alias(getType[T])
  }

  def getIndex[T <: Timestamped](implicit m: Manifest[T]): String = {
    index(getType[T])
  }

}
