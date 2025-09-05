package app.softnetwork.elastic.sql

sealed trait SQLType { def typeId: String }

trait SQLTemporal extends SQLType
trait SQLDate extends SQLTemporal
trait SQLDateTime extends SQLTemporal
trait SQLNumber extends SQLType
trait SQLString extends SQLType
