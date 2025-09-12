package app.softnetwork.elastic.sql

sealed trait SQLType { def typeId: String }

trait SQLAny extends SQLType
trait SQLTemporal extends SQLType
trait SQLDate extends SQLTemporal
trait SQLTime extends SQLTemporal
trait SQLDateTime extends SQLTemporal
trait SQLNumber extends SQLType
trait SQLString extends SQLType
trait SQLBool extends SQLType
