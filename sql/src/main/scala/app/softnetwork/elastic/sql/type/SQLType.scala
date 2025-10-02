package app.softnetwork.elastic.sql.`type`

sealed trait SQLType {
  def typeId: String
  override def toString: String = typeId
}

trait SQLAny extends SQLType

trait SQLNull extends SQLAny

trait SQLTemporal extends SQLType

trait SQLDate extends SQLTemporal
trait SQLTime extends SQLTemporal
trait SQLDateTime extends SQLTemporal
trait SQLTimestamp extends SQLDateTime

trait SQLNumeric extends SQLType

trait SQLTinyInt extends SQLNumeric
trait SQLSmallInt extends SQLNumeric
trait SQLInt extends SQLNumeric
trait SQLBigInt extends SQLNumeric
trait SQLDouble extends SQLNumeric
trait SQLReal extends SQLNumeric

trait SQLLiteral extends SQLType
trait SQLVarchar extends SQLLiteral
trait SQLChar extends SQLLiteral

trait SQLBool extends SQLType

trait SQLArray extends SQLType { def elementType: SQLType }

trait SQLStruct extends SQLType
