package app.softnetwork.elastic.sql.`type`

object SQLTypes {
  case object Any extends SQLAny { val typeId = "ANY" }

  case object Null extends SQLAny { val typeId = "NULL" }

  case object Temporal extends SQLTemporal { val typeId = "TEMPORAL" }

  case object Date extends SQLTemporal with SQLDate { val typeId = "DATE" }
  case object Time extends SQLTemporal with SQLTime { val typeId = "TIME" }
  case object DateTime extends SQLTemporal with SQLDateTime { val typeId = "DATETIME" }
  case object Timestamp extends SQLTimestamp { val typeId = "TIMESTAMP" }

  case object Numeric extends SQLNumeric { val typeId = "NUMERIC" }

  case object TinyInt extends SQLTinyInt { val typeId = "TINYINT" }
  case object SmallInt extends SQLSmallInt { val typeId = "SMALLINT" }
  case object Int extends SQLInt { val typeId = "INT" }
  case object BigInt extends SQLBigInt { val typeId = "BIGINT" }
  case object Double extends SQLDouble { val typeId = "DOUBLE" }
  case object Real extends SQLReal { val typeId = "REAL" }

  case object Literal extends SQLLiteral { val typeId = "LITERAL" }

  case object Char extends SQLChar { val typeId = "CHAR" }
  case object Varchar extends SQLVarchar { val typeId = "VARCHAR" }

  case object Boolean extends SQLBool { val typeId = "BOOLEAN" }

  case class Array(elementType: SQLType) extends SQLArray {
    val typeId = s"array<${elementType.typeId}>"
  }

  case object Struct extends SQLStruct { val typeId = "STRUCT" }
}
