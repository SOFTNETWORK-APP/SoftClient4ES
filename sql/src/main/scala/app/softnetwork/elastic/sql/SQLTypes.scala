package app.softnetwork.elastic.sql

object SQLTypes {
  case object Any extends SQLAny { val typeId = "any" }

  case object Null extends SQLAny { val typeId = "null" }

  case object Temporal extends SQLTemporal { val typeId = "temporal" }

  case object Date extends SQLTemporal with SQLDate { val typeId = "date" }
  case object Time extends SQLTemporal with SQLTime { val typeId = "time" }
  case object DateTime extends SQLTemporal with SQLDateTime { val typeId = "datetime" }
  case object Timestamp extends SQLTimestamp { val typeId = "timestamp" }

  case object Numeric extends SQLNumeric { val typeId = "numeric" }

  case object TinyInt extends SQLTinyInt { val typeId = "tinyint" }
  case object SmallInt extends SQLSmallInt { val typeId = "smallint" }
  case object Int extends SQLInt { val typeId = "int" }
  case object BigInt extends SQLBigInt { val typeId = "bigint" }
  case object Double extends SQLDouble { val typeId = "double" }
  case object Real extends SQLReal { val typeId = "float" }

  case object Literal extends SQLLiteral { val typeId = "literal" }

  case object Char extends SQLChar { val typeId = "char" }
  case object Varchar extends SQLVarchar { val typeId = "varchar" }

  case object Boolean extends SQLBool { val typeId = "boolean" }

  case class Array(elementType: SQLType) extends SQLArray {
    val typeId = s"array<${elementType.typeId}>"
  }

  case object Struct extends SQLStruct { val typeId = "struct" }
}
