package app.softnetwork.elastic.sql

object SQLTypes {
  case object Any extends SQLAny { val typeId = "any" }
  case object Null extends SQLAny { val typeId = "null" }
  case object Temporal extends SQLTemporal { val typeId = "temporal" }
  case object Date extends SQLTemporal with SQLDate { val typeId = "date" }
  case object Time extends SQLTemporal with SQLTime { val typeId = "time" }
  case object DateTime extends SQLTemporal with SQLDateTime { val typeId = "datetime" }
  case object Timestamp extends SQLTemporal with SQLDateTime { val typeId = "timestamp" }
  case object Number extends SQLNumber { val typeId = "number" }
  case object Int extends SQLNumber { val typeId = "integer" }
  case object Long extends SQLNumber { val typeId = "long" }
  case object Double extends SQLNumber { val typeId = "double" }
  case object Float extends SQLNumber { val typeId = "float" }
  case object String extends SQLString { val typeId = "string" }
  case object Boolean extends SQLBool { val typeId = "boolean" }
}
