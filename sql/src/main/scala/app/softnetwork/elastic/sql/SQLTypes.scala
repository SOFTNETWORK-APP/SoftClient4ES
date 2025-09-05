package app.softnetwork.elastic.sql

object SQLTypes {
  case object Temporal extends SQLTemporal { val typeId = "temporal" }
  case object Date extends SQLDate { val typeId = "date" }
  case object DateTime extends SQLDateTime { val typeId = "datetime" }
  case object Number extends SQLNumber { val typeId = "number" }
  case object String extends SQLString { val typeId = "string" }
}
