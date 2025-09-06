package app.softnetwork.elastic.sql

sealed trait SQLType { def typeId: String }

trait SQLAny extends SQLType
trait SQLTemporal extends SQLType
trait SQLDate extends SQLTemporal
trait SQLDateTime extends SQLTemporal
trait SQLNumber extends SQLType
trait SQLString extends SQLType

object SQLTypeCompatibility {
  def matches(out: SQLType, in: SQLType): Boolean =
    out.typeId == in.typeId ||
    (out.typeId == "temporal" && Set("date", "datetime").contains(in.typeId)) ||
    (in.typeId == "temporal" && Set("date", "datetime").contains(out.typeId)) ||
    out.typeId == "any" || in.typeId == "any"
}
