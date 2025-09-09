package app.softnetwork.elastic.sql
import SQLTypes._

sealed trait SQLType { def typeId: String }

trait SQLAny extends SQLType
trait SQLTemporal extends SQLType
trait SQLDate extends SQLTemporal
trait SQLTime extends SQLTemporal
trait SQLDateTime extends SQLTemporal
trait SQLNumber extends SQLType
trait SQLString extends SQLType
trait SQLBool extends SQLType

object SQLTypeCompatibility {
  def matches(out: SQLType, in: SQLType): Boolean =
    out.typeId == in.typeId ||
    (out.typeId == Temporal.typeId && Set(Date.typeId, DateTime.typeId, Time.typeId).contains(
      in.typeId
    )) ||
    (in.typeId == Temporal.typeId && Set(Date.typeId, DateTime.typeId, Time.typeId).contains(
      out.typeId
    )) ||
    out.typeId == Any.typeId || in.typeId == Any.typeId
}
