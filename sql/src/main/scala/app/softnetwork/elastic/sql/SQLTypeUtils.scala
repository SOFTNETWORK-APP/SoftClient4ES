package app.softnetwork.elastic.sql
import SQLTypes._

object SQLTypeUtils {

  def matches(out: SQLType, in: SQLType): Boolean =
    out.typeId == in.typeId ||
    (out.typeId == Temporal.typeId && Set(
      Date.typeId,
      DateTime.typeId,
      Time.typeId,
      Timestamp.typeId
    ).contains(
      in.typeId
    )) ||
    (in.typeId == Temporal.typeId && Set(
      Date.typeId,
      DateTime.typeId,
      Time.typeId,
      Timestamp.typeId
    ).contains(
      out.typeId
    )) ||
    (out.typeId == Number.typeId && Set(Int.typeId, Long.typeId, Double.typeId, Float.typeId)
      .contains(
        in.typeId
      )) ||
    (in.typeId == Number.typeId && Set(Int.typeId, Long.typeId, Double.typeId, Float.typeId)
      .contains(
        out.typeId
      )) ||
    out.typeId == Any.typeId || in.typeId == Any.typeId ||
    out.typeId == Null.typeId || in.typeId == Null.typeId

  def leastCommonSuperType(types: List[SQLType]): SQLType = {
    val distinct = types.distinct
    if (distinct.size == 1) return distinct.head

    // 1. String
    if (distinct.exists(matches(SQLTypes.String, _))) return SQLTypes.String

    // 2. Number
    if (distinct.exists(matches(SQLTypes.Double, _))) return SQLTypes.Double
    if (distinct.exists(matches(SQLTypes.Long, _))) return SQLTypes.Long
    if (distinct.exists(matches(SQLTypes.Int, _))) return SQLTypes.Int
    if (distinct.exists(matches(SQLTypes.Number, _))) return SQLTypes.Number

    // 3. Temporal
    if (distinct.exists(matches(SQLTypes.Timestamp, _))) return SQLTypes.Timestamp
    if (distinct.exists(matches(SQLTypes.DateTime, _))) return SQLTypes.DateTime

    // mixed case DATE + TIME → DATETIME
    if (distinct.exists(matches(SQLTypes.Date, _)) && distinct.exists(matches(SQLTypes.Time, _)))
      return SQLTypes.DateTime

    if (distinct.exists(matches(SQLTypes.Date, _))) return SQLTypes.Date
    if (distinct.exists(matches(SQLTypes.Time, _))) return SQLTypes.Time
    if (distinct.exists(matches(SQLTypes.Temporal, _))) return SQLTypes.Timestamp

    // 4. Null or Any
    if (distinct.exists(matches(SQLTypes.Null, _))) return SQLTypes.Any
    if (distinct.exists(matches(SQLTypes.Any, _))) return SQLTypes.Any

    // 5. Fallback
    SQLTypes.Any
  }

  def coerce(in: PainlessScript, to: SQLType): String = {
    val expr = in.painless
    val from = in.out
    val ret = {
      (from, to) match {
        // ---- DATE & TIME ----
        case (SQLTypes.Date, SQLTypes.DateTime | SQLTypes.Timestamp) =>
          s"($expr).atStartOfDay(ZoneId.of('Z'))"
        case (SQLTypes.DateTime | SQLTypes.Timestamp, SQLTypes.Date) =>
          s"($expr).toLocalDate()"
        case (SQLTypes.DateTime | SQLTypes.Timestamp, SQLTypes.Time) =>
          s"($expr).toLocalTime()"

        // ---- NUMERIQUES ----
        case (SQLTypes.Int, SQLTypes.Long) =>
          s"((long) $expr)"
        case (SQLTypes.Int, SQLTypes.Double) =>
          s"((double) $expr)"
        case (SQLTypes.Long, SQLTypes.Double) =>
          s"((double) $expr)"

        // ---- NUMERIC <-> TEMPORAL ----
        case (SQLTypes.Long, SQLTypes.Timestamp) =>
          s"Instant.ofEpochMilli($expr).atZone(ZoneId.of('Z'))"
        case (SQLTypes.Timestamp, SQLTypes.Long) =>
          s"$expr.toInstant().toEpochMilli()"

        // ---- BOOLEEN -> NUMERIC ----
        case (SQLTypes.Boolean, SQLTypes.Number) =>
          s"($expr ? 1 : 0)"

        // ---- IDENTITY ----
        case (_, _) if from == to =>
          return expr

        // ---- PAR DEFAUT ----
        case _ =>
          return expr // fallback
      }
    }
    if (!in.nullable)
      return ret
    s"($expr != null ? $ret : null)"
  }

}
