package app.softnetwork.elastic.sql.`type`

import app.softnetwork.elastic.sql.PainlessScript
import app.softnetwork.elastic.sql.`type`.SQLTypes._

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
    (out.typeId == Numeric.typeId && Set(
      TinyInt.typeId,
      SmallInt.typeId,
      Int.typeId,
      BigInt.typeId,
      Double.typeId,
      Real.typeId
    )
      .contains(
        in.typeId
      )) ||
    (in.typeId == Numeric.typeId && Set(
      TinyInt.typeId,
      SmallInt.typeId,
      Int.typeId,
      BigInt.typeId,
      Double.typeId,
      Real.typeId
    )
      .contains(
        out.typeId
      )) ||
    (out.isInstanceOf[SQLNumeric] && in.isInstanceOf[SQLNumeric]) ||
    (out.typeId == Varchar.typeId && in.typeId == Varchar.typeId) ||
    (out.typeId == Boolean.typeId && in.typeId == Boolean.typeId) ||
    out.typeId == Any.typeId || in.typeId == Any.typeId ||
    out.typeId == Null.typeId || in.typeId == Null.typeId

  def leastCommonSuperType(types: List[SQLType]): SQLType = {
    val distinct = types.distinct
    if (distinct.size == 1) return distinct.head

    // 1. String
    if (distinct.contains(SQLTypes.Varchar)) return SQLTypes.Varchar

    // 2. Number
    if (distinct.contains(SQLTypes.Double)) return SQLTypes.Double
    if (distinct.contains(SQLTypes.Real)) return SQLTypes.Real
    if (distinct.contains(SQLTypes.BigInt)) return SQLTypes.BigInt
    if (distinct.contains(SQLTypes.Int)) return SQLTypes.Int
    if (distinct.contains(SQLTypes.SmallInt)) return SQLTypes.SmallInt
    if (distinct.contains(SQLTypes.TinyInt)) return SQLTypes.TinyInt
    if (distinct.contains(SQLTypes.Numeric)) return SQLTypes.Numeric

    // 3. Temporal
    if (distinct.contains(SQLTypes.Timestamp)) return SQLTypes.Timestamp
    if (distinct.contains(SQLTypes.DateTime)) return SQLTypes.DateTime

    // mixed case DATE + TIME → DATETIME
    if (distinct.contains(SQLTypes.Date) && distinct.contains(SQLTypes.Time))
      return SQLTypes.DateTime

    if (distinct.contains(SQLTypes.Date)) return SQLTypes.Date
    if (distinct.contains(SQLTypes.Time)) return SQLTypes.Time
    if (distinct.contains(SQLTypes.Temporal)) return SQLTypes.Timestamp

    // 4. Null or Any
    if (distinct.contains(SQLTypes.Null)) return SQLTypes.Any
    if (distinct.contains(SQLTypes.Any)) return SQLTypes.Any

    // 5. Fallback
    SQLTypes.Any
  }

  def coerce(in: PainlessScript, to: SQLType): String = {
    val expr = in.painless()
    val from = in.baseType
    val nullable = in.nullable
    coerce(expr, from, to, nullable)
  }

  def coerce(expr: String, from: SQLType, to: SQLType, nullable: Boolean): String = {
    val ret = {
      (from, to) match {
        // ---- DATE & TIME ----
        case (SQLTypes.Date, SQLTypes.DateTime | SQLTypes.Timestamp) =>
          s"$expr.atStartOfDay(ZoneId.of('Z'))"
        case (SQLTypes.DateTime | SQLTypes.Timestamp, SQLTypes.Date) =>
          s"$expr.toLocalDate()"
        case (SQLTypes.DateTime | SQLTypes.Timestamp, SQLTypes.Time) =>
          s"$expr.toLocalTime()"

        // ---- NUMERIQUES ----
        case (SQLTypes.Int, SQLTypes.BigInt) =>
          s"((long) $expr)"
        case (SQLTypes.Int, SQLTypes.Double) =>
          s"((double) $expr)"
        case (SQLTypes.BigInt, SQLTypes.Double) =>
          s"((double) $expr)"

        // ---- NUMERIC <-> TEMPORAL ----
        case (SQLTypes.BigInt, SQLTypes.Timestamp | SQLTypes.DateTime) =>
          s"Instant.ofEpochMilli($expr).atZone(ZoneId.of('Z'))"
        case (SQLTypes.Timestamp | SQLTypes.DateTime, SQLTypes.BigInt) =>
          s"$expr.toInstant().toEpochMilli()"

        // ---- BOOLEAN -> NUMERIC ----
        case (SQLTypes.Boolean, SQLTypes.Numeric | SQLTypes.Int) =>
          s"($expr ? 1 : 0)"
        case (SQLTypes.Boolean, SQLTypes.BigInt) =>
          s"($expr ? 1L : 0L)"
        case (SQLTypes.Boolean, SQLTypes.Double) =>
          s"($expr ? 1.0 : 0.0)"
        case (SQLTypes.Boolean, SQLTypes.Real) =>
          s"($expr ? 1.0f : 0.0f)"
        case (SQLTypes.Boolean, SQLTypes.SmallInt) =>
          s"(short)($expr ? 1 : 0)"
        case (SQLTypes.Boolean, SQLTypes.TinyInt) =>
          s"(byte)($expr ? 1 : 0)"

        // ---- VARCHAR -> NUMERIC ----
        case (SQLTypes.Varchar, SQLTypes.Int) =>
          s"Integer.parseInt($expr).intValue()"
        case (SQLTypes.Varchar, SQLTypes.BigInt) =>
          s"Long.parseLong($expr).longValue()"
        case (SQLTypes.Varchar, SQLTypes.Double) =>
          s"Double.parseDouble($expr).doubleValue()"
        case (SQLTypes.Varchar, SQLTypes.Real) =>
          s"Float.parseFloat($expr).floatValue()"
        case (SQLTypes.Varchar, SQLTypes.SmallInt) =>
          s"Short.parseShort($expr).shortValue()"
        case (SQLTypes.Varchar, SQLTypes.TinyInt) =>
          s"Byte.parseByte($expr).byteValue()"

        // ---- VARCHAR -> TEMPORAL ----
        case (SQLTypes.Varchar, SQLTypes.Date) =>
          s"LocalDate.parse($expr, DateTimeFormatter.ofPattern('yyyy-MM-dd'))"
        case (SQLTypes.Varchar, SQLTypes.Time) =>
          s"LocalTime.parse($expr, DateTimeFormatter.ofPattern('HH:mm:ss'))"
        case (SQLTypes.Varchar, SQLTypes.DateTime) =>
          s"ZonedDateTime.parse($expr, DateTimeFormatter.ISO_DATE_TIME)"
        case (SQLTypes.Varchar, SQLTypes.Timestamp) =>
          s"ZonedDateTime.parse($expr, DateTimeFormatter.ISO_ZONED_DATE_TIME)"

        // ---- IDENTITY ----
        case (_, _) if from == to =>
          return expr

        // ---- Any -> VARCHAR ----
        case (_, SQLTypes.Varchar) =>
          s"String.valueOf($expr)"

        // ---- PAR DEFAUT ----
        case _ =>
          return expr // fallback
      }
    }
    if (!nullable)
      return ret
    s"($expr != null ? $ret : null)"
  }

}
