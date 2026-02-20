/*
 * Copyright 2026 SOFTNETWORK
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package app.softnetwork.elastic.sql.`type`

/** Runtime value type inference and coercion utilities.
  *
  * These functions operate on plain Scala/Java runtime values (not Painless scripts) and are
  * independent of any JDBC or Arrow-specific API. Both the JDBC driver and the Arrow Flight SQL
  * server delegate to this object for type inference and value coercion.
  *
  * JDBC-specific mappings (`toJdbcType`, `toJdbcTypeName`, `coerceValue`, etc.) remain in the
  * `driver` module's `TypeMapping` object, which delegates here for the general-purpose methods.
  */
object ValueCoercion {

  // ─── Type inference ─────────────────────────────────────────────────────────

  /** Infer the [[SQLType]] from a runtime value. */
  def inferType(value: Any): SQLType = value match {
    case null                                   => SQLTypes.Null
    case _: Int                                 => SQLTypes.Int
    case _: Long                                => SQLTypes.BigInt
    case _: Double                              => SQLTypes.Double
    case _: Float                               => SQLTypes.Real
    case _: Boolean                             => SQLTypes.Boolean
    case _: Short                               => SQLTypes.SmallInt
    case _: Byte                                => SQLTypes.TinyInt
    case _: java.math.BigDecimal                => SQLTypes.Double
    case _: BigDecimal                          => SQLTypes.Double
    case _: java.sql.Date                       => SQLTypes.Date
    case _: java.sql.Time                       => SQLTypes.Time
    case _: java.sql.Timestamp                  => SQLTypes.Timestamp
    case _: java.time.LocalDate                 => SQLTypes.Date
    case _: java.time.LocalTime                 => SQLTypes.Time
    case _: java.time.LocalDateTime             => SQLTypes.Timestamp
    case _: java.time.Instant                   => SQLTypes.Timestamp
    case _: java.time.ZonedDateTime             => SQLTypes.Timestamp
    case _: java.time.temporal.TemporalAccessor => SQLTypes.Timestamp
    case _: Seq[_]                              => SQLTypes.Array(SQLTypes.Any)
    case _: Map[_, _]                           => SQLTypes.Struct
    case _: Array[Byte]                         => SQLTypes.VarBinary
    case _: String                              => SQLTypes.Varchar
    case _: Number                              => SQLTypes.Double
    case _                                      => SQLTypes.Varchar
  }

  // ─── Coercions ───────────────────────────────────────────────────────────────

  def coerceToString(value: Any): String = value match {
    case null           => null
    case s: String      => s
    case seq: Seq[_]    => seq.mkString("[", ", ", "]")
    case map: Map[_, _] => map.map { case (k, v) => s"$k: $v" }.mkString("{", ", ", "}")
    case other          => other.toString
  }

  def coerceToInt(value: Any): java.lang.Integer = value match {
    case null       => null
    case n: Number  => n.intValue()
    case s: String  => java.lang.Integer.valueOf(s)
    case b: Boolean => if (b) 1 else 0
    case _ =>
      throw new java.sql.SQLException(s"Cannot convert ${value.getClass.getName} to INT")
  }

  def coerceToLong(value: Any): java.lang.Long = value match {
    case null       => null
    case n: Number  => n.longValue()
    case s: String  => java.lang.Long.valueOf(s)
    case b: Boolean => if (b) 1L else 0L
    case _ =>
      throw new java.sql.SQLException(s"Cannot convert ${value.getClass.getName} to BIGINT")
  }

  def coerceToDouble(value: Any): java.lang.Double = value match {
    case null      => null
    case n: Number => n.doubleValue()
    case s: String => java.lang.Double.valueOf(s)
    case _ =>
      throw new java.sql.SQLException(s"Cannot convert ${value.getClass.getName} to DOUBLE")
  }

  def coerceToFloat(value: Any): java.lang.Float = value match {
    case null      => null
    case n: Number => n.floatValue()
    case s: String => java.lang.Float.valueOf(s)
    case _ =>
      throw new java.sql.SQLException(s"Cannot convert ${value.getClass.getName} to REAL")
  }

  def coerceToBoolean(value: Any): java.lang.Boolean = value match {
    case null       => null
    case b: Boolean => b
    case n: Number  => n.intValue() != 0
    case s: String  => java.lang.Boolean.valueOf(s)
    case _ =>
      throw new java.sql.SQLException(s"Cannot convert ${value.getClass.getName} to BOOLEAN")
  }

  def coerceToByte(value: Any): java.lang.Byte = value match {
    case null      => null
    case n: Number => n.byteValue()
    case s: String => java.lang.Byte.valueOf(s)
    case _ =>
      throw new java.sql.SQLException(s"Cannot convert ${value.getClass.getName} to TINYINT")
  }

  def coerceToShort(value: Any): java.lang.Short = value match {
    case null      => null
    case n: Number => n.shortValue()
    case s: String => java.lang.Short.valueOf(s)
    case _ =>
      throw new java.sql.SQLException(s"Cannot convert ${value.getClass.getName} to SMALLINT")
  }

  def coerceToBigDecimal(value: Any): java.math.BigDecimal = value match {
    case null                     => null
    case bd: java.math.BigDecimal => bd
    case bd: BigDecimal           => bd.bigDecimal
    case n: Number                => java.math.BigDecimal.valueOf(n.doubleValue())
    case s: String                => new java.math.BigDecimal(s)
    case _ =>
      throw new java.sql.SQLException(s"Cannot convert ${value.getClass.getName} to DECIMAL")
  }

  def coerceToDate(value: Any): java.sql.Date = value match {
    case null                         => null
    case d: java.sql.Date             => d
    case ts: java.sql.Timestamp       => new java.sql.Date(ts.getTime)
    case ld: java.time.LocalDate      => java.sql.Date.valueOf(ld)
    case ldt: java.time.LocalDateTime => java.sql.Date.valueOf(ldt.toLocalDate)
    case zdt: java.time.ZonedDateTime => java.sql.Date.valueOf(zdt.toLocalDate)
    case i: java.time.Instant         => new java.sql.Date(i.toEpochMilli)
    case t: java.time.temporal.TemporalAccessor =>
      try {
        java.sql.Date.valueOf(java.time.LocalDate.from(t))
      } catch {
        case _: Exception => throw new java.sql.SQLException("Cannot convert temporal to DATE")
      }
    case s: String =>
      try { java.sql.Date.valueOf(s) }
      catch { case _: Exception => throw new java.sql.SQLException(s"Cannot parse '$s' as DATE") }
    case _ =>
      throw new java.sql.SQLException(s"Cannot convert ${value.getClass.getName} to DATE")
  }

  def coerceToTime(value: Any): java.sql.Time = value match {
    case null                         => null
    case t: java.sql.Time             => t
    case lt: java.time.LocalTime      => java.sql.Time.valueOf(lt)
    case ldt: java.time.LocalDateTime => java.sql.Time.valueOf(ldt.toLocalTime)
    case s: String =>
      try { java.sql.Time.valueOf(s) }
      catch { case _: Exception => throw new java.sql.SQLException(s"Cannot parse '$s' as TIME") }
    case _ =>
      throw new java.sql.SQLException(s"Cannot convert ${value.getClass.getName} to TIME")
  }

  def coerceToTimestamp(value: Any): java.sql.Timestamp = value match {
    case null                         => null
    case ts: java.sql.Timestamp       => ts
    case d: java.sql.Date             => new java.sql.Timestamp(d.getTime)
    case i: java.time.Instant         => java.sql.Timestamp.from(i)
    case ldt: java.time.LocalDateTime => java.sql.Timestamp.valueOf(ldt)
    case zdt: java.time.ZonedDateTime => java.sql.Timestamp.from(zdt.toInstant)
    case ld: java.time.LocalDate      => java.sql.Timestamp.valueOf(ld.atStartOfDay())
    case t: java.time.temporal.TemporalAccessor =>
      try {
        java.sql.Timestamp.from(java.time.Instant.from(t))
      } catch {
        case _: Exception =>
          try {
            java.sql.Timestamp.valueOf(java.time.LocalDateTime.from(t))
          } catch {
            case _: Exception =>
              throw new java.sql.SQLException("Cannot convert temporal to TIMESTAMP")
          }
      }
    case s: String =>
      try { java.sql.Timestamp.valueOf(s) }
      catch {
        case _: Exception =>
          try { java.sql.Timestamp.from(java.time.Instant.parse(s)) }
          catch {
            case _: Exception =>
              throw new java.sql.SQLException(s"Cannot parse '$s' as TIMESTAMP")
          }
      }
    case n: Number => new java.sql.Timestamp(n.longValue())
    case _ =>
      throw new java.sql.SQLException(s"Cannot convert ${value.getClass.getName} to TIMESTAMP")
  }
}
