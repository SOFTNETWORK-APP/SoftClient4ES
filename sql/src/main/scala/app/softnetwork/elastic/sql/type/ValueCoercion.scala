/*
 * Copyright 2025 SOFTNETWORK
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
    case null      => null
    case s: String => s
    // SCHEMA-2 (D2) -- core reads a VARBINARY as its decoded bytes; its TEXT stays the base64
    // Elasticsearch holds, as in 0.24.0 (JDBC `getString`, arrow's VARCHAR writer)
    case b: Array[Byte] => java.util.Base64.getEncoder.encodeToString(b)
    case seq: Seq[_]    => seq.mkString("[", ", ", "]")
    case map: Map[_, _] => map.map { case (k, v) => s"$k: $v" }.mkString("{", ", ", "}")
    case other          => other.toString
  }

  def coerceToInt(value: Any): java.lang.Integer = value match {
    case null => null
    // SCHEMA-17 fix R-177: the canonical class as it is, any other number within INT's range
    case i: java.lang.Integer => i
    case n: Number            => java.lang.Integer.valueOf(whole(n, IntRange).toInt)
    case s: String            => java.lang.Integer.valueOf(s)
    case b: Boolean           => if (b) 1 else 0
    case _ =>
      throw new java.sql.SQLException(s"Cannot convert ${value.getClass.getName} to INT")
  }

  def coerceToLong(value: Any): java.lang.Long = value match {
    case null              => null
    case l: java.lang.Long => l
    case n: Number         => java.lang.Long.valueOf(whole(n, BigIntRange))
    case s: String         => java.lang.Long.valueOf(s)
    case b: Boolean        => if (b) 1L else 0L
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
    case null               => null
    case f: java.lang.Float => f
    case n: Number          => java.lang.Float.valueOf(real(n))
    case s: String          => java.lang.Float.valueOf(s)
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
    case null              => null
    case b: java.lang.Byte => b
    case n: Number         => java.lang.Byte.valueOf(whole(n, TinyIntRange).toByte)
    case s: String         => java.lang.Byte.valueOf(s)
    case _ =>
      throw new java.sql.SQLException(s"Cannot convert ${value.getClass.getName} to TINYINT")
  }

  def coerceToShort(value: Any): java.lang.Short = value match {
    case null               => null
    case s: java.lang.Short => s
    case n: Number          => java.lang.Short.valueOf(whole(n, SmallIntRange).toShort)
    case s: String          => java.lang.Short.valueOf(s)
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

  /** Coerce a runtime value to a `java.sql.Date`.
    *
    * A `Number` is read as **epoch milliseconds**, the shape Elasticsearch returns for a `date`
    * field whose `_source` stores the value numerically (`"event_ts": 1735689600000`) — ES's
    * default `date` format is `strict_date_optional_time||epoch_millis`, and
    * `ElasticConversion.jsonNodeToAny` only attempts a temporal parse on TEXTUAL nodes, so a
    * numeric date arrives here as a `java.lang.Long`. Without this arm every such value fell to the
    * catch-all and threw `Cannot convert java.lang.Long to DATE`, which took down the whole
    * cross-index JOIN leg (softclient4es-arrow#168) and `ResultSet.getDate` with it. The sibling
    * [[coerceToTimestamp]] has always had this arm; the asymmetry was unintentional.
    *
    * Deliberately NOT mirrored in [[coerceToTime]]: a bare number in a TIME position is ambiguous
    * (epoch millis vs millis-of-day), Elasticsearch has no `time` mapping token — `SQLTypes.Time`
    * is unreachable from an ES mapping, which resolves `"date"` to `SQLTypes.Date` — and no defect
    * has been reported there. Add it when a real value shape demands it, not by symmetry.
    */
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
    case n: Number => new java.sql.Date(n.longValue())
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

  // ─── Range guard (SCHEMA-17 fix R-177) ──────────────────────────────────────
  //
  // [[coerceToByte]], [[coerceToShort]], [[coerceToInt]], [[coerceToLong]] and [[coerceToFloat]]
  // never narrow a number silently: one outside the target type's range throws
  // `SQLException("Value <v> is out of range for <TYPE>")`, as DuckDB's conversion does, where
  // `intValue()` & co. wrapped it (JDBC `getInt`, arrow's `Int32` cell: 3000000000 read
  // -1294967296). A value within the range converts as it always did, its canonical class first
  // and as it is (no new allocation); a fractional value is truncated toward zero as before, and
  // it is the truncated value that must fit (2147483647.9 is 2147483647 for INT).

  /** The range of an integral SQL type, for [[whole]]: `bits` the most a two's-complement value of
    * it holds besides its sign; `below` / `above` the double values strictly between which a number
    * truncated toward zero is in range (below `Long.MinValue`, the next double down).
    */
  private final class IntegralRange(val name: String, val min: Long, val max: Long, val bits: Int) {
    val below: Double = if (min == Long.MinValue) Math.nextDown(min.toDouble) else min - 1d
    val above: Double = max + 1d
    val belowDecimal: java.math.BigDecimal =
      java.math.BigDecimal.valueOf(min).subtract(java.math.BigDecimal.ONE)
    val aboveDecimal: java.math.BigDecimal =
      java.math.BigDecimal.valueOf(max).add(java.math.BigDecimal.ONE)
  }

  private val TinyIntRange = new IntegralRange("TINYINT", Byte.MinValue, Byte.MaxValue, 7)
  private val SmallIntRange = new IntegralRange("SMALLINT", Short.MinValue, Short.MaxValue, 15)
  private val IntRange = new IntegralRange("INT", Int.MinValue, Int.MaxValue, 31)
  private val BigIntRange = new IntegralRange("BIGINT", Long.MinValue, Long.MaxValue, 63)

  /** The largest REAL, exactly, as a decimal and as an integer (it is a whole number). */
  private val RealMaxDecimal: java.math.BigDecimal =
    new java.math.BigDecimal(Float.MaxValue.toDouble)
  private val RealMinDecimal: java.math.BigDecimal = RealMaxDecimal.negate()
  private val RealMaxInteger: java.math.BigInteger = RealMaxDecimal.toBigInteger
  private val RealMinInteger: java.math.BigInteger = RealMaxInteger.negate()

  private def outOfRange(value: Number, sqlType: String): java.sql.SQLException =
    new java.sql.SQLException(s"Value $value is out of range for $sqlType")

  /** `n` truncated toward zero, where that is within `range`; else the out-of-range failure. A
    * whole number of the JDK is judged exactly, a `BigInteger` by its bit length, a `BigDecimal`
    * against the bounds (a fraction of magnitude below 1 is 0, never divided by its scale's power
    * of ten); a `Double`, a `Float` and any other number by its double value, so NaN and the
    * infinities are out of range.
    */
  private def whole(n: Number, range: IntegralRange): Long = n match {
    case _: java.lang.Integer | _: java.lang.Long | _: java.lang.Short | _: java.lang.Byte |
        _: java.util.concurrent.atomic.AtomicInteger | _: java.util.concurrent.atomic.AtomicLong |
        _: java.util.concurrent.atomic.LongAdder | _: java.util.concurrent.atomic.LongAccumulator =>
      val l = n.longValue()
      if (l < range.min || l > range.max) throw outOfRange(n, range.name) else l
    case b: java.math.BigInteger =>
      if (b.bitLength() > range.bits) throw outOfRange(n, range.name) else b.longValue()
    case b: scala.math.BigInt => whole(b.bigInteger, range)
    case d: java.math.BigDecimal =>
      if (d.compareTo(range.belowDecimal) <= 0 || d.compareTo(range.aboveDecimal) >= 0)
        throw outOfRange(n, range.name)
      else if (d.precision().toLong - d.scale() <= 0) 0L
      else d.longValue()
    case d: scala.math.BigDecimal => whole(d.bigDecimal, range)
    case _ =>
      val d = n.doubleValue()
      if (d > range.below && d < range.above) n.longValue() else throw outOfRange(n, range.name)
  }

  /** `n` as a REAL, where it is within ±`Float.MaxValue`; else the out-of-range failure. NaN and
    * the infinities of a floating number stay what they are; a whole number or a decimal beyond the
    * bounds, which `floatValue()` turned into an infinity, fails.
    */
  private def real(n: Number): Float = n match {
    case b: java.math.BigInteger =>
      if (b.compareTo(RealMaxInteger) > 0 || b.compareTo(RealMinInteger) < 0)
        throw outOfRange(n, "REAL")
      else b.floatValue()
    case b: scala.math.BigInt => real(b.bigInteger)
    case d: java.math.BigDecimal =>
      if (d.compareTo(RealMaxDecimal) > 0 || d.compareTo(RealMinDecimal) < 0)
        throw outOfRange(n, "REAL")
      else d.floatValue()
    case d: scala.math.BigDecimal => real(d.bigDecimal)
    case _ =>
      val d = n.doubleValue()
      if (java.lang.Double.isFinite(d) && Math.abs(d) > Float.MaxValue) throw outOfRange(n, "REAL")
      else n.floatValue()
  }
}
