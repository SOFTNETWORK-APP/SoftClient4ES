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

package app.softnetwork.elastic.client

import app.softnetwork.elastic.client.SetOperationValues.DateReading
import app.softnetwork.elastic.sql.`type`.{SQLType, SQLTypes}
import app.softnetwork.elastic.sql.query.{SingleSearch, TemporalLiterals, UnnestScope}
import app.softnetwork.elastic.sql.query.UnnestScope.ElementConversion
import app.softnetwork.elastic.sql.time.TimeUnit

import java.math.{BigDecimal => JBigDecimal, BigInteger, RoundingMode}
import java.time.{
  DayOfWeek,
  Instant,
  LocalDate,
  LocalDateTime,
  LocalTime,
  OffsetDateTime,
  ZoneOffset,
  ZonedDateTime
}
import java.time.temporal.{ChronoUnit, TemporalAdjusters}
import java.util.Locale

/** THE READER of each conversion of a bare UNNEST element column a row statement projects
  * (`UnnestScope.elementConversions`): `CAST`, `TRY_CAST` / `SAFE_CAST`, `::`, `CONVERT` and
  * `DATE_TRUNC`. No script computes them -- Elasticsearch would evaluate it once per PARENT, where
  * the element is not visible -- so the row explosion places the element's value, read as every
  * UNNEST column is read, and the function below converts it, as DuckDB 1.5.5 converts it (MEASURED
  * value by value):
  *
  *   - the element is first READ AS its column's declared type
  *     (`SetOperationValues.elementReader`): a date as its field's mapping format reads it on the
  *     Elasticsearch major the statement runs on (a `DATE` its UTC day, a `TIMESTAMP` its instant),
  *     failing the statement where that format does not read it; a number as its field indexes it.
  *     A `TIMESTAMP` element whose value is date-only (`2024-01-03`) is read as every UNNEST column
  *     is read, a DATE (its UTC day);
  *   - to TEXT: DuckDB's spelling of the value's type (`2024-01-03`, `2024-01-03 10:15:00`, `3`,
  *     `2.5`);
  *   - to an INTEGER type: a number rounded to the nearest (a `DOUBLE` half to even, a text or a
  *     decimal half away from zero), a text read as a number (`' 5 '`, `'5.5'`, `'1e2'`); a value
  *     outside the type's range, a text that is no number, and a date fail -- or are NULL under
  *     `TRY_CAST`;
  *   - to a `DOUBLE` or a `REAL`: a number, a text read as one (`'inf'`, `'nan'` too);
  *   - to a `DATE` or a `TIMESTAMP`: a date only (a `DATE` is its day's start as a `TIMESTAMP`);
  *   - `DATE_TRUNC`: a date truncated in UTC; a `DATE` truncated to a day or a longer unit stays a
  *     `DATE`, every other is a `TIMESTAMP`.
  *
  * A conversion the column TYPES refuse is refused before any row (`UnnestScope.
  * elementConversionRefusal`); the failures here are the ones only a VALUE decides, and those of a
  * statement whose schema is not known.
  */
private[client] object ElementConversions {

  /** The reader of each element conversion of `single`, keyed by its row column. Empty -- at no
    * cost
    * -- for a statement without one. `version` is the Elasticsearch version its dates are read as,
    * asked only when a date is read.
    */
  def of(single: SingleSearch, version: => Option[String]): Map[String, Any => Any] = {
    val conversions = UnnestScope.elementConversions(single)
    if (conversions.isEmpty) Map.empty
    else {
      val dates = DateReading(version)
      conversions.map(c => c.output -> converter(single, c, dates)).toMap
    }
  }

  private def converter(
    single: SingleSearch,
    c: ElementConversion,
    dates: DateReading
  ): Any => Any = {
    val source = UnnestScope.elementSourceType(single, c)
    val format =
      if (source.isTemporal)
        UnnestScope.elementColumn(single, c).map(TemporalLiterals.FieldFormat.of(_).spec)
      else None
    converter(source, c.target, c.truncation, c.safe, c.expression, c.path, format, dates)
  }

  /** The conversion of ONE element value of `source` (`ANY`: typed by its value), built once per
    * statement.
    */
  private[client] def converter(
    source: SQLType,
    target: Option[SQLType],
    truncation: Option[TimeUnit],
    safe: Boolean,
    expression: String,
    path: String,
    format: Option[String],
    dates: DateReading
  ): Any => Any = {
    val nonScalar =
      s"Conversion Error: $expression converts ONE value, and an element of $path holds several"
    val declared =
      SetOperationValues.elementReader(source, format, path, expression, dates, nonScalar)
    val read: Any => Any = source match {
      // A TIMESTAMP element is read as every UNNEST column is: a date-only value -- the response
      // parser reads `2024-01-03` as a `LocalDate` -- is a DATE, its day read by the field's format
      // (the same instant, its UTC day); any other value is the instant the format reads.
      case SQLTypes.Timestamp =>
        val one =
          SetOperationValues.elementReader(SQLTypes.Any, None, path, expression, dates, nonScalar)
        value =>
          one(value) match {
            case null => null
            case d: LocalDate =>
              declared(d) match {
                case z: ZonedDateTime => z.withZoneSameInstant(ZoneOffset.UTC).toLocalDate
                case other            => other
              }
            case other => declared(other)
          }
      case _ => declared
    }
    def failed(detail: String): Any =
      if (safe) null
      else throw new IllegalArgumentException(s"Conversion Error: $detail in $expression")
    def unimplemented(value: Any, to: String): Any =
      failed(s"Unimplemented type for cast (${typeOfValue(value)} -> $to)")
    def toIntegral(name: String, duck: String, min: Long, max: Long)(box: Long => Any): Any => Any =
      value => integral(value, name, duck, min, max, failed, unimplemented).map(box).orNull
    val convert: Any => Any = (target, truncation) match {
      case (Some(SQLTypes.Varchar | SQLTypes.Char | SQLTypes.Text | SQLTypes.Keyword), _) =>
        text(source)
      case (Some(SQLTypes.TinyInt), _) =>
        toIntegral("TINYINT", "INT8", Byte.MinValue, Byte.MaxValue)(l =>
          java.lang.Byte.valueOf(l.toByte)
        )
      case (Some(SQLTypes.SmallInt), _) =>
        toIntegral("SMALLINT", "INT16", Short.MinValue, Short.MaxValue)(l =>
          java.lang.Short.valueOf(l.toShort)
        )
      case (Some(SQLTypes.Int), _) =>
        toIntegral("INTEGER", "INT32", Int.MinValue, Int.MaxValue)(l =>
          java.lang.Integer.valueOf(l.toInt)
        )
      case (Some(SQLTypes.BigInt), _) =>
        toIntegral("BIGINT", "INT64", Long.MinValue, Long.MaxValue)(l => java.lang.Long.valueOf(l))
      case (Some(SQLTypes.Double), _) =>
        value =>
          floating(value, "DOUBLE", failed, unimplemented) match {
            case Some(d) => java.lang.Double.valueOf(d)
            case None    => null
          }
      case (Some(SQLTypes.Real), _) =>
        value =>
          floating(value, "FLOAT", failed, unimplemented) match {
            case Some(d) if !d.isNaN && !d.isInfinite && math.abs(d) > Float.MaxValue =>
              failed(
                s"Type DOUBLE with value $d can't be cast because the value is out of range " +
                "for the destination type FLOAT"
              )
            case Some(d) => java.lang.Float.valueOf(d.toFloat)
            case None    => null
          }
      case (Some(SQLTypes.Date), _) =>
        value => dayOf(value).getOrElse(notADate(value, "DATE", unimplemented))
      case (Some(SQLTypes.Timestamp | SQLTypes.DateTime), _) =>
        value => instantOf(value).getOrElse(notADate(value, "TIMESTAMP", unimplemented))
      case (None, Some(unit)) =>
        value => truncated(value, unit).getOrElse(notADate(value, "DATE_TRUNC", unimplemented))
      case _ =>
        // `UnnestScope.elementConversions` derives no other: a guard, never reached
        _ => throw new IllegalStateException(s"No reader converts $expression")
    }
    value => {
      val v = read(value)
      if (v == null) null else convert(v)
    }
  }

  /** TEXT as DuckDB spells the source type: by its column's type -- a temporal, and a value of no
    * known type, by the value's class (a TIMESTAMP element read as a DATE is spelled as one).
    */
  private def text(source: SQLType): Any => Any =
    if (source != SQLTypes.Any && !source.isTemporal) SetOperationValues.textOfType(source)
    else {
      case n @ (_: java.lang.Long | _: java.lang.Integer | _: java.lang.Short | _: java.lang.Byte |
          _: BigInteger) =>
        SetOperationValues.textOfType(SQLTypes.BigInt)(n)
      case d: java.lang.Double => SetOperationValues.textOfType(SQLTypes.Double)(d)
      case f: java.lang.Float  => SetOperationValues.textOfType(SQLTypes.Real)(f)
      case d: LocalDate        => SetOperationValues.textOfType(SQLTypes.Date)(d)
      case t: LocalTime        => SetOperationValues.textOfType(SQLTypes.Time)(t)
      case z @ (_: ZonedDateTime | _: OffsetDateTime | _: LocalDateTime | _: Instant) =>
        SetOperationValues.textOfType(SQLTypes.Timestamp)(z)
      case other => SetOperationValues.textOfType(SQLTypes.Varchar)(other)
    }

  /** The name DuckDB gives the type of a value (its messages). */
  private def typeOfValue(value: Any): String =
    value match {
      case _: LocalDate                                                         => "DATE"
      case _: ZonedDateTime | _: OffsetDateTime | _: LocalDateTime | _: Instant => "TIMESTAMP"
      case _: LocalTime                                                         => "TIME"
      case _: java.lang.Boolean                                                 => "BOOLEAN"
      case _: String                                                            => "VARCHAR"
      case _: java.lang.Double | _: java.lang.Float                             => "DOUBLE"
      case _: java.lang.Number                                                  => "BIGINT"
      case other => other.getClass.getSimpleName
    }

  /** A text read as a number as DuckDB reads it: trimmed, `+` / `-`, digits, a fraction, an
    * exponent. `None` for any other text.
    */
  private def decimalOf(text: String): Option[JBigDecimal] = {
    val t = text.trim
    if (t.isEmpty || !t.exists(_.isDigit)) None
    else scala.util.Try(new JBigDecimal(t)).toOption
  }

  /** An integral value of the range [min, max], as a `Long`: `None` for NULL (a `TRY_CAST`'s
    * failure). Fails through `failed` / `unimplemented`.
    */
  private def integral(
    value: Any,
    sqlName: String,
    duckName: String,
    min: Long,
    max: Long,
    failed: String => Any,
    unimplemented: (Any, String) => Any
  ): Option[Long] = {
    def inRange(d: JBigDecimal, typeName: String, shown: String): Option[Long] =
      if (d.compareTo(JBigDecimal.valueOf(min)) < 0 || d.compareTo(JBigDecimal.valueOf(max)) > 0) {
        failed(
          s"Type $typeName with value $shown can't be cast because the value is out of range " +
          s"for the destination type $duckName"
        )
        None
      } else Some(d.longValueExact())
    value match {
      case n @ (_: java.lang.Long | _: java.lang.Integer | _: java.lang.Short |
          _: java.lang.Byte) =>
        inRange(
          JBigDecimal.valueOf(n.asInstanceOf[java.lang.Number].longValue()),
          "INT64",
          n.toString
        )
      case b: BigInteger => inRange(new JBigDecimal(b), "INT128", b.toString)
      case n @ (_: java.lang.Double | _: java.lang.Float) =>
        val d = n.asInstanceOf[java.lang.Number].doubleValue()
        if (d.isNaN || d.isInfinite) {
          failed(
            s"Type DOUBLE with value $d can't be cast because the value is out of range for " +
            s"the destination type $duckName"
          )
          None
        } else inRange(new JBigDecimal(math.rint(d)), "DOUBLE", n.toString)
      case d: JBigDecimal =>
        inRange(d.setScale(0, RoundingMode.HALF_UP), "DECIMAL", d.toPlainString)
      case b: java.lang.Boolean => Some(if (b) 1L else 0L)
      case s: String =>
        decimalOf(s).map(_.setScale(0, RoundingMode.HALF_UP)) match {
          case Some(d)
              if d.compareTo(JBigDecimal.valueOf(min)) >= 0 &&
                d.compareTo(JBigDecimal.valueOf(max)) <= 0 =>
            Some(d.longValueExact())
          case _ =>
            failed(s"Could not convert string '$s' to $duckName")
            None
        }
      case other =>
        unimplemented(other, sqlName)
        None
    }
  }

  /** A floating-point value: `None` for NULL (a `TRY_CAST`'s failure). */
  private def floating(
    value: Any,
    sqlName: String,
    failed: String => Any,
    unimplemented: (Any, String) => Any
  ): Option[Double] =
    value match {
      case b: java.lang.Boolean => Some(if (b) 1.0 else 0.0)
      case n: java.lang.Number  => Some(n.doubleValue())
      case s: String =>
        s.trim.toLowerCase(Locale.ROOT) match {
          case "inf" | "+inf" | "infinity" | "+infinity" => Some(Double.PositiveInfinity)
          case "-inf" | "-infinity"                      => Some(Double.NegativeInfinity)
          case "nan" | "+nan" | "-nan"                   => Some(Double.NaN)
          case _ =>
            decimalOf(s).map(_.doubleValue()) match {
              case found @ Some(_) => found
              case None =>
                failed(s"Could not convert string '$s' to $sqlName")
                None
            }
        }
      case other =>
        unimplemented(other, sqlName)
        None
    }

  /** A date's UTC day. */
  private def dayOf(value: Any): Option[LocalDate] =
    value match {
      case d: LocalDate      => Some(d)
      case z: ZonedDateTime  => Some(z.withZoneSameInstant(ZoneOffset.UTC).toLocalDate)
      case o: OffsetDateTime => Some(o.atZoneSameInstant(ZoneOffset.UTC).toLocalDate)
      case l: LocalDateTime  => Some(l.toLocalDate)
      case i: Instant        => Some(i.atZone(ZoneOffset.UTC).toLocalDate)
      case _                 => None
    }

  /** A date's instant, in UTC; a `DATE` is its day's start. */
  private def instantOf(value: Any): Option[ZonedDateTime] =
    value match {
      case d: LocalDate      => Some(d.atStartOfDay(ZoneOffset.UTC))
      case z: ZonedDateTime  => Some(z.withZoneSameInstant(ZoneOffset.UTC))
      case o: OffsetDateTime => Some(o.atZoneSameInstant(ZoneOffset.UTC))
      case l: LocalDateTime  => Some(l.atZone(ZoneOffset.UTC))
      case i: Instant        => Some(i.atZone(ZoneOffset.UTC))
      case _                 => None
    }

  /** A value that is no date reaching a date conversion: a text, whose reading DuckDB's rules
    * decide (refused when its column is known to be a text -- `UnnestScope.
    * elementConversionRefusal` -- and FAILED here, never guessed, when it is not); any other, a
    * conversion DuckDB does not implement.
    */
  private def notADate(value: Any, to: String, unimplemented: (Any, String) => Any): Any =
    value match {
      case s: String =>
        throw new IllegalArgumentException(
          s"Conversion Error: '$s' is a text, which is not read as a $to the way DuckDB reads it"
        )
      case other => unimplemented(other, to)
    }

  /** DATE_TRUNC in UTC (ISO weeks start on Monday): a `DATE` truncated to a day or a longer unit is
    * a `DATE`, every other truncation a `TIMESTAMP`.
    */
  private def truncated(value: Any, unit: TimeUnit): Option[Any] = {
    def day(d: LocalDate): LocalDate =
      unit match {
        case TimeUnit.YEARS => d.withDayOfYear(1)
        case TimeUnit.QUARTERS =>
          d.withMonth(((d.getMonthValue - 1) / 3) * 3 + 1).withDayOfMonth(1)
        case TimeUnit.MONTHS => d.withDayOfMonth(1)
        case TimeUnit.WEEKS  => d.`with`(TemporalAdjusters.previousOrSame(DayOfWeek.MONDAY))
        case _               => d
      }
    val dateLevel = unit match {
      case TimeUnit.YEARS | TimeUnit.QUARTERS | TimeUnit.MONTHS | TimeUnit.WEEKS | TimeUnit.DAYS =>
        true
      case _ => false
    }
    value match {
      case d: LocalDate =>
        Some(if (dateLevel) day(d) else d.atStartOfDay(ZoneOffset.UTC))
      case other =>
        instantOf(other).map { z =>
          if (dateLevel) day(z.toLocalDate).atStartOfDay(ZoneOffset.UTC)
          else
            unit match {
              case TimeUnit.HOURS   => z.truncatedTo(ChronoUnit.HOURS)
              case TimeUnit.MINUTES => z.truncatedTo(ChronoUnit.MINUTES)
              case _                => z.truncatedTo(ChronoUnit.SECONDS)
            }
        }
    }
  }
}
