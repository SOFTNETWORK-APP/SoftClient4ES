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

import app.softnetwork.elastic.sql.{Identifier, LiteralParam, PainlessContext, PainlessScript}
import app.softnetwork.elastic.sql.`type`.SQLTypes._

object SQLTypeUtils {

  def painlessType(sqlType: SQLType): String = sqlType match {
    case TinyInt   => "byte"
    case SmallInt  => "short"
    case Int       => "int"
    case BigInt    => "long"
    case Double    => "double"
    case Real      => "float"
    case Numeric   => "java.math.BigDecimal"
    case Varchar   => "String"
    case Boolean   => "boolean"
    case Date      => "LocalDate"
    case Time      => "LocalTime"
    case DateTime  => "LocalDateTime"
    case Timestamp => "ZonedDateTime"
    case Temporal  => "ZonedDateTime"
    case Array(inner) =>
      inner match {
        case TinyInt  => "byte[]"
        case SmallInt => "short[]"
        case Int      => "int[]"
        case BigInt   => "long[]"
        case Double   => "double[]"
        case Real     => "float[]"
        case Boolean  => "boolean[]"
        case _        => s"java.util.List<${painlessType(inner)}>"
      }
    case Struct => "Map<String, Object>"
    case Any    => "Object"
    case Null   => "Object"
    case _      => "Object"
  }

  def elasticType(sqlType: SQLType): String = sqlType match {
    case Null           => "null"
    case TinyInt        => "byte"
    case SmallInt       => "short"
    case Int            => "integer"
    case BigInt         => "long"
    case Double         => "double"
    case Real           => "float"
    case Numeric        => "scaled_float"
    case Varchar | Text => "text"
    case Keyword        => "keyword"
    case Boolean        => "boolean"
    case Date           => "date"
    case Time           => "date"
    case DateTime       => "date"
    case Timestamp      => "date"
    case Temporal       => "date"
    case Array(Struct)  => "nested"
    case Struct         => "object"
    case _              => "object"
  }

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
    (Set(DateTime.typeId, Timestamp.typeId)
      .contains(out.typeId) && Set(DateTime.typeId, Timestamp.typeId).contains(
      in.typeId
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
    (Set(Varchar.typeId, Text.typeId, Keyword.typeId)
      .contains(out.typeId) && Set(Varchar.typeId, Text.typeId, Keyword.typeId).contains(
      in.typeId
    )) ||
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

  def coerce(in: PainlessScript, to: SQLType, context: Option[PainlessContext]): String = {
    context match {
      case Some(_) =>
        in match {
          case identifier: Identifier =>
            identifier.baseType match {
              case SQLTypes.Any => // in painless context, Any is ZonedDateTime
                to match {
                  case SQLTypes.Date =>
                    identifier.addPainlessMethod(".toLocalDate()")
                  case SQLTypes.Time =>
                    identifier.addPainlessMethod(".toLocalTime()")
                  case _ => // do nothing
                }
              case _ => // do nothing
            }
          case _ => // do nothing
        }
      case _ => // do nothing
    }
    val expr = in.painless(context)
    val from = in.baseType
    val nullable = in.nullable
    coerce(expr, from, to, nullable, context)
  }

  def coerce(
    expr: String,
    from: SQLType,
    to: SQLType,
    nullable: Boolean,
    context: Option[PainlessContext]
  ): String = {
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
          context match {
            case Some(ctx) =>
              ctx.addParam(
                LiteralParam(
                  "LocalDate.parse(" + expr + ", DateTimeFormatter.ofPattern(\"yyyy-MM-dd\"))"
                )
              ) match {
                case Some(p) => return p
                case None    => // continue
              }
            case None => // continue
          }
          "LocalDate.parse(" + expr + ", DateTimeFormatter.ofPattern(\"yyyy-MM-dd\"))"
        case (SQLTypes.Varchar, SQLTypes.Time) =>
          context match {
            case Some(ctx) =>
              ctx.addParam(
                LiteralParam(
                  "LocalTime.parse(" + expr + ", DateTimeFormatter.ofPattern(\"HH:mm:ss\"))"
                )
              ) match {
                case Some(p) => return p
                case None    => // continue
              }
            case None => // continue
          }
          "LocalTime.parse(" + expr + ", DateTimeFormatter.ofPattern(\"HH:mm:ss\"))"
        case (SQLTypes.Varchar, SQLTypes.DateTime) =>
          context match {
            case Some(ctx) =>
              ctx.addParam(
                LiteralParam(s"LocalDateTime.parse($expr, DateTimeFormatter.ISO_DATE_TIME)")
              ) match {
                case Some(p) => return p
                case None    => // continue
              }
            case None => // continue
          }
          s"LocalDateTime.parse($expr, DateTimeFormatter.ISO_DATE_TIME)"
        case (SQLTypes.Varchar, SQLTypes.Timestamp) =>
          context match {
            case Some(ctx) =>
              ctx.addParam(
                LiteralParam(s"ZonedDateTime.parse($expr, DateTimeFormatter.ISO_ZONED_DATE_TIME)")
              ) match {
                case Some(p) => return p
                case None    => // continue
              }
            case None => // continue
          }
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
