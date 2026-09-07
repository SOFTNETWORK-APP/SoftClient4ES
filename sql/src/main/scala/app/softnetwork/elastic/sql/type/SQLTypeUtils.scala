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
    case Null                       => "null"
    case TinyInt | Array(TinyInt)   => "byte"
    case SmallInt | Array(SmallInt) => "short"
    case Int | Array(Int)           => "integer"
    case BigInt | Array(BigInt)     => "long"
    case Double | Array(Double)     => "double"
    case Real | Array(Real)         => "float"
    case Numeric                    => "scaled_float"
    // `Char` joins `Varchar`, not `Keyword`: `text` is what the DDL documentation already promises
    // for a character column. Without this arm `Char` fell to `case _ => "object"`, so
    // `CREATE TABLE t (c CHAR)` silently created an OBJECT-typed Elasticsearch field. That was
    // nearly unreachable while bare `CHAR` was the only spelling; story 21.5 accepts `CHAR(n)`,
    // which is the spelling everybody writes, so advertising it without this arm would route new
    // traffic into a persisted wrong mapping.
    case Varchar | Text | Char                      => "text"
    case Array(Varchar) | Array(Text) | Array(Char) => "text"
    case Keyword | Array(Keyword)                   => "keyword"
    case Boolean | Array(Boolean)                   => "boolean"
    case Date                                       => "date"
    case Time                                       => "date"
    case DateTime                                   => "date"
    case Timestamp                                  => "date"
    case Temporal                                   => "date"
    case Array(Date) | Array(Time) | Array(DateTime) | Array(Timestamp) | Array(Temporal) =>
      "date"
    case GeoPoint | Array(GeoPoint) => "geo_point"
    case Array(Struct)              => "nested"
    case Struct                     => "object"
    case VarBinary                  => "binary"
    case _                          => "object"
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
    // `exists(_.isInstanceOf[SQLVarchar])`, not `contains(Varchar)`: the case-object form missed
    // `Text` and `Keyword`, which is what EVERY Elasticsearch string field maps to — the same
    // case-object-equality hole this file carried in `coerce`'s source arms, 100 lines down. It
    // became REACHABLE when story 21.5 made `CAST(x AS TEXT)` / `AS KEYWORD` legal targets, so
    // `COALESCE(CAST(a AS TEXT), CAST(b AS KEYWORD))` fell through every branch to `SQLTypes.Any`.
    //
    // `Varchar` is returned rather than the encountered subtype on purpose: this is the LEAST
    // COMMON super type of a mixed set, and `SQLVarchar` is exactly what Text and Keyword share.
    if (distinct.exists(_.isInstanceOf[SQLVarchar])) return SQLTypes.Varchar

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
      case Some(ctx) =>
        in match {
          case identifier: Identifier =>
            identifier.originalType match {
              case SQLTypes.Any if !ctx.isProcessor => // in painless context, Any is ZonedDateTime
                to match {
                  case SQLTypes.Date =>
                    identifier.addPainlessMethod(".toLocalDate()")
                  case SQLTypes.Time =>
                    identifier.addPainlessMethod(".toLocalTime()")
                  case _ => // do nothing
                }
              case SQLTypes.Any if ctx.isProcessor =>
                to match {
                  case SQLTypes.DateTime | SQLTypes.Timestamp =>
                    val expr = identifier.painless(context)
                    val from = SQLTypes.BigInt
                    val ret = coerce(expr, from, to, identifier.nullable, context)
                    return ret
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

        // ---- NUMERIC NARROWING ----
        // There were WIDENING arms only, so every narrowing pair fell to the identity fallback at
        // the bottom of this match: `CAST(1.9 AS INT)` emitted `1.9` and `CAST(<double col> AS
        // BIGINT)` emitted the raw doc value. A cast that silently does nothing.
        //
        // Rank-guarded rather than thirteen enumerated pairs, and deliberately so: the thing an
        // enumeration would spell out a SECOND time is "the Painless primitive of a numeric
        // SQLType", which `painlessType` already IS. One key, one derivation — the defect that
        // recurred four times in story 21.3. `numericRank` already encodes exactly this lattice
        // (TinyInt 1 ... Double 6, Java's widening order), so a new numeric type joins both halves
        // at once instead of silently missing the narrowing half.
        //
        // Semantics: EXACTLY Java/Painless cast semantics (the lead-approved rule), which are TWO
        // different behaviours and must not be glossed as one:
        //   - float/double -> integral TRUNCATES TOWARD ZERO: `(int) 1.9` is `1`, `(int) -1.9` is
        //     `-1`;
        //   - integral -> narrower integral DISCARDS THE HIGH-ORDER BITS, i.e. it WRAPS:
        //     `(byte) 300` is `44`, `(short) 70000` is `4464`. It does NOT clamp and does not raise.
        // The real divergence to document is therefore the second one: an engine that clamps an
        // out-of-range narrowing to the target's bounds, or rejects it, disagrees with this one.
        // Recorded in functions_type_conversion.md rather than hidden.
        //
        // (An earlier revision of this comment said "truncation toward zero" for BOTH cases. That
        // is true only of the float case; the implementation was always right, the gloss was not.)
        //
        // `painlessType`'s `case _ => "Object"` default is unreachable from here by construction:
        // the guard admits only types `numericRankOf` knows, and every one of those has a
        // primitive arm.
        case (f, t) if isNumericNarrowing(f, t) =>
          s"((${painlessType(t)}) $expr)"

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

        // ---- LITERAL (VARCHAR / TEXT / KEYWORD) -> NUMERIC ----
        // 🔴 These arms matched the `SQLTypes.Varchar` case OBJECT, and no Elasticsearch mapping
        // ever reports VARCHAR — `SQLTypes.apply(String)` maps every string field to `Text` or
        // `Keyword`. So the one arm that worked was unreachable from a real index and
        // `CAST(name AS BIGINT)` fell to the identity fallback below, emitting the RAW STRING: a
        // cast that silently did nothing. `_: SQLVarchar` covers Varchar/Text/Keyword and nothing
        // else — `SQLChar` deliberately stays out, because no ES mapping produces it.
        case (_: SQLVarchar, SQLTypes.Int) =>
          s"Integer.parseInt($expr).intValue()"
        case (_: SQLVarchar, SQLTypes.BigInt) =>
          s"Long.parseLong($expr).longValue()"
        case (_: SQLVarchar, SQLTypes.Double) =>
          s"Double.parseDouble($expr).doubleValue()"
        case (_: SQLVarchar, SQLTypes.Real) =>
          s"Float.parseFloat($expr).floatValue()"
        case (_: SQLVarchar, SQLTypes.SmallInt) =>
          s"Short.parseShort($expr).shortValue()"
        case (_: SQLVarchar, SQLTypes.TinyInt) =>
          s"Byte.parseByte($expr).byteValue()"

        // ---- LITERAL (VARCHAR / TEXT / KEYWORD) -> TEMPORAL ----
        // Same case-object-equality defect as the numeric arms above, same widening.
        case (_: SQLVarchar, SQLTypes.Date) =>
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
        case (_: SQLVarchar, SQLTypes.Time) =>
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
        case (_: SQLVarchar, SQLTypes.DateTime) =>
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
        case (_: SQLVarchar, SQLTypes.Timestamp) =>
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

        // ---- Any -> VARCHAR / CHAR / TEXT / KEYWORD ----
        // Was `case (_, SQLTypes.Varchar)` — case-object equality, so CHAR fell through to the
        // identity fallback and `CAST(1 AS CHAR)` emitted the number `1` where a string was asked
        // for. `SQLLiteral`'s complete implementor set is {SQLVarchar, SQLChar} plus EsqlText and
        // EsqlKeyword (which extend SQLVarchar) — no temporal, numeric, boolean, array, struct or
        // binary type mixes it in — so the widening is tight, and the identity arm above still
        // keeps `Varchar -> Varchar` an identity.
        //
        // Not optional: story 21.5 makes CHAR/TEXT/KEYWORD newly REACHABLE from a DQL cast, and
        // widening the door without fixing the room behind it routes new traffic into a silent
        // wrong answer (the #218 lesson — a parse fix can upgrade a loud failure into a silent
        // corruption).
        case (_, _: SQLLiteral) =>
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

  private val numericRank: Map[Class[_], Int] = Map(
    classOf[SQLTinyInt]  -> 1,
    classOf[SQLSmallInt] -> 2,
    classOf[SQLInt]      -> 3,
    classOf[SQLBigInt]   -> 4,
    classOf[SQLReal]     -> 5,
    classOf[SQLDouble]   -> 6
  )

  private def numericRankOf(t: SQLType): Option[Int] =
    numericRank.collectFirst { case (cls, rank) if cls.isInstance(t) => rank }

  /** True when both sides are ranked numerics and the target is STRICTLY lower in the lattice.
    *
    * 🔴 Used by `coerce` only. `canConvert`'s step 3 asks a different question about the same ranks
    * — whether a SCHEMA EVOLUTION may change a column's type — and its "expansion only" rule must
    * NOT be relaxed to match: loosening it would silently legalise narrowing column-type changes in
    * an ALTER diff (`schema/TableDiff.scala` is its only production consumer). The two rules share
    * the lattice and nothing else.
    */
  private def isNumericNarrowing(from: SQLType, to: SQLType): Boolean =
    (numericRankOf(from), numericRankOf(to)) match {
      case (Some(r1), Some(r2)) => r2 < r1
      case _                    => false
    }

  def canConvert(from: SQLType, to: SQLType): Boolean = {

    // 1. Identity
    if (from.typeId == to.typeId) return true

    // 2. ANY / NULL
    if (to.isInstanceOf[SQLAny]) return true
    if (from.isInstanceOf[SQLNull]) return true

    // 3. Numerics
    (numericRankOf(from), numericRankOf(to)) match {
      case (Some(r1), Some(r2)) =>
        return r1 <= r2 // expansion only
      case _ =>
    }

    // 4. Literals (VARCHAR, CHAR, TEXT, KEYWORD)
    if (from.isInstanceOf[SQLLiteral] && to.isInstanceOf[SQLLiteral])
      return true

    // 5. Booleans
    if (from.isInstanceOf[SQLBool] && to.isInstanceOf[SQLBool])
      return true

    // 6. Temporals
    if (from.isInstanceOf[SQLTemporal] && to.isInstanceOf[SQLTemporal])
      return true

    // 7. Arrays
    (from, to) match {
      case (f: SQLArray, t: SQLArray) =>
        canConvert(f.elementType, t.elementType)
      case _ =>
    }

    // 8. Structs
    (from, to) match {
      case (f: SQLStruct, t: SQLStruct) =>
        structConvertible(f, t)
      case _ =>
    }

    false
  }

  private def structConvertible(from: SQLStruct, to: SQLStruct): Boolean = {
    // TODO To be refined according to our definition of SQLStruct
    true
  }
}
