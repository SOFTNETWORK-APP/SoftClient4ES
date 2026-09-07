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

import app.softnetwork.elastic.sql.{
  painlessUtcLocalDate,
  painlessUtcLocalTime,
  Identifier,
  LiteralParam,
  PainlessContext,
  PainlessScript
}
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

    /** The null guard the four temporal arms below must apply THEMSELVES, because they return from
      * inside `ctx.addParam(...)` and so never reach the wrapper at the end of this method. Kept as
      * one definition rather than four copies — the escape rule this file already learned the hard
      * way (one key, one derivation).
      */
    /** 🔴 The UTC normalisation belongs to a QUERY script and only to one.
      *
      * In a query, the operand of a temporal conversion is what Elasticsearch hands Painless for a
      * `date` field — a `ZonedDateTime` on ES 7+, a `JodaCompatibleZonedDateTime` on 6.8 — so it
      * must be normalised through `painlessUtcZonedDateTime` (see there for the measurement).
      *
      * In a PROCESSOR (ingest pipeline) script it is `ctx.<field>`: the raw JSON scalar of the
      * document being indexed, before Elasticsearch has parsed it into anything. `toInstant()` is
      * not defined on it, and emitting the chain there both breaks the script and — because the
      * rendered processor no longer matches the stored one — makes `ALTER` re-create a processor
      * nobody changed (recorded as `local-21.8-alter-churns-unchanged-pipeline-processor.md`).
      *
      * 🔴 The rule, at the precision it actually holds — it is about RUNTIME TYPE, not context.
      *
      * The mapped type describes what `doc['f'].value` hands a QUERY script. In an INGEST script
      * the operand is `ctx.<field>`: the raw JSON value of the document being indexed, so its
      * runtime type is the JSON type, not what Elasticsearch would have parsed it into.
      *
      *   - a `keyword` is a `String` in BOTH — so `CAST(code AS BIGINT)` emits `Long.parseLong` in
      *     either context and is deliberately NOT guarded here;
      *   - a `date` is a temporal object in a query and a raw string/number in `ctx` — so an arm
      *     keyed on a TIMESTAMP SOURCE must not fire in ingest context.
      *
      * So the guard is not "ingest ignores the mapped type". It is narrower: an arm whose SOURCE
      * type has a different JSON representation — the temporal ones — must not fire in ingest
      * context. Every other arm is untouched.
      *
      * This is the SAME discriminator `function/package.scala` already guards its own date-method
      * injection with (`case SQLTypes.Any if !ctx.isProcessor`), and it is reused rather than
      * re-derived: one key, one derivation.
      *
      * 🔴 The guard makes the arms NOT MATCH in a processor context, so the conversion falls to the
      * identity arm at the bottom of the match. That is deliberate and it is the whole point: it
      * restores the emission BYTE-FOR-BYTE to what it was before #306 made this column resolve to
      * `Timestamp`. Verified at `d1eef583`, where `profile.join_date` read back as `Date`, so
      * `(Date, Date)` matched no temporal arm and returned `expr` untouched. Emitting
      * `.toLocalDate()` here instead would ALSO differ from what is stored, and the ALTER churn
      * would survive — the ingest script is unchanged, so its render must be unchanged too.
      *
      * The rule, stated once: **#306's mapped type governs QUERY scripts; an ingest script keeps
      * the DECLARED-type behaviour**, because `ctx.<field>` is the raw document value, whose shape
      * follows what the user declared rather than what the mapping hands a query.
      */
    val isProcessorContext: Boolean = context.exists(_.isProcessor)

    def temporalGuard(body: String): String =
      if (nullable) s"($expr != null ? $body : null)" else body

    val ret = {
      (from, to) match {
        // ---- DATE & TIME ----
        case (SQLTypes.Date, SQLTypes.DateTime | SQLTypes.Timestamp) =>
          s"$expr.atStartOfDay(ZoneId.of('Z'))"
        // 🔴 TIMESTAMP and DATETIME are split, and the split is the rule rather than an exception.
        // A TIMESTAMP-typed operand is what an Elasticsearch `date` field resolves to (#306), so it
        // is whatever ES hands Painless — a `ZonedDateTime` on 7/8/9, a
        // `JodaCompatibleZonedDateTime` on 6.8 — and every conversion over it normalises through
        // `painlessUtcZonedDateTime` first. BOTH conversions, not just the one that happened to
        // fail: `toLocalDate()` exists on the 6.8 class and `toLocalTime()` does not, and a rule
        // that follows that accident is a rule with an "except" in it.
        //
        // A DATETIME-typed operand must NOT be normalised: `coerce`'s `(varchar, DateTime)` arm
        // emits `LocalDateTime.parse(...)`, and `LocalDateTime` has no zero-argument `toInstant()`,
        // so the chain would be a compile error inside Elasticsearch. `toLocalDate()` /
        // `toLocalTime()` work directly on it.
        case (SQLTypes.Timestamp, SQLTypes.Date) if !isProcessorContext =>
          s"$expr$painlessUtcLocalDate"
        case (SQLTypes.Timestamp, SQLTypes.Time) if !isProcessorContext =>
          s"$expr$painlessUtcLocalTime"
        case (SQLTypes.DateTime, SQLTypes.Date) =>
          s"$expr.toLocalDate()"
        case (SQLTypes.DateTime, SQLTypes.Time) =>
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
        //
        // 🔴 These four arms `return` from INSIDE `ctx.addParam(...)`, so they never reach the
        // nullable wrapper at the end of this method — and issue #306, which made them reachable
        // for a COLUMN operand, is what turned that into a live defect: a document merely MISSING
        // the field yields `LocalDate.parse(null, ...)` -> NullPointerException -> all shards
        // failed. The numeric arms fall through and are guarded; these must guard themselves.
        //
        // The guard goes on the param BODY, not on the param reference: the parse runs when the
        // param is EVALUATED, so guarding the reference site would be too late. Each arm therefore
        // owns its whole result and returns it, which also keeps the end-of-method wrapper from
        // applying a second time. No boxing — every one of these returns a reference.
        case (_: SQLVarchar, SQLTypes.Date) =>
          val guarded = temporalGuard(
            "LocalDate.parse(" + expr + ", DateTimeFormatter.ofPattern(\"yyyy-MM-dd\"))"
          )
          context match {
            case Some(ctx) =>
              ctx.addParam(LiteralParam(guarded)) match {
                case Some(p) => return p
                case None    => // continue
              }
            case None => // continue
          }
          return guarded
        case (_: SQLVarchar, SQLTypes.Time) =>
          val guarded = temporalGuard(
            "LocalTime.parse(" + expr + ", DateTimeFormatter.ofPattern(\"HH:mm:ss\"))"
          )
          context match {
            case Some(ctx) =>
              ctx.addParam(LiteralParam(guarded)) match {
                case Some(p) => return p
                case None    => // continue
              }
            case None => // continue
          }
          return guarded
        case (_: SQLVarchar, SQLTypes.DateTime) =>
          val guarded = temporalGuard(
            s"LocalDateTime.parse($expr, DateTimeFormatter.ISO_DATE_TIME)"
          )
          context match {
            case Some(ctx) =>
              ctx.addParam(LiteralParam(guarded)) match {
                case Some(p) => return p
                case None    => // continue
              }
            case None => // continue
          }
          return guarded
        case (_: SQLVarchar, SQLTypes.Timestamp) =>
          val guarded = temporalGuard(
            s"ZonedDateTime.parse($expr, DateTimeFormatter.ISO_ZONED_DATE_TIME)"
          )
          context match {
            case Some(ctx) =>
              ctx.addParam(LiteralParam(guarded)) match {
                case Some(p) => return p
                case None    => // continue
              }
            case None => // continue
          }
          return guarded

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
    // 🔴 `(def)` is load-bearing, and issue #306 is what made it necessary. Painless must unify the
    // two branches of a ternary, and MOST conversion arms produce a PRIMITIVE — `((long) x)`,
    // `Long.parseLong(x).longValue()`, `Integer.parseInt(x).intValue()`. A primitive cannot unify
    // with `null`, so `(x != null ? ((long) x) : null)` is a script COMPILE ERROR:
    //
    //   class_cast_exception: Cannot cast from [long] to [java.lang.Object].
    //
    // (measured on real Elasticsearch 8.18). Casting to `def` boxes the primitive and the branches
    // unify. It is a no-op for the arms that already return a reference (`LocalDate.parse(...)`,
    // `String.valueOf(...)`).
    //
    // This was LATENT and unreachable before #306: `nullable` is true only for an identifier
    // operand, and until the schema reached the execution path every identifier arrived with
    // `baseType = Any`, so NO arm fired for a column and the ternary never wrapped a primitive.
    // Literal operands are `nullable = false` and take the `return ret` above, which is why every
    // literal case passed both before and after.
    // Boxed ONLY when the arm produced a PRIMITIVE, which is exactly when the target is a ranked
    // numeric: `Integer.parseInt(x).intValue()`, `((long) x)`, `.toEpochMilli()`. The temporal and
    // literal arms already return references (`LocalDate.parse(...)`, `String.valueOf(...)`) and
    // must stay BYTE-IDENTICAL — a blanket `(def)` moved a bridge pin that was never broken, which
    // is churn, not a correction.
    if (producesPainlessPrimitive(to)) s"($expr != null ? (def)($ret) : null)"
    else s"($expr != null ? $ret : null)"
  }

  private val numericRank: Map[Class[_], Int] = Map(
    classOf[SQLTinyInt]  -> 1,
    classOf[SQLSmallInt] -> 2,
    classOf[SQLInt]      -> 3,
    classOf[SQLBigInt]   -> 4,
    classOf[SQLReal]     -> 5,
    classOf[SQLDouble]   -> 6
  )

  /** Does an arm targeting `to` return a PAINLESS PRIMITIVE, and therefore need boxing before the
    * null guard wraps it?
    *
    * This is the property that actually matters — a primitive cannot unify with `null` in a ternary
    * — so it is asked directly rather than through the `numericRankOf(to).isDefined` proxy the
    * first version used. The proxy was keyed by CLASS, so `SQLTypes.Numeric` fell outside it while
    * the `Boolean -> Numeric | Int` arm returns a primitive `int`: a shape the review could not
    * reach through a nullable operand today, but one a future arm could.
    *
    * `painlessType` is the single existing derivation of "the Painless type of an SQLType", so the
    * question is asked of it rather than by re-listing the primitives here.
    */
  private val painlessPrimitives: Set[String] =
    Set("byte", "short", "int", "long", "float", "double", "boolean", "char")

  private def producesPainlessPrimitive(to: SQLType): Boolean =
    painlessPrimitives.contains(painlessType(to))

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
