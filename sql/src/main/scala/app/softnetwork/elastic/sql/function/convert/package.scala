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

package app.softnetwork.elastic.sql.function

import app.softnetwork.elastic.sql.{
  query,
  Alias,
  DateMathRounding,
  Expr,
  Identifier,
  PainlessContext,
  PainlessScript,
  TokenRegex,
  Updateable
}
import app.softnetwork.elastic.sql.`type`.{SQLType, SQLTypeUtils, SQLTypes}

package object convert {

  sealed trait Conversion extends TransformFunction[SQLType, SQLType] with DateMathRounding {
    override def toSQL(base: String): String = sql

    def value: PainlessScript
    def targetType: SQLType
    def safe: Boolean

    override def inputType: SQLType = value.baseType
    override def outputType: SQLType = targetType

    override def args: List[PainlessScript] = List.empty

    //override def nullable: Boolean = value.nullable

    override def painless(context: Option[PainlessContext] = None): String =
      SQLTypeUtils.coerce(value, targetType, context)

    /** A conversion renders as an expression -- EXCEPT the one case `toPainless` below folds: an
      * un-typed (no schema) identifier narrowed to DATE or TIME gets `.toLocalDate()` /
      * `.toLocalTime()` appended to its parameter OBJECT. The default test reads `painless(None)`,
      * an expression, and said "never" -- so `CAST(x AS DATE) - INTERVAL 3 DAY` keyed to the raw
      * column, collapsed with a bare `x` in the same script, and `THEN x` returned `x - 3 days`
      * (issue #370, found by review). Same predicate as the fold, stated once.
      */
    override def foldsOntoOperand: Boolean =
      value.isInstanceOf[Identifier] && inputType == SQLTypes.Any &&
      (outputType == SQLTypes.Date || outputType == SQLTypes.Time)

    /** A conversion is the definition of PRODUCING a type: `CAST(d AS DATE)` renders a `LocalDate`
      * whatever its receiver was. Unconditional, and deliberately NOT restricted to DATE/TIME the
      * way `foldsOntoOperand` above is — `CAST(d AS TIMESTAMP)` produces a `ZonedDateTime` just as
      * definitely, it merely happens to coincide with a date column's own rendering (issue #384).
      */
    override def producesOutputJavaType: Boolean = true

    /** A SAFE conversion turns a non-null operand into a null when the conversion fails: the
      * hoisted local stays at its initial `null` and nothing throws. That is a nullability source
      * the raw column parameter does not carry, and `FunctionN.painless` guards on it (issue #382).
      * A plain `CAST` throws instead, so it does not qualify.
      */
    override def rendersNullOnFailure: Boolean = safe

    override def toPainless(base: String, idx: Int, context: Option[PainlessContext]): String = {
      context match {
        case Some(ctx) =>
          // 🔴 A receiver that ALREADY renders a `LocalDate` must not be narrowed again (issue
          // #370, found by review). A comparison against a DATE literal appends `.toLocalDate()`
          // to the column's parameter object before this chain renders; re-coercing it as
          // TIMESTAMP -> DATE emitted `param1.toInstant()…` onto a `LocalDate` and failed the
          // shard -- for the plain BI filter `CAST(d AS DATE) = CAST('2025-01-05' AS DATE)` on
          // `main` already (the old interval fold DISCARDED this coercion, which is the only
          // reason `CAST(d AS DATE) - INTERVAL 3 DAY = …` worked there). The receiver is what
          // its parameter's methods have rendered it into -- `rendersLocalDate`, the same rule
          // `DateTrunc` reads -- and when it is already the target, the conversion is identity.
          // ...and the same for TIME: `CAST(ts AS TIME) + INTERVAL 1 HOUR > CAST('07:00:00' AS
          // TIME)` was right on `main` for the same accidental reason and re-narrowed a
          // `LocalTime` here (review).
          val receiver = ctx.find(base)
          if (outputType == SQLTypes.Date && receiver.exists(_.rendersLocalDate)) return base
          if (outputType == SQLTypes.Time && receiver.exists(_.rendersLocalTime)) return base
          value match {
            case _: Identifier =>
              inputType match {
                case SQLTypes.Any =>
                  ctx.find(base) match {
                    case Some(identifier) =>
                      outputType match {
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
        case _ => // do nothing
      }
      val ret = SQLTypeUtils.coerce(base, value.baseType, targetType, value.nullable, context)
      val bloc = ret.startsWith("{") && ret.endsWith("}")
      val retWithBrackets = if (bloc) ret else s"{ $ret }"
      if (!safe) ret
      else
        context match {
          // 🔴 Issue #367 — a safe cast is a `try`/`catch` STATEMENT, because Painless has no
          // expression-level try. As the tail of a whole script that is fine (a `script_fields`
          // projection of `TRY_CAST(num AS INT)` returns 6/9/12 on real ES 8.18), but an OPERAND
          // cannot host a statement: once a predicate stopped dropping the conversion, `WHERE
          // TRY_CAST(num AS INT) = 6` emitted `def left1 = try { … };` and Elasticsearch answered
          // `script_exception: compile error`. With a context the statements are hoisted into the
          // script PROLOGUE and the operand is a plain name, so EVERY venue gets an expression.
          //
          // ⚠️ `return null` cannot travel to the prologue — it would leave the whole script — so
          // the hoisted form leaves the local at its initial `null`, which is the same answer for
          // every consumer: a failed safe cast is NULL.
          //
          // 🔴 THE BOUNDARY IS "IS THERE A PROLOGUE", AND NOTHING ELSE (issue #382).
          //
          // A context-free rendering keeps the statement form byte-for-byte: there is nowhere to
          // hoist to, it is only ever the tail of a whole script, and `ConversionTargetTypeSpec` /
          // `NarrowingCastSpec` pin it. Every CONTEXT has a prologue, so every context hoists.
          //
          // ⚠️ #367 restricted this to QUERY on the ground that a PROCESSOR (ingest) or TRANSFORM
          // (materialized-view) script is PERSISTED and diffed — `ScriptProcessor.source` is read
          // back from `_meta` and compared — so "nothing is gained by moving those bytes". That
          // argument expired with #382: it held only while a safe cast in a computed column was
          // broken anyway, and #382 is the repair. No stored processor can churn under this change,
          // because a table whose computed column used `TRY_CAST` could never be CREATED — the
          // assignment landed inside the `catch` and Elasticsearch answered `compile error`.
          case Some(ctx) if !bloc =>
            ctx.bindLocalWith("safe")(name =>
              s"def $name = null; try { $name = $ret; } catch (Exception e) {} "
            )
          case _ => s"try $retWithBrackets catch (Exception e) { return null; }"
        }
    }

    override def roundingScript: Option[String] = DateMathRounding(targetType)

    override def dateMathScript: Boolean = isTemporal
  }

  case object Cast extends Expr("CAST") with TokenRegex

  case object TryCast extends Expr("TRY_CAST") with TokenRegex {
    override def words: List[String] = List(sql, "SAFE_CAST")
  }

  case class Cast(
    value: PainlessScript,
    targetType: SQLType,
    as: Boolean = true,
    safe: Boolean = false
  ) extends Conversion {
    override def sql: String = {
      val ret = s"${value.sql} ${if (as) s"$Alias " else ""}$targetType"
      if (safe) s"$TryCast($ret)"
      else s"$Cast($ret)"
    }
    value.cast(targetType)

    override def update(request: query.SingleSearch): Conversion = {
      value match {
        case updatable: Updateable =>
          this.copy(value = updatable.update(request).asInstanceOf[PainlessScript])
        case _ => this
      }
    }
  }

  case object CastOperator extends Expr("\\:\\:") with TokenRegex

  case class CastOperator(value: PainlessScript, targetType: SQLType) extends Conversion {
    override def sql: String = s"${value.sql}::$targetType"

    override def safe: Boolean = false

    value.cast(targetType)

    override def update(request: query.SingleSearch): CastOperator = {
      value match {
        case updatable: Updateable =>
          this.copy(value = updatable.update(request).asInstanceOf[PainlessScript])
        case _ => this
      }
    }
  }

  case object Convert extends Expr("CONVERT") with TokenRegex

  case class Convert(value: PainlessScript, targetType: SQLType, transactSql: Boolean = false)
      extends Conversion {
    override def sql: String = {
      if (transactSql) s"$Convert($targetType, ${value.sql})"
      else s"$Convert(${value.sql}, $targetType)"
    }

    override def safe: Boolean = false

    value.cast(targetType)

    override def update(request: query.SingleSearch): Convert = {
      value match {
        case updatable: Updateable =>
          this.copy(value = updatable.update(request).asInstanceOf[PainlessScript])
        case _ => this
      }
    }
  }
}
