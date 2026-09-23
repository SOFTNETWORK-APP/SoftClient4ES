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
  Expr,
  Identifier,
  LiteralParam,
  PainlessContext,
  PainlessOperandForm,
  PainlessScript,
  TokenRegex,
  Updateable
}
import app.softnetwork.elastic.sql.`type`.{
  SQLAny,
  SQLBool,
  SQLNumeric,
  SQLTemporal,
  SQLType,
  SQLTypeUtils,
  SQLTypes
}
import app.softnetwork.elastic.sql.parser.Validator
import app.softnetwork.elastic.sql.query.{Criteria, CriteriaWithConditionalFunction, Expression}

package object cond {

  sealed trait ConditionalOp extends PainlessScript with TokenRegex {
    override def painless(context: Option[PainlessContext] = None): String = sql
  }

  case object Coalesce extends Expr("COALESCE") with ConditionalOp
  case object IsNull extends Expr("ISNULL") with ConditionalOp
  case object IsNotNull extends Expr("ISNOTNULL") with ConditionalOp
  case object NullIf extends Expr("NULLIF") with ConditionalOp
  case object Greatest extends Expr("GREATEST") with ConditionalOp
  case object Least extends Expr("LEAST") with ConditionalOp

  case object Case extends Expr("CASE") with ConditionalOp {

    /** The `WHEN` conditions of every `CASE` in an operand's chain, as the [[Criteria]] they are
      * (issue #384, item 1).
      *
      * 🔴 ONE walk, because two places need it and they must not drift: a SELECT-list / ORDER BY
      * `CASE` in a SEARCH (`SingleSearch.validateResolved`) and a computed column's stored
      * expression in DDL (`schema.validateScriptReferences`). A comparison written inside a CASE is
      * the same comparison as one written in WHERE, and it used to be checked in neither.
      *
      * ⚠️ `Case.args` deliberately EXCLUDES the conditions (it returns the expression, the THEN
      * results and the ELSE), so they are unreachable through the chain and have to be read off
      * `conditions` -- which is exactly why nothing had ever looked at them.
      */
    def conditionsOf(chain: FunctionChain): Seq[Criteria] = {
      // 🔴 ARGUMENTS as well as the chain, and that is not a refinement — it is the difference
      // between refusing a wrong answer and shipping one. `transformFunctions` walks the chain
      // APPLIED TO an operand, so it sees `CASE WHEN … END` but not the CASE inside
      // `ABS(CASE WHEN … END)` or `(CASE WHEN … END) + 1`. MEASURED: the wrapped form was accepted
      // and stored `c = 0.0` for every document (the temporal collapse having erased the
      // `CAST(ts AS TIME)`), while the bare form was correctly refused -- found by review, one
      // function away from the shape the check did see.
      def walk(fn: Function, depth: Int): Seq[Criteria] =
        if (depth > MaxNesting) Nil
        else
          (fn match {
            case c: Case =>
              c.conditions.collect { case (criteria: Criteria, _) => criteria } ++
                c.conditions
                  .collect { case (_, result: FunctionChain) => result }
                  .flatMap(
                    from(_, depth + 1)
                  ) ++
                c.expression.toSeq
                  .collect { case f: FunctionChain => f }
                  .flatMap(from(_, depth + 1))
            case _ => Nil
          }) ++ (fn match {
            case n: FunctionN[_, _] =>
              n.args.collect { case f: FunctionChain => f }.flatMap(from(_, depth + 1))
            case _ => Nil
          })

      def from(c: FunctionChain, depth: Int): Seq[Criteria] =
        if (depth > MaxNesting) Nil
        else FunctionUtils.transformFunctions(c).flatMap(walk(_, depth))

      from(chain, 0)
    }

    /** A depth stop for [[conditionsOf]]. A CASE inside a CASE inside a function is already beyond
      * anything measured; the bound exists so a cyclic or pathological AST cannot hang validation,
      * not because the depth is meaningful.
      */
    private val MaxNesting = 12

  }

  case object WHEN extends Expr("WHEN") with TokenRegex
  case object THEN extends Expr("THEN") with TokenRegex
  case object ELSE extends Expr("ELSE") with TokenRegex
  case object END extends Expr("END") with TokenRegex

  sealed trait ConditionalFunction[In <: SQLType]
      extends TransformFunction[In, SQLBool]
      with FunctionWithIdentifier {
    def conditionalOp: ConditionalOp

    override def fun: Option[PainlessScript] = Some(conditionalOp)

    override def outputType: SQLBool = SQLTypes.Boolean

  }

  case class IsNull(identifier: Identifier) extends ConditionalFunction[SQLAny] {
    override def conditionalOp: ConditionalOp = IsNull

    override def args: List[PainlessScript] = List(identifier)

    override def inputType: SQLAny = SQLTypes.Any

    override def toSQL(base: String): String = sql

    override def checkIfNullable: Boolean = false

    override def toPainlessCall(callArgs: List[String], context: Option[PainlessContext]): String =
      callArgs match {
        case List(arg) =>
          s"${arg.trim} == null" // TODO check when identifier is nullable and has functions
        case _ => throw new IllegalArgumentException("ISNULL requires exactly one argument")
      }

    override def update(request: query.SingleSearch): IsNull = {
      this.copy(identifier = identifier.update(request))
    }
  }

  case class IsNotNull(identifier: Identifier) extends ConditionalFunction[SQLAny] {
    override def conditionalOp: ConditionalOp = IsNotNull

    override def args: List[PainlessScript] = List(identifier)

    override def inputType: SQLAny = SQLTypes.Any

    override def toSQL(base: String): String = sql

    override def checkIfNullable: Boolean = false

    override def toPainlessCall(callArgs: List[String], context: Option[PainlessContext]): String =
      callArgs match {
        case List(arg) =>
          s"${arg.trim} != null" // TODO check when identifier is nullable and has functions
        case _ => throw new IllegalArgumentException("ISNOTNULL requires exactly one argument")
      }

    override def update(request: query.SingleSearch): IsNotNull = {
      this.copy(identifier = identifier.update(request))
    }
  }

  case class Coalesce(values: List[PainlessScript])
      extends TransformFunction[SQLAny, SQLType]
      with FunctionWithIdentifier {
    def operator: ConditionalOp = Coalesce

    override def fun: Option[ConditionalOp] = Some(operator)

    override def args: List[PainlessScript] = values

    override def outputType: SQLType =
      baseType //SQLTypeUtils.leastCommonSuperType(args.map(_.baseType))

    override def identifier: Identifier = Identifier()

    override def inputType: SQLAny = SQLTypes.Any

    override def sql: String = s"$Coalesce(${values.map(_.sql).mkString(", ")})"

    // Reprend l’idée de SQLValues mais pour n’importe quel token
    override def baseType: SQLType = SQLTypeUtils.leastCommonSuperType(argTypes)

    override def validate(): Either[String, Unit] = {
      if (values.isEmpty) Left("COALESCE requires at least one argument")
      else Right(())
    }

    override def checkIfNullable: Boolean = false

    override def toPainlessCall(callArgs: List[String], context: Option[PainlessContext]): String =
      callArgs match {
        case Nil => throw new IllegalArgumentException("COALESCE requires at least one argument")
        // 🔴 `COALESCE(x)` IS `x`. Without this arm the fold below took `values.length - 1 = 0`
        // arguments, so the `" : "` join produced nothing and the tail appended a closing paren that
        // opened nowhere: `String.valueOf( : param1))`, which Painless cannot parse. Degenerate SQL,
        // legal SQL, and UNREACHABLE until issue #367 stopped predicates from dropping the operand's
        // transform — `WHERE COALESCE(d) = 'x'` compared the raw column before and emits this now.
        case single :: Nil => single.trim
        // 🔴 A RIGHT fold, because the old one opened `values.length - 1` parentheses and closed
        // exactly ONE: at two arguments that balances by luck, and at three or more it emitted
        // `(a != null ? a : (b != null ? b : c)` — Painless cannot parse it, so `COALESCE(a, b, c)`
        // was a shard failure in EVERY venue, SELECT included, and no test had ever looked. Found by
        // the delimiter check in `PredicateTransformSurvivalSpec`. Byte-identical at two arguments,
        // which is what every existing fixture pins.
        case _ =>
          callArgs.init.foldRight(callArgs.last.trim) { (arg, rest) =>
            s"(${arg.trim} != null ? ${arg.trim} : $rest)"
          }
      }

    override def nullable: Boolean = values.forall(_.nullable)

    override def update(request: query.SingleSearch): Coalesce = {
      this.copy(values = values.map {
        case u: Updateable => u.update(request).asInstanceOf[PainlessScript]
        case other         => other
      })
    }
  }

  case class NullIf(expr1: PainlessScript, expr2: PainlessScript)
      extends ConditionalFunction[SQLAny] {
    override def conditionalOp: ConditionalOp = NullIf

    override def args: List[PainlessScript] = List(expr1, expr2)

    override def identifier: Identifier = Identifier()

    override def inputType: SQLAny = SQLTypes.Any

    override def baseType: SQLType = SQLTypeUtils.leastCommonSuperType(argTypes)

    /** The type an argument's rendering really PRODUCES -- the question [[receiverIsTime]] three
      * lines below already asks of the receiver, asked here of BOTH arguments (issue #382).
      *
      * 🔴 `FunctionN.argTypes` answers `_.out`, and a schema-resolved `Identifier.out` reports the
      * COLUMN's type, not the chain's. So `NULLIF(CAST(s AS BIGINT), 0)` over a KEYWORD column
      * reported `List(KEYWORD, BIGINT)`, `leastCommonSuperType` came out VARCHAR, and
      * [[toPainlessCall]] took its `case SQLTypes.Varchar` arm -- whose `$arg1 != null` guard over
      * a primitive numeric literal Elasticsearch refuses to compile:
      * {{{
      * def param3 = param2 == null || (0 != null && param2.compareTo(0) == 0) ? null : param2;
      *   class_cast_exception: Cannot cast from [int] to [java.lang.Object].
      * }}}
      * MEASURED on ES 8.18.3, and identical for `CAST`, `TRY_CAST` and `::`. With the chain's type
      * the arm is the numeric one, `param2 == 0 ? null : param2`, which compiles and answers `null`
      * / `125` / `null` for `"0"` / `"125"` / a missing column.
      *
      * This is [[app.softnetwork.elastic.sql.operator.math.ArithmeticExpression]] 's `argTypeOf`
      * (the same issue's OQ-2) in a second function: `argTypes` is the single input to
      * [[baseType]], `baseType` feeds `out`, and `out` is what [[toPainlessCall]] switches on -- so
      * one derivation fixes every venue, and it also stops the expression DESCRIBING itself as a
      * string to a CTAS target mapping.
      *
      * ⚠️ The repair deliberately stops at the TYPE. It does NOT cover `NULLIF(s, 0)`, where the
      * operand really is a KEYWORD and the supertype really is VARCHAR; that shape is a type
      * mismatch, [[validate]] already computes exactly the right `Left` for it, and the reason it
      * never fires is that validation runs at PARSE time -- before a schema is attached, when `s`
      * has no type at all. Widening validation to a resolved statement is a separate change and is
      * RECORDED on #382, not made here. Emitting the comparison without the guard was measured and
      * REJECTED: it turns the compile error into a PER-DOCUMENT runtime failure
      * (`wrong_method_type_exception: cannot convert MethodHandle(String,String)int to
      * (Object,int)Object`), which is strictly worse.
      */
    private[this] def argTypeOf(arg: PainlessScript): SQLType = arg match {
      case i: Identifier => i.chainType
      case other         => other.out
    }

    override def argTypes: List[SQLType] = args.map(argTypeOf)

    /** Whether the value `expr1` RENDERS is a `java.time.LocalTime`. `out` and `expr1.out` both
      * report the column's type; `Identifier.chainType` reports the chain's (#367).
      */
    private[this] def receiverIsTime: Boolean = expr1 match {
      case i: Identifier => i.chainType == SQLTypes.Time
      case other         => other.out == SQLTypes.Time
    }

    private[this] def checkIfExpressionNullable(expr: PainlessScript): Boolean = expr match {
      case f: FunctionChain if f.functions.nonEmpty => true
      case _                                        => false
    }

    override def checkIfNullable: Boolean =
      false //checkIfExpressionNullable(expr1) || checkIfExpressionNullable(expr2)

    /** A rendering that may be spliced into the templates below exactly as it stands: a Painless
      * NAME, or a LITERAL (a number, a quoted string, `true` / `false` / `null` -- all of which the
      * name pattern already covers). Everything else is COMPOUND and must be bound, so the pattern
      * is deliberately narrow: answering "simple" about a compound rendering is the defect, while
      * answering "compound" about a name costs one redundant local.
      */
    private[this] val simpleOperand =
      """(?:[A-Za-z_][A-Za-z0-9_]*|-?\d+(?:\.\d+)?[lLfFdD]?|"(?:[^"\\]|\\.)*")""".r

    /** Bind an argument's rendering to a prologue local unless it is already a simple operand.
      *
      * 🔴 Issue #382. `arg0` appears THREE times in the guarded template and `arg1` twice, both
      * spliced RAW -- and `&&`, `||` and `==` all bind tighter than `?:`, so a COMPOUND rendering
      * re-associates across the template. Two measured consequences, both on ES 8.18.3:
      * {{{
      * NULLIF(UPPER(s), 'X')                         -- arg0 renders `(p == null) ? null : p.toUpperCase()`
      *   (p == null) ? null : p.toUpperCase() == null || ("X" != null && (p == null) ? null : ...)
      *   illegal_argument_exception: Cannot cast null to a primitive type [boolean].   LOUD
      *
      * NULLIF(CASE WHEN n > 1 THEN 1 ELSE 2 END, 1)  -- the NUMERIC arm, `$arg0 == $arg1 ? null : $arg0`
      *   param2 ? 1 : 2 == 1 ? null : param2 ? 1 : 2
      *   reads as `param2 ? 1 : ((2 == 1) ? null : …)`, so n = 5 stored c = 1 where SQL says NULL.
      *   HTTP 200 -- SILENT, the #205 family.                                          SILENT
      * }}}
      * The silent one is why this cannot be narrowed to the arms that fail loudly: the numeric arm
      * is correct only when the compound rendering happens to be a `guard ? null : value`, which is
      * luck, not a rule. Binding is applied at the ONE place both templates read their operands
      * from, so the two cannot drift.
      *
      * Binding also evaluates the operand ONCE (#373 item 9's "duplicated guarded expression"
      * family) -- `NULLIF(UPPER(s), 'X')` called `toUpperCase()` three times per document.
      *
      * ⚠️ With NO context there is no prologue to hoist into. Parentheses are the whole of the
      * association fix, so a context-free EXPRESSION gets them; a rendering that is not an
      * expression at all (a safe cast's `try` / `catch`) is beyond their help and is left exactly
      * as it was -- pre-existing, and not this repair's to change
      * ([[app.softnetwork.elastic.sql.operator.math.ArithmeticExpression]]'s `bind` fallback makes
      * the same distinction).
      */
    private[this] def operand(rendered: String, context: Option[PainlessContext]): String = {
      val trimmed = rendered.trim
      // An EMPTY rendering is what an AGGREGATE answers in a script venue (`NULLIF(COUNT(*), 0)`),
      // and the surrounding emission is already degenerate there. Binding it would only add `def
      // nif1 = ;` to it, so it is left exactly as it was -- pre-existing, recorded on #382.
      if (trimmed.isEmpty || simpleOperand.pattern.matcher(trimmed).matches()) trimmed
      else
        context match {
          case Some(ctx)                                      => ctx.bindLocal(trimmed, "nif")
          case None if PainlessOperandForm.placeable(trimmed) => s"($trimmed)"
          case None                                           => trimmed
        }
    }

    override def toPainlessCall(
      callArgs: List[String],
      context: Option[PainlessContext]
    ): String = {
      callArgs match {
        case List(rendered0, rendered1) =>
          val arg0 = operand(rendered0, context)
          val arg1 = operand(rendered1, context)
          // 🔴 The SECOND argument is guarded too (issue #373, found by review) -- the same hole
          // `checkCase` had, one method up in this file. `NULLIF(CAST(d AS TIME), CAST(ts AS
          // TIME))` called the comparison with a null `arg1` for any document missing `ts`:
          // MEASURED on ES 8.18, `null_pointer_exception: Cannot read field "hour" because
          // "other" is null`.
          //
          // 🔴 And the guard means NOT-EQUAL, not null. SQL says `NULLIF(a, b)` is NULL when
          // `a = b`; with `b` NULL the comparison is UNKNOWN, so the rows are NOT equal and the
          // answer is `a`. Short-circuiting `arg1 == null` to the NULL branch would invert that.
          def nullIf(comparison: String): String =
            s"$arg0 == null || ($arg1 != null && $comparison) ? null : $arg0"
          val expr =
            out match {
              case SQLTypes.Varchar =>
                nullIf(s"$arg0.compareTo($arg1) == 0")
              // 🔴 Keyed on what the RECEIVER's chain RENDERS, not on `out` (issue #373).
              // `LocalTime` has no `isEqual`, and MEASURED on ES 8.18 before this,
              // `NULLIF(CAST(d AS TIME), CAST('00:00:00' AS TIME))` failed the shard in a real
              // `script_fields`. Neither `out` nor `expr1.out` says so: both are TIMESTAMP, the
              // COLUMN's type -- `chainType` is the derivation that describes the rendering
              // (#367), and it answers TIME. Two wrong keys were tried before measuring it.
              case _: SQLTemporal if receiverIsTime =>
                nullIf(s"$arg0.compareTo($arg1) == 0")
              case _: SQLTemporal => nullIf(s"$arg0.isEqual($arg1)")
              // `==` is null-safe in Painless, so a numeric / boolean pair needs no guard.
              case _ => s"$arg0 == $arg1 ? null : $arg0"
            }
          context match {
            case Some(ctx) =>
              ctx.addParam(LiteralParam(expr)) match {
                case Some(e) => return e
                case _       =>
              }
            case _ =>
          }
          expr
        case _ => throw new IllegalArgumentException("NULLIF requires exactly two arguments")
      }
    }

    override def validate(): Either[String, Unit] = {
      for {
        _ <- expr1.validate()
        _ <- expr2.validate()
        _ <- Validator.validateTypesMatching(expr1.out, expr2.out)
      } yield ()
    }

    override def update(request: query.SingleSearch): NullIf = {
      this.copy(
        expr1 = expr1 match {
          case u: Updateable => u.update(request).asInstanceOf[PainlessScript]
          case other         => other
        },
        expr2 = expr2 match {
          case u: Updateable => u.update(request).asInstanceOf[PainlessScript]
          case other         => other
        }
      )
    }
  }

  case class Case(
    expression: Option[PainlessScript],
    conditions: List[(PainlessScript, PainlessScript)],
    default: Option[PainlessScript]
  ) extends TransformFunction[SQLAny, SQLAny] {
    override def args: List[PainlessScript] = expression.toList ++
      conditions.map { case (_, res) => res } ++
      default.toList

    override def inputType: SQLAny = SQLTypes.Any

    override def outputType: SQLAny = SQLTypes.Any

    override def sql: String = {
      val exprPart = expression.map(e => s"$Case ${e.sql}").getOrElse(Case.sql)
      val whenThen = conditions
        .map { case (cond, res) => s"$WHEN ${cond.sql} $THEN ${res.sql}" }
        .mkString(" ")
      val elsePart = default.map(d => s" $ELSE ${d.sql}").getOrElse("")
      s"$exprPart $whenThen$elsePart $END"
    }

    override def baseType: SQLType = SQLTypeUtils.leastCommonSuperType(argTypes)

    override def validate(): Either[String, Unit] = {
      // Story 22.2 (AD-6) — a CASE-WHEN condition is parsed by `case_condition`, which shares
      // `whereCriteria` with WHERE, so a subquery node is grammatically reachable here. It is
      // reached at PARSE time (`Field.validate` -> `FunctionChain.validate` ->
      // `Validator.validateChain` -> `functions.map(_.validate())`), and the check below passes a
      // criteria (its `out` IS BOOLEAN), so without this arm the statement parses and dies later in
      // the node's `painless` throw — a rendering-time internal error where the analyst wants a
      // named rejection.
      val subquery = conditions.collectFirst {
        case (c: Criteria, _) if c.subqueries.nonEmpty => c
      }
      if (subquery.isDefined)
        Left(
          s"A subquery is not supported in a CASE WHEN condition: ${subquery.get.sql}. " +
          "Filter in WHERE, or compute the flag in a separate query."
        )
      else if (conditions.isEmpty) Left("CASE WHEN requires at least one condition")
      else if (
        expression.isEmpty && conditions.exists { case (cond, _) => cond.out != SQLTypes.Boolean }
      )
        Left("CASE WHEN conditions must be of type BOOLEAN")
      else if (
        expression.isDefined && conditions.exists { case (cond, _) =>
          !SQLTypeUtils.matches(cond.out, expression.get.out)
        }
      )
        Left("CASE WHEN conditions must be of the same type as the expression")
      else Right(())
    }

    /** Whether the CASE EXPRESSION -- the receiver of every `WHEN` comparison -- renders a
      * `java.time.LocalTime`. `out` reports the CASE's result type and cannot say (issue #373).
      */
    private[this] def receiverIsTime: Boolean = expression.exists {
      case i: Identifier => i.chainType == SQLTypes.Time
      case other         => other.out == SQLTypes.Time
    }

    /** @param candidateNullable
      *   whether `c` is a BOUND parameter that can hold `null`.
      *
      * 🔴 The CANDIDATE needs a guard too (issue #373). `CASE <expr> WHEN <column> THEN …` emitted
      * `e != null && e.isEqual(c) ? v` and called the comparison with `c == null` for any document
      * missing that column -- MEASURED on ES 8.18: `NullPointerException:
      * ChronoLocalDate.toEpochDay() because "other" is null`. `compareTo` on the `Varchar` arm has
      * exactly the same hole.
      *
      * ⚠️ The candidate is ALWAYS bound before it is guarded, so `c != null` costs one reference
      * and never a second rendering of its chain. See the call site for why no predicate decides
      * it: none of the three the AST offers can see a function-wrapped column's nullability.
      */
    private[this] def checkCase(
      e: String,
      c: String,
      v: String,
      candidateNullable: Boolean
    ): String = {
      val guard = if (candidateNullable) s"$e != null && $c != null" else s"$e != null"
      // 🔴 A TIME is decided by what the RECEIVER RENDERS, not by `out` (issue #373, found by
      // review). `out` is the CASE's RESULT type -- the least common supertype of the expression,
      // the conditions and the results -- and it reports TIMESTAMP for
      // `CASE CAST(d AS TIME) WHEN CAST(ts AS TIME) …` whose operands are both `LocalTime`, so an
      // arm keyed on it was DEAD and that shape still emitted `isEqual`. MEASURED on ES 8.18:
      // `dynamic method [java.time.LocalTime, isEqual/1] not found`, the very failure this PR's
      // first commit exists to remove. `Identifier.chainType` (#367) is the derivation that
      // describes the rendering -- the same key `NULLIF` needed.
      if (receiverIsTime) s"$guard && $e.compareTo($c) == 0 ? $v"
      else
        out match {
          case SQLTypes.Varchar =>
            s"$guard && $e.compareTo($c) == 0 ? $v"
          case _: SQLTemporal =>
            s"$guard && $e.isEqual($c) ? $v"
          // `==` is null-safe in Painless, so a numeric / boolean candidate needs no guard.
          case _ => s"$e == $c ? $v"
        }
    }

    /** A result rendering that can sit opposite a `null` branch.
      *
      * 🔴 Story BIDC-8 (round 10, MEDIUM-3). With no `ELSE`, SQL says the missing branch is NULL —
      * but Painless types a ternary from its BRANCHES, so `param2 ? 1 : null` is
      * `class_cast_exception: Cannot cast from [int] to [java.lang.Object]` (MEASURED live on ES
      * 8.18.3, and MEASURED again to confirm it is the absence of a `return` that makes it fatal:
      * the same expression compiles behind an explicit `return`). `(def)` on the result side is the
      * spelling that compiles in BOTH positions — also measured. Applied only when there is no
      * default, so every existing emission is byte-identical.
      */
    private def boxWhenNoDefault(rendered: String): String =
      // 🔴 Round 11 — PARENTHESISED. `(def)$rendered` casts only the first token, so a result that
      // is itself a ternary came out as `param4 ? (def)param3 ? (def)1 : null : null`, casting the
      // nested CONDITION rather than the value. Benign today (a `def` condition still works) and
      // wrong as written.
      if (default.isEmpty) s"(def)($rendered)" else rendered

    override def painless(context: Option[PainlessContext] = None): String = {
      context match {
        case Some(ctx) =>
          var cases =
            expression match {
              case Some(expr) => // case with expression to evaluate
                val e = SQLTypeUtils.coerce(expr, out, context)
                val expParam = ctx.addParam(
                  LiteralParam(e)
                )
                conditions
                  .map { case (cond, res) =>
                    val name =
                      cond match {
                        case e: Expression =>
                          e.identifier.name
                        case f: FunctionWithIdentifier =>
                          f.identifier.name
                        case i: Identifier =>
                          i.name
                        case _ => ""
                      }
                    val c = SQLTypeUtils.coerce(cond, out, context)
                    val r =
                      boxWhenNoDefault(res match {
                        case i: Identifier if i.name == name && name.nonEmpty =>
                          i.withNullable(false)
                          SQLTypeUtils.coerce(
                            i,
                            out,
                            context
                          )
                        case _ =>
                          SQLTypeUtils.coerce(res, out, context)
                      })
                    expParam match {
                      case Some(e) =>
                        // 🔴 ALWAYS bound, ALWAYS guarded (issue #373, found by review). The
                        // earlier version asked `cond.nullable`, and MEASURED that is false for a
                        // function-wrapped column -- `UPPER(other)` reports `nullable = false`
                        // while rendering `(param3 == null) ? null : param3.toUpperCase()` -- so
                        // `CASE UPPER(name) WHEN UPPER(other) …` was neither bound nor guarded and
                        // threw `null_pointer_exception: Cannot read field "value"` on ES 8.18.
                        //
                        // No predicate decides it any more, because none of the three I measured
                        // is reliable: the wrapper's `name` is EMPTY, its `nullable` is false, and
                        // its function is not a `FunctionWithIdentifier`, so the inner column is
                        // not reachable from here. The unconditional rule is provably safe
                        // instead: binding is a no-op when the candidate is already a name
                        // (`addParam` returns the existing one), and `c != null` on a candidate
                        // that cannot be null is redundant bytes, never a different answer.
                        ctx.addParam(LiteralParam(c)) match {
                          case Some(bound) => checkCase(e, bound, r, candidateNullable = true)
                          case _           => checkCase(e, c, r, candidateNullable = false)
                        }
                      case _ =>
                        checkCase(e, c, r, candidateNullable = false)
                    }
                  }
                  .mkString(" : ")
              case _ =>
                conditions
                  .map { case (cond, res) =>
                    val name =
                      cond match {
                        case e: Expression =>
                          e.identifier.name
                        case f: FunctionWithIdentifier =>
                          f.identifier.name
                        case i: Identifier =>
                          i.name
                        case _ => ""
                      }
                    val c = SQLTypeUtils.coerce(cond, SQLTypes.Boolean, context)
                    val r =
                      boxWhenNoDefault(res match {
                        case i: Identifier if i.name == name && name.nonEmpty =>
                          i.withNullable(false)
                          SQLTypeUtils.coerce(
                            i,
                            out,
                            context
                          )
                        case _ =>
                          SQLTypeUtils.coerce(res, out, context)
                      })
                    if (!cond.isInstanceOf[CriteriaWithConditionalFunction[_]] && cond.nullable) {
                      ctx.addParam(LiteralParam(c)) match {
                        case Some(c) => s"$c ? $r"
                        case _       => s"$c ? $r"
                      }
                    } else {
                      s"$c ? $r"
                    }
                  }
                  .mkString(" : ")
            }
          default match {
            case Some(df) =>
              val d = SQLTypeUtils.coerce(df, out, context)
              if (df.nullable) {
                ctx.addParam(LiteralParam(d)) match {
                  case Some(d) => cases = s"$cases : $d"
                  case _       => cases = s"$cases : $d"
                }
              } else {
                cases = s"$cases : $d"
              }
            // 🔴 Story BIDC-8 (round 10, MEDIUM-3). A `CASE … THEN x END` with NO `ELSE` used to
            // leave the ternary truncated — `param2 ? 1` — which Elasticsearch rejects at compile
            // time: `unexpected token ['<EOF>'] was expecting one of [':']` (MEASURED live on
            // 8.18.3). SQL says the missing ELSE is NULL, so that is what the else-branch emits.
            // Pre-existing; it sits on the surface this story's CASE gate exercises, and the gate
            // missed it because every fixture supplied an ELSE.
            case _ => cases = s"$cases : null"
          }

          cases

        case _ =>
          throw new IllegalArgumentException(s"Painless context is required for $sql")
      }
    }

    override def toPainless(base: String, idx: Int, context: Option[PainlessContext]): String =
      s"$base${painless(context)}"

    override def nullable: Boolean =
      conditions.exists { case (_, res) => res.nullable } || default.forall(_.nullable)

    override def update(request: query.SingleSearch): Case = {
      this.copy(
        expression = expression.map {
          case u: Updateable => u.update(request).asInstanceOf[PainlessScript]
          case other         => other
        },
        conditions = conditions.map { case (cond, res) =>
          val newCond = cond match {
            case u: Updateable => u.update(request).asInstanceOf[PainlessScript]
            case other         => other
          }
          val newRes = res match {
            case u: Updateable => u.update(request).asInstanceOf[PainlessScript]
            case other         => other
          }
          (newCond, newRes)
        },
        default = default.map {
          case u: Updateable => u.update(request).asInstanceOf[PainlessScript]
          case other         => other
        }
      )
    }
  }

  /** N-ary numeric reducer shared by `GREATEST` / `LEAST`.
    *
    * Emits a right-folded nested ternary so a NULL argument is skipped and the whole expression
    * yields NULL only when every argument is NULL: pairwise(x, y) = (x == null ? y : (y == null ? x
    * : Math.{max|min}(x, y)))
    *
    * Output is `SQLNumeric` because `Math.max` / `Math.min` only operate on numeric primitives. The
    * base type narrows to the widest input numeric.
    */
  sealed trait NumericReducer
      extends TransformFunction[SQLAny, SQLNumeric]
      with FunctionWithIdentifier {
    def values: List[PainlessScript]
    def operator: ConditionalOp
    protected def mathFn: String // "Math.max" | "Math.min"

    override def fun: Option[ConditionalOp] = Some(operator)

    override def args: List[PainlessScript] = values

    override def outputType: SQLNumeric = SQLTypes.Numeric

    override def identifier: Identifier = Identifier()

    override def inputType: SQLAny = SQLTypes.Any

    override def baseType: SQLType = SQLTypeUtils.leastCommonSuperType(argTypes) match {
      case n: SQLNumeric => n
      case _             => outputType
    }

    override def sql: String = s"$operator(${values.map(_.sql).mkString(", ")})"

    override def checkIfNullable: Boolean = false

    override def validate(): Either[String, Unit] =
      if (values.isEmpty) Left(s"$operator requires at least one argument")
      else
        // Accept numeric args and still-unresolved args (SQLAny / NULL, which
        // extends SQLAny): a bare field has no known type until it is resolved
        // against the index mapping, so we must not reject it here. Only reject
        // args whose type is *definitively* non-numeric (string, temporal,
        // boolean, …) — those would emit Math.{max,min}() on a non-numeric and
        // fail at ES runtime.
        values.find { v =>
          v.out match {
            case _: SQLNumeric => false
            case _: SQLAny     => false
            case _             => true
          }
        } match {
          case Some(nonNumeric) =>
            Left(
              s"$operator requires numeric arguments but got ${nonNumeric.out} for ${nonNumeric.sql}"
            )
          case None => Right(())
        }

    override def nullable: Boolean = values.forall(_.nullable)

    override def toPainlessCall(
      callArgs: List[String],
      context: Option[PainlessContext]
    ): String = {
      // Pair each rendered arg with its nullability so non-nullable args (e.g.
      // numeric literals, which render inline as primitives) are NOT guarded
      // with `== null` — Painless rejects `<primitive> == null` at compile time.
      // Right-fold pairwise: pairwise(a, pairwise(b, pairwise(c, …))).
      // The combined sub-tree is itself nullable only when BOTH sides can be
      // null, which lets us drop the guard on parent levels too.
      def fold(args: List[(String, Boolean)]): (String, Boolean) =
        args match {
          case Nil =>
            throw new IllegalArgumentException(s"$operator requires at least one argument")
          case (s, n) :: Nil => (s.trim, n)
          case (s, xNullable) :: rest =>
            val x = s.trim
            val (y, yNullable) = fold(rest)
            val expr =
              (xNullable, yNullable) match {
                case (true, true)   => s"($x == null ? $y : ($y == null ? $x : $mathFn($x, $y)))"
                case (true, false)  => s"($x == null ? $y : $mathFn($x, $y))"
                case (false, true)  => s"($y == null ? $x : $mathFn($x, $y))"
                case (false, false) => s"$mathFn($x, $y)"
              }
            // result is null only if every branch can be null
            (expr, xNullable && yNullable)
        }

      callArgs match {
        case Nil =>
          throw new IllegalArgumentException(s"$operator requires at least one argument")
        case x :: Nil => x
        case _        => fold(callArgs.zip(values.map(_.nullable)))._1
      }
    }
  }

  case class Greatest(values: List[PainlessScript]) extends NumericReducer {
    override def operator: ConditionalOp = Greatest
    override protected def mathFn: String = "Math.max"

    override def update(request: query.SingleSearch): Greatest =
      this.copy(values = values.map {
        case u: Updateable => u.update(request).asInstanceOf[PainlessScript]
        case other         => other
      })
  }

  case class Least(values: List[PainlessScript]) extends NumericReducer {
    override def operator: ConditionalOp = Least
    override protected def mathFn: String = "Math.min"

    override def update(request: query.SingleSearch): Least =
      this.copy(values = values.map {
        case u: Updateable => u.update(request).asInstanceOf[PainlessScript]
        case other         => other
      })
  }
}
