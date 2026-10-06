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
  renderedTypeOf,
  BooleanValue,
  ComparisonRule,
  Expr,
  GenericIdentifier,
  Identifier,
  LiteralParam,
  NumericValue,
  PainlessContext,
  PainlessOperandForm,
  PainlessScript,
  StringValue,
  TokenRegex,
  Updateable
}
import app.softnetwork.elastic.sql.`type`.{
  SQLAny,
  SQLBigInt,
  SQLBool,
  SQLInt,
  SQLNumeric,
  SQLSmallInt,
  SQLTemporal,
  SQLTinyInt,
  SQLType,
  SQLTypeUtils,
  SQLTypes,
  SQLVarchar
}
import app.softnetwork.elastic.sql.operator.math.ArithmeticExpression
import app.softnetwork.elastic.sql.parser.Validator
import app.softnetwork.elastic.sql.schema.Column
import app.softnetwork.elastic.sql.query.{
  Criteria,
  CriteriaWithConditionalFunction,
  Expression,
  TemporalLiterals
}

package object cond {

  sealed trait ConditionalOp extends PainlessScript with TokenRegex {
    override def painless(context: Option[PainlessContext] = None): String = sql
  }

  case object Coalesce extends Expr("COALESCE") with ConditionalOp
  case object IsNull extends Expr("ISNULL") with ConditionalOp
  case object IsNotNull extends Expr("ISNOTNULL") with ConditionalOp
  case object NullIf extends Expr("NULLIF") with ConditionalOp {

    /** Every `NULLIF` in an operand's chain whose two arguments do not RENDER comparable types, as
      * the rejection each one earns (issue #382).
      *
      * 🔴 Asked of a SCHEMA-RESOLVED expression and of nothing else, for the same reason
      * [[Case.conditionsOf]] 's consumer is: without a schema a bare column's type is `Any`, which
      * matches everything, so `NULLIF(s, 0)` is indistinguishable at parse time from a legal
      * `NULLIF(n, 0)`. `NullIf.validate()` runs there and cannot see it.
      *
      * ONE walk, [[functionsOf]], shared with `Case` -- a `NULLIF` nested in another `NULLIF`'s
      * argument, in a `CASE` branch or under `ABS(...)` is the same `NULLIF`.
      */
    def mismatchesOf(chain: FunctionChain): Seq[String] =
      functionsOf(chain).collect { case n: NullIf => n }.flatMap(_.typeMismatchError)
  }
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
    def conditionsOf(chain: FunctionChain): Seq[Criteria] =
      functionsOf(chain).collect { case c: Case => c }.flatMap {
        _.conditions.collect { case (criteria: Criteria, _) => criteria }
      }

  }

  /** Every function reachable from `chain`: the chain itself, the arguments of every `FunctionN` in
    * it, and -- because `Case.args` deliberately excludes them -- a `CASE`'s `THEN` results and its
    * `CASE <expr>` operand.
    *
    * 🔴 ARGUMENTS as well as the chain, and that is not a refinement -- it is the difference
    * between refusing a wrong answer and shipping one. `transformFunctions` walks the chain APPLIED
    * TO an operand, so it sees `CASE WHEN ... END` but not the CASE inside `ABS(CASE WHEN ... END)`
    * or `(CASE WHEN ... END) + 1`. MEASURED (issue #384): the wrapped form was accepted and stored
    * `c = 0.0` for every document, while the bare form was correctly refused -- found by review,
    * one function away from the shape the check did see.
    *
    * 🔴 ONE walk for every post-resolution rule that asks "where is a node of kind K in this
    * expression?" -- [[Case.conditionsOf]] (issue #384) and [[NullIf.mismatchesOf]] (issue #382).
    * Two walks would be two answers to the same question, and the second one would inherit none of
    * the corrections the first one was given.
    */
  private[elastic] def functionsOf(chain: FunctionChain): Seq[Function] = {
    def from(c: FunctionChain, depth: Int): Seq[Function] =
      if (depth > MaxNesting) Nil
      else
        FunctionUtils.transformFunctions(c).flatMap { fn =>
          (fn +: (fn match {
            case cs: Case =>
              cs.conditions.collect { case (_, result: FunctionChain) => result }.flatMap {
                from(_, depth + 1)
              } ++
                cs.expression.toSeq
                  .collect { case f: FunctionChain => f }
                  .flatMap(from(_, depth + 1))
            case _ => Nil
          })) ++ (fn match {
            case n: FunctionN[_, _] =>
              n.args.collect { case f: FunctionChain => f }.flatMap(from(_, depth + 1))
            case _ => Nil
          })
        }

    from(chain, 0)
  }

  /** A depth stop for [[functionsOf]]. A CASE inside a CASE inside a function is already beyond
    * anything measured; the bound exists so a cyclic or pathological AST cannot hang validation,
    * not because the depth is meaningful.
    */
  private val MaxNesting = 12

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

    /** `COALESCE` returns whichever arm is first non-null, so the type it REPORTS is the common
      * super type of the arms — the same derivation as [[baseType]], over the arms' own reported
      * types. This is where widening legitimately happens, and DuckDB 0.10.1 agrees:
      * `COALESCE(NULLIF(<DATE col>, <TIMESTAMP>), CURRENT_DATE)` is `DATE`, while the same over a
      * `TIMESTAMP` column is `TIMESTAMP`.
      */
    override def reportedType: SQLType = SQLTypeUtils.leastCommonSuperType(reportedArgTypes)

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

  /** A rendering that may be spliced into [[NullIf]] 's templates exactly as it stands: a Painless
    * NAME, or a LITERAL (a number, a quoted string, `true` / `false` / `null` -- all of which the
    * name pattern already covers). Everything else is COMPOUND and must be bound, so the pattern is
    * deliberately narrow: answering "simple" about a compound rendering is the defect, while
    * answering "compound" about a name costs one redundant local. Compiled ONCE, here, rather than
    * per AST node.
    */
  private val simpleOperand =
    """(?:[A-Za-z_][A-Za-z0-9_]*|-?\d+(?:\.\d+)?[lLfFdD]?|"(?:[^"\\]|\\.)*")""".r

  /** A whole-number Painless literal (`GREATEST` / `LEAST` compare two of them when rendering). */
  private val WholeLiteral = """-?\d+[lL]?""".r

  /** How a simple `CASE <expr> WHEN <v>` compares (`Case.whenComparison`). */
  private[cond] sealed trait WhenComparison
  private[cond] case object WhenAsResult extends WhenComparison
  private[cond] case object WhenInstants extends WhenComparison
  private[cond] case object WhenNumbers extends WhenComparison

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
      * ⚠️ This derivation answers which ARM a comparable pair takes. It says nothing about a pair
      * that is not comparable at all (`NULLIF(s, 0)`, where the operand really is a KEYWORD): no
      * arm of [[toPainlessCall]] is right for those, and they are REFUSED by [[typeMismatchError]],
      * which reads the very same derivation.
      *
      * 🔴 It asks `renderedType`, NOT `chainType`, and the difference is a MEASURED regression this
      * function shipped with in #382. `chainType` is `functions.head.out` -- it reports what the
      * OUTERMOST function DECLARES, which is right for a producer (`Cast`) and wrong for an
      * adjuster, because every adjuster in the temporal family returns its RECEIVER's type (#368)
      * and so declares the un-narrowed `TEMPORAL`. Measured on `975aa87b` vs `245f45f2`:
      * {{{
      * DATE_PARSE('2025-09-11', '%Y-%m-%d') - INTERVAL 2 DAY   out=DATE  chainType=TEMPORAL
      * COALESCE(NULLIF(createdAt, <that>), CURRENT_DATE)       DATE  ->  TIMESTAMP
      * }}}
      * so a `DATE` computed column started DECLARING itself a `TIMESTAMP` -- which is what `SHOW
      * MATERIALIZED VIEW` prints and what JDBC/BI metadata reports for the column. Caught
      * downstream by softclient4es-extensions' `ExtensionsIntegrationSpec`, not here: no core suite
      * nested a `NULLIF` over an interval-adjusted operand inside a `COALESCE`.
      *
      * [[app.softnetwork.elastic.sql.Identifier.renderedType]] is issue #384's answer to exactly
      * this question -- it returns `chainType` UNCHANGED for everything non-temporal (so the cast
      * repair above is untouched) and, for a temporal chain, scans outermost-inward for the first
      * function that PRODUCES its output type rather than preserving its receiver's. Here that
      * finds `DateParse`, and the operand reports `DATE` again.
      */
    private def argTypeOf(arg: PainlessScript): SQLType = arg match {
      case i: Identifier => i.renderedType
      case other         => other.out
    }

    /** What a string LITERAL argument is read as beside the other argument -- the ONE comparison
      * rule (`ComparisonRule.readingOf`, the lead's ruling of 2026-10-05): `NULLIF(n, '3')`
      * compares `n` with the number 3, `NULLIF(COALESCE(d, d2), '2024-01-31')` with a date. `None`
      * for anything else, and for a literal that spells no value of the other's kind.
      *
      * Computed ONCE per `NULLIF` and memoised (its type, its rendering and the comparison it sits
      * in all ask it; `ComparisonRule` says why with no lock); attaching the schema copies the
      * `NULLIF` (`update`), so a resolved statement's reading is read against the resolved types.
      */
    private[cond] def secondReading: Option[ComparisonRule.Reading] = {
      var reading = _secondReading
      if (reading eq null) {
        reading = ComparisonRule.readingOf(expr2, ComparisonRule.declaredTypeOf(expr1))
        _secondReading = reading
      }
      reading
    }

    private[cond] def firstReading: Option[ComparisonRule.Reading] = {
      var reading = _firstReading
      if (reading eq null) {
        reading = ComparisonRule.readingOf(expr1, ComparisonRule.declaredTypeOf(expr2))
        _firstReading = reading
      }
      reading
    }

    private[this] var _secondReading: Option[ComparisonRule.Reading] = null
    private[this] var _firstReading: Option[ComparisonRule.Reading] = null

    /** A string literal argument READ as a date or a timestamp is rendered from that reading
      * (`ComparisonRule.temporalPainless`), never through the per-document `String -> temporal`
      * coercion the generic rendering binds for a literal: that coercion parses the literal's OWN
      * text by the operand's type, so a date-time literal beside a DATE and a date-only one beside
      * a TIMESTAMP -- both of which the reading reads, as PostgreSQL does -- threw on every
      * document, and a computed column stored NULL (`ignore_failure`).
      */
    override protected def ownArgumentPainless(
      index: Int,
      context: Option[PainlessContext]
    ): Option[String] =
      (index match {
        case 0 => firstReading
        case 1 => secondReading
        case _ => None
      }).collect { case t: ComparisonRule.TemporalReading => ComparisonRule.temporalPainless(t) }

    /** A literal argument counts as what it is read as: `NULLIF(n, '3')` compares two numbers. */
    override def argTypes: List[SQLType] =
      List(
        firstReading.map(_.sqlType).getOrElse(argTypeOf(expr1)),
        secondReading.map(_.sqlType).getOrElse(argTypeOf(expr2))
      )

    /** 🔴 `NULLIF` REPORTS THE TYPE OF ITS FIRST ARGUMENT, not the super type of both.
      *
      * `NULLIF(a, b)` is `CASE WHEN a = b THEN NULL ELSE a END`: the value is always `a` or NULL,
      * and `b` is only ever COMPARED. This emitter says so itself — [[toPainlessCall]] renders
      * `$arg0 == null || ($arg1 != null && <comparison>) ? null : $arg0`, in which `arg1` appears
      * in no result position. So widening the reported type to include `b` describes a value this
      * function cannot produce.
      *
      * Corroborated on DuckDB 0.10.1, over a table with `d DATE` and `createdAt TIMESTAMP`:
      * {{{
      * typeof(NULLIF(d,         strptime('2025-09-11','%Y-%m-%d') - INTERVAL 2 DAY))  -> DATE
      * typeof(NULLIF(createdAt, strptime('2025-09-11','%Y-%m-%d') - INTERVAL 2 DAY))  -> TIMESTAMP
      * }}}
      * i.e. the answer follows the FIRST argument in both, never the pair.
      *
      * ⚠️ Deliberately NOT applied to [[baseType]]. `out` is what [[toPainlessCall]] switches on to
      * pick a COMPARISON, and comparing is the one thing that really does involve both operands --
      * `NULLIF(CAST(s AS BIGINT), 0)` must take the numeric arm because BOTH sides render numbers.
      * Two questions, two derivations: `out` says how to compare, `reportedType` says what comes
      * out. A pair that cannot be compared at all is refused by [[typeMismatchError]] before either
      * matters.
      *
      * A first argument that is a string literal READ beside the second (`firstReading`) is the
      * value it is read as, and is rendered as one: `NULLIF('2024-01-31', ts)` answers that
      * midnight, a TIMESTAMP, as PostgreSQL types it -- not the VARCHAR the literal is written as,
      * which a `CREATE TABLE … AS SELECT` declared, storing text.
      */
    override def reportedType: SQLType =
      firstReading.map(_.sqlType).getOrElse(reportedArgTypes.headOption.getOrElse(baseType))

    /** The rejection this `NULLIF` earns when its two arguments RENDER types SQL does not compare
      * (issue #382, the LEAD RULING of 2026-09-23).
      *
      * 🔴 A type mismatch is not an emission problem; it is a type ERROR, and the engine says so
      * instead of building a script around it. Every arm of [[toPainlessCall]] is wrong for a
      * TEXT-against-NUMBER pair and each is wrong in its own way, all MEASURED on ES 8.18.3:
      *   - the string arm's `compareTo` throws PER DOCUMENT (`wrong_method_type_exception: cannot
      *     convert MethodHandle(String,String)int to (Object,int)Object`), and in a computed column
      *     `ScriptProcessor.ignoreFailure` defaults to `true`, so the throw is SWALLOWED and the
      *     column silently DISAPPEARS -- HTTP 200 over a document missing the value it asked for,
      *     the #205 family;
      *   - its `$arg1 != null` guard over a primitive numeric literal does not even compile
      *     (`class_cast_exception: Cannot cast from [int] to [java.lang.Object]`), which is how
      *     `NULLIF(CAST(n AS KEYWORD), 0)` used to fail;
      *   - the numeric arm's `==` compiles for any pair and answers `false` for a `Long` against a
      *     `String`, so the `NULLIF` would never null anything -- silently.
      *
      * Emitting nothing at all was considered and rejected for the same reason: an absent column is
      * the silent failure, not an escape from it.
      *
      * '''The rule is deliberately NARROW''' (lead, 2026-09-23) -- exactly TEXT against NUMBER,
      * plus the temporal-literal edge below. `SQLTypeUtils.matches` was implemented first and
      * MEASURED to refuse 540 further shapes in the same corpus, among them TEMPORAL-against-NUMBER
      * (`NULLIF(d, 0)`) and every BOOLEAN pair (`NULLIF(b, 0)`, `NULLIF(b, 'x')`). Those emit today
      * exactly what they emitted before and are RECORDED as a known boundary, not refused: no
      * emission for them was measured to be wrong, and a validator that refuses what it has not
      * measured is the same mistake in the other direction.
      *
      * The comparison is between what each argument RENDERS ([[argTypeOf]]), never between the
      * COLUMNS' declared types: `NULLIF(CAST(d AS DATE), CAST('2025-01-01' AS DATE))` reports
      * `List(TIMESTAMP, DATE)` on `out` and `List(DATE, DATE)` on the chain, and it WORKS -- asking
      * `out` would refuse a shape this engine executes correctly today. And a cast can CREATE the
      * mismatch as well as cure it: `NULLIF(CAST(n AS KEYWORD), 0)` renders TEXT against NUMBER
      * over a numeric column, and IS refused.
      *
      * 🔴 A `NULL` operand is never refused, and STRUCTURALLY so rather than by a guard: SQL says
      * `NULLIF(a, NULL)` is UNKNOWN, so the answer is `a`, and `SQLTypes.Null` is neither an
      * `SQLVarchar` nor an `SQLNumeric`, so no widening of either predicate can start refusing it.
      * An explicit short-circuit was written first and MEASURED to change no verdict in the whole
      * corpus -- dead by construction, so it is the invariant that is recorded, not the code.
      * `NULLIF(s, NULL)` and `NULLIF(NULL, 0)` pin it.
      */
    /** Can [[toPainlessCall]] express a comparison between these two renderings at all?
      *
      * 🔴 DERIVED FROM THE ARMS, not written alongside them. The arms are: both TEXT (`compareTo`),
      * both TEMPORAL (`isEqual` / `compareTo`), and a final `==` that Painless applies to any pair
      * without complaining. An earlier spelling listed the ONE pair it had measured -- TEXT against
      * NUMBER -- and everything else fell through to `==`. That is how a TEMPORAL or BOOLEAN
      * operand against a NUMBER or a text LITERAL reached `==`: Painless answers `false` for
      * `ZonedDateTime == String` and for `Boolean == Long`, never throwing, so `NULLIF` returned
      * its first argument for EVERY row, HTTP 200, in every venue -- the silent always-false this
      * very rule refuses TEXT-against-NUMBER to avoid. A predicate written beside the arms says
      * nothing when a new arm appears; one derived FROM them cannot drift.
      *
      * ⚠️ UNKNOWN accepts. An unresolved column reports `Any` (no schema attached: a wildcard or
      * multi-index FROM, a mapping that failed to load), and refusing there would reject legitimate
      * SQL on every schema-less path while the resolved spelling of the same statement is fine. A
      * NULL operand accepts for the same reason, and SQL agrees: `NULLIF(a, NULL)` is `a`.
      */
    private def comparable(left: SQLType, right: SQLType): Boolean =
      left.isUnknown || right.isUnknown ||
      (left.isText && right.isText) ||
      (left.isTemporal && right.isTemporal) ||
      (left.isNumber && right.isNumber) ||
      (left.isBoolean && right.isBoolean)

    /** A TEMPORAL operand against a text one. Refused -- but by [[temporalLiteralError]] FIRST,
      * because it can say something far more useful than the type names.
      *
      * 🔴 Both halves are refusals today, and the reason is the EMITTER, not the types.
      * [[toPainlessCall]] has no way to compare a `ZonedDateTime` with a `String`: the pair folds
      * to VARCHAR, misses the text arm (one side is not text), misses the temporal arm (`out` is
      * not temporal) and lands on `==`, which Painless evaluates to `false` for every document
      * without ever throwing. `NULLIF(created_at, '2025-01-01')` therefore returned `created_at`
      * for every row, HTTP 200, in every venue -- measured, and the reason this rule exists.
      *
      * Making it WORK needs a `String -> temporal` coercion `SQLTypeUtils.coerce` does not have
      * (its temporal arms are all temporal-to-temporal), one per subtype, in three venues, on four
      * Elasticsearch majors -- and Elasticsearch DATE MATH (`now-1d`) has no Painless equivalent at
      * all, so that spelling can never be coerced and would have to be refused by name. That is a
      * story, not a guard, and it is scoped as one. Until it lands the answer is LOUD.
      */
    private def temporalAgainstText(left: SQLType, right: SQLType): Boolean =
      (left.isTemporal && right.isText) || (left.isText && right.isTemporal)

    def typeMismatchError: Option[String] = {
      val left = argTypeOf(expr1)
      val right = argTypeOf(expr2)
      // A literal the other argument READS (`secondReading`) is a value of that argument's kind:
      // comparable by construction, and rendered as that value. Asked first, by the query seam
      // and the DDL one alike, so a computed column and a query read `NULLIF(n, '3')` the same.
      if (secondReading.isDefined || firstReading.isDefined) None
      else if (temporalAgainstText(left, right))
        // The resolver goes first so a MALFORMED literal is named as such (`'x'` is not a date for
        // this field's format) rather than reported as a bare type mismatch; a WELL-FORMED one
        // falls through to a message that says what is actually wrong -- the engine, not the SQL.
        temporalLiteralError(expr1, left, expr2, right)
          .orElse(temporalLiteralError(expr2, right, expr1, left))
          .orElse(
            Some(
              s"$sql compares ${left.typeId} with ${right.typeId}: comparing a temporal value " +
              "with a string is not supported yet, so cast the string to the column's type"
            )
          )
      else if (!comparable(left, right))
        Some(
          s"$sql compares ${left.typeId} with ${right.typeId}: NULLIF requires two arguments of " +
          "comparable types, so cast one of them"
        )
      else None
    }

    /** The edge the type rule CANNOT decide: a string literal against a date-mapped column.
      *
      * 🔴 MEASURED: `NULLIF(d, '2025-01-01')` and `NULLIF(d, 'x')` present IDENTICALLY as
      * `TIMESTAMP` against `VARCHAR`, so no rule written over types alone can accept the first and
      * refuse the second -- and refusing both would reject legitimate SQL. The lead's ruling is to
      * ask the SAME machinery a `WHERE` clause already asks (issue #276's
      * [[TemporalLiterals.normalizeLiteral]]), against the column's own mapping `format`, so there
      * is ONE date-literal grammar in the engine and not two.
      *
      * It is reachable from both seams because it is a pure function of (literal, field, format)
      * and the resolved identifier carries its [[Column]]. Only the VERDICT is used: `NULLIF` does
      * not rewrite its operand the way `WHERE` does, so a space-form literal is accepted and
      * forwarded exactly as before -- recorded on #382, not changed here.
      *
      * `TIME` is excluded by [[TemporalLiterals.isTemporalColumn]] (Elasticsearch has no time-only
      * mapping), and a function-wrapped operand on EITHER side is excluded: a cast or a function
      * makes the comparison one between what the CHAINS render, not one against the mapped field.
      */
    private def temporalLiteralError(
      column: PainlessScript,
      columnType: SQLType,
      literal: PainlessScript,
      literalType: SQLType
    ): Option[String] =
      if (!columnType.isTemporal || !literalType.isText) None
      else
        for {
          col   <- mappedColumn(column).filter(c => TemporalLiterals.isTemporalColumn(c.dataType))
          value <- stringLiteral(literal)
          field <- Some(column).collect { case id: Identifier => id.name }
          reason <- TemporalLiterals
            .normalizeLiteral(value, field, TemporalLiterals.FieldFormat.of(col))
            .left
            .toOption
        } yield reason

    /** The mapped column a BARE operand names, or `None` for anything else.
      *
      * 🔴 The parser wraps EVERY operand in an `Identifier`, literals included
      * (`Parser.functionAsIdentifier`), so "is this a column?" is not `isInstanceOf[Identifier]` --
      * it is "does it carry a name and no functions, and did the schema attach a [[Column]] to
      * it?". MEASURED: a first version asked the type only, found `expr2` to be a
      * `GenericIdentifier(name = "", functions = List(StringValue))`, and the whole temporal edge
      * silently never fired.
      */
    private def mappedColumn(operand: PainlessScript): Option[Column] = operand match {
      case id: GenericIdentifier if id.name.nonEmpty && id.functions.isEmpty => id.col
      case _                                                                 => None
    }

    /** The text of a BARE string literal operand -- `'2025-01-01'`, not `CAST('x' AS DATE)`. */
    private def stringLiteral(operand: PainlessScript): Option[String] = operand match {
      case id: GenericIdentifier if id.name.isEmpty =>
        id.functions match {
          case List(value: StringValue) => Some(value.value)
          case _                        => None
        }
      case value: StringValue => Some(value.value)
      case _                  => None
    }

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
      * as it was -- pre-existing, and not this repair's to change.
      *
      * 🔴 The `Some(ctx)` branch binds UNCONDITIONALLY, and that is NOT the shape
      * [[app.softnetwork.elastic.sql.operator.math.ArithmeticExpression]] uses: its `operandOf`
      * asks `PainlessOperandForm.placeable` BEFORE binding and falls back to the operand's
      * registered parameter when the rendering is a statement. It can be asked to bind a `try` /
      * `catch`; this site cannot, and the INVARIANT is why -- '''with a context, a safe cast has
      * already hoisted itself''', so what arrives in `callArgs` is the parameter NAME (`TRY_CAST(s
      * AS BIGINT)` renders `def safe1 = null; try {...} catch {...} def param2 = safe1;` into the
      * prologue and hands this method `param2`). A `placeable` guard here would therefore be an arm
      * no input can reach. The invariant is not asserted by reading the code: `NullIfOperandSpec`
      * checks it over the whole operand corpus, in every venue.
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
          // A string literal argument is the value the other argument READS it as -- the number
          // or the boolean it spells, rendered as one (`secondReading` / `firstReading`).
          def scalar(reading: Option[ComparisonRule.Reading], rendered: String): String =
            reading.flatMap(ComparisonRule.scalarPainless).getOrElse(rendered)
          val arg0 = operand(scalar(firstReading, rendered0), context)
          // A second argument READ as a temporal is compared as the instant it denotes
          // (`comparedMillis`) and is never the answer, so its value is not bound: it would be
          // evaluated on every document for nothing.
          val arg1 =
            if (secondReading.exists(_.isInstanceOf[ComparisonRule.TemporalReading])) rendered1
            else operand(scalar(secondReading, rendered1), context)
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
          // Two temporals that do not render the same java.time type -- a DATE column's
          // `ZonedDateTime` against a cast's `LocalDate` -- or a string literal READ as a temporal
          // are compared as the instants they denote, a DATE as the instant its UTC day starts at
          // (`ArithmeticExpression.comparedMillis`). `isEqual` failed between the first two
          // (`NULLIF(COALESCE(d, d2), CAST('2024-01-31' AS DATE))`, all shards failed on 8.18.3);
          // a literal is NEVER NULL, and `<primitive> != null` does not compile, so it is not
          // guarded.
          val temporalLiteral = Seq(firstReading, secondReading).exists {
            case Some(_: ComparisonRule.TemporalReading) => true
            case _                                       => false
          }
          def dateCarrying(t: SQLType): Boolean =
            t == SQLTypes.Date || t == SQLTypes.DateTime || t == SQLTypes.Timestamp
          // (An ingest processor reads both arguments by their runtime class already -- the
          // `processorInstant` arm below -- and its source is persisted: it keeps its bytes.)
          val mixedTemporals =
            !context.exists(_.isProcessor) && !receiverIsTime &&
            dateCarrying(ComparisonRule.declaredTypeOf(expr1)) &&
            dateCarrying(ComparisonRule.declaredTypeOf(expr2)) && {
              val (java1, java2) = (argTypeOf(expr1), argTypeOf(expr2))
              !java1.isUnknown && !java2.isUnknown && java1 != java2
            }
          lazy val instants =
            s"${ArithmeticExpression.comparedMillis(expr1, firstReading, arg0, context)} == " +
            s"${ArithmeticExpression.comparedMillis(expr2, secondReading, arg1, context)}"
          val expr =
            if (secondReading.exists(_.isInstanceOf[ComparisonRule.TemporalReading]))
              s"$arg0 == null || ($instants) ? null : $arg0"
            else if (temporalLiteral)
              s"$arg0 == null || ($arg1 != null && $instants) ? null : $arg0"
            else if (mixedTemporals) nullIf(instants)
            else
              out match {
                // 🔴 `String.compareTo` takes a `String` and NOTHING else, so this arm is right only
                // when BOTH operands render text -- and `leastCommonSuperType` answers VARCHAR for a
                // MIXED pair too (`[BOOLEAN, KEYWORD]`, `[TIMESTAMP, KEYWORD]`). Without the guard
                // `NULLIF(b, CAST(n AS KEYWORD))` and `NULLIF(d, CAST(i AS VARCHAR))` emitted
                // `param1.compareTo(param2)` over a `Boolean` / `ZonedDateTime` receiver and threw
                // PER DOCUMENT on ES 8.18.3 -- which in a computed column `ignoreFailure` SWALLOWS,
                // so the column silently disappears. MEASURED: 32 corpus rows, all of them shapes
                // that answered a value before the type fix of this same issue moved the supertype
                // from the column's type to the chain's. They fall to the `==` arm below, which is
                // what they emitted before and which Painless applies to any pair without throwing.
                //
                // The TEXT-against-NUMBER half of the same population is refused instead, by
                // [[typeMismatchError]] -- `==` there would be a silent always-false.
                case SQLTypes.Varchar if argTypes.forall(_.isText) =>
                  nullIf(s"$arg0.compareTo($arg1) == 0")
                // 🔴 Keyed on what the RECEIVER's chain RENDERS, not on `out` (issue #373).
                // `LocalTime` has no `isEqual`, and MEASURED on ES 8.18 before this,
                // `NULLIF(CAST(d AS TIME), CAST('00:00:00' AS TIME))` failed the shard in a real
                // `script_fields`. Neither `out` nor `expr1.out` says so: both are TIMESTAMP, the
                // COLUMN's type -- `chainType` is the derivation that describes the rendering
                // (#367), and it answers TIME. Two wrong keys were tried before measuring it.
                case _: SQLTemporal if receiverIsTime =>
                  nullIf(s"$arg0.compareTo($arg1) == 0")
                // 🔴 In an INGEST processor a column argument is the incoming document's raw JSON --
                // a string or epoch milliseconds -- so `isEqual` failed on every document (`String`
                // has none) and `ignore_failure` stored NULL: `NULLIF(d, d2) - d2` was NULL on every
                // row (found by review, measured on 8.18.3). Both are compared as the instants they
                // denote, read by their runtime class (`SQLTypeUtils.processorInstant`); NULLIF still
                // answers its FIRST argument as it came.
                case _: SQLTemporal if context.exists(_.isProcessor) =>
                  nullIf(
                    s"${SQLTypeUtils.processorInstant(arg0)}" +
                    s".isEqual(${SQLTypeUtils.processorInstant(arg1)})"
                  )
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
      } yield ()
    }

    /** The TYPE rules of `NULLIF`, asked once the column types are known (the lead's ruling of
      * 2026-10-05): [[typeMismatchError]] (issue #382), then the ONE comparison rule
      * (`ComparisonRule`) -- `NULLIF(a, b)` compares `a = b`, so it is judged as that comparison:
      * on the types the two arguments are DECLARED with, a DATE comparable with a TIMESTAMP.
      * `NULLIF(COALESCE(d, d2), CAST('2024-01-31' AS DATE))` compares two dates, and is accepted
      * like `NULLIF(d, CAST('2025-01-01' AS DATE))` always was (the lead's ruling of 2026-09-23).
      */
    override def typeError: Option[String] =
      typeMismatchError.orElse {
        // judged on the readings this NULLIF already holds (`firstReading`, `secondReading`)
        ComparisonRule
          .mismatch(
            ComparisonRule.comparedType(firstReading, expr1),
            ComparisonRule.comparedType(secondReading, expr2)
          )
          .map { case (left, right) =>
            s"Type mismatch: output '${left.typeId}' is not compatible with input '${right.typeId}'"
          }
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

    /** The type of the CASE's VALUE: the least common supertype of its `THEN` results and its
      * `ELSE` -- never the `CASE <expr>` operand, which is only compared ([[comparisonType]]), as
      * [[reportedType]] already reads it. With the operand in, `CASE s WHEN 'a' THEN 1 ELSE 0 END`
      * over a KEYWORD `s` answered the strings `"1"` / `"0"` and `CASE x WHEN 1.5 THEN 1 ELSE 0
      * END` over a DOUBLE `x` the doubles `1.0` / `0.0` while reporting BIGINT; PostgreSQL answers
      * the integers 1 and 0.
      */
    override def baseType: SQLType =
      SQLTypeUtils.leastCommonSuperType(
        conditions.map { case (_, res) => res.out } ++ default.map(_.out)
      )

    /** The type a simple CASE compares its operand and each `WHEN` value in: the common supertype
      * of the operand and the results -- what [[baseType]] answered while it included the operand
      * -- or the type a cast over the CASE sets, so every comparison renders exactly as it did.
      * Only the VALUE's type moved ([[baseType]]).
      */
    private[this] def comparisonType: SQLType =
      if (out != baseType) out else SQLTypeUtils.leastCommonSuperType(argTypes)

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
      else Right(())
    }

    /** The TYPE rules of the conditions, asked once the column types are known (the lead's ruling
      * of 2026-10-05) and on what each operand RENDERS (`renderedTypeOf`): asked when the statement
      * was parsed, `CASE WHEN flag THEN …` was refused because a BOOLEAN column is `Any` there. A
      * type that is still unknown is accepted: Elasticsearch answers.
      */
    override def typeError: Option[String] =
      expression match {
        case None if conditions.exists { case (cond, _) =>
              val t = renderedTypeOf(cond)
              t != SQLTypes.Boolean && !t.isUnknown
            } =>
          Some("CASE WHEN conditions must be of type BOOLEAN")
        // Each value is compared with the expression by the ONE comparison rule (`ComparisonRule`):
        // on their DECLARED types, a DATE comparable with a TIMESTAMP, a string literal read by
        // what it is compared with -- `CASE n WHEN '3'`, `CASE d WHEN CAST('2024-01-31' AS DATE)`
        // -- judged on the readings this CASE already holds (`whenReadings`).
        case Some(e) if {
              val (declared, readings) = whenReadings(e)
              val literalExpression = ComparisonRule.stringLiteral(e).isDefined
              conditions.zip(readings).exists { case ((cond, _), reading) =>
                val left =
                  if (!literalExpression) declared
                  else
                    ComparisonRule
                      .readingOf(e, ComparisonRule.declaredTypeOf(cond))
                      .map(_.sqlType)
                      .getOrElse(declared)
                ComparisonRule
                  .mismatch(left, ComparisonRule.comparedType(reading, cond))
                  .isDefined
              }
            } =>
          Some("CASE WHEN conditions must be of the same type as the expression")
        case _ => None
      }

    /** The least common supertype of the BRANCHES' reported types -- the THEN results and the ELSE,
      * never the `CASE <expr>` operand -- as `COALESCE` reports its arms': `CASE … THEN d ELSE d2
      * END` is a DATE when both are.
      */
    override def reportedType: SQLType =
      SQLTypeUtils.leastCommonSuperType(
        conditions.map { case (_, result) => result.reportedType } ++ default.map(_.reportedType)
      )

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
      candidateNullable: Boolean,
      compared: SQLType
    ): String = {
      val guard = if (candidateNullable) s"$e != null && $c != null" else s"$e != null"
      // 🔴 A TIME is decided by what the RECEIVER RENDERS, not by `out` (issue #373, found by
      // review). `out` -- then the least common supertype of the expression and the results, now
      // [[comparisonType]] -- reports TIMESTAMP for
      // `CASE CAST(d AS TIME) WHEN CAST(ts AS TIME) …` whose operands are both `LocalTime`, so an
      // arm keyed on it was DEAD and that shape still emitted `isEqual`. MEASURED on ES 8.18:
      // `dynamic method [java.time.LocalTime, isEqual/1] not found`, the very failure this PR's
      // first commit exists to remove. `Identifier.chainType` (#367) is the derivation that
      // describes the rendering -- the same key `NULLIF` needed.
      if (receiverIsTime) s"$guard && $e.compareTo($c) == 0 ? $v"
      else
        compared match {
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

    /** How `CASE <expr> WHEN <v> …` compares its expression with each value -- the ONE comparison
      * rule (`ComparisonRule`) decides what is compared; this decides how it renders.
      *
      * The historical rendering converts BOTH sides to [[comparisonType]] -- the common type of the
      * expression and the results, what `out` was until the CASE's value was typed by its results
      * alone -- and is kept wherever that compares what SQL compares ([[WhenAsResult]]). It does
      * not when:
      *   - the expression is a DATE or a TIMESTAMP and a value is a string literal read as one, the
      *     two render different java.time types, or the result is not a temporal: a `LocalDate`
      *     value against a column's `ZonedDateTime` failed the shard, and a numeric result compared
      *     `toEpochMilli()` with a `LocalDate` (`CASE d WHEN CAST('2024-01-31' AS DATE) THEN 1 ELSE
      *     0 END` answered 0 for 2024-01-31) -- they are compared as the instants they denote
      *     ([[WhenInstants]]);
      *   - the expression is a number and the result is not: two numbers were compared as their
      *     strings (`"1.0"` against `"1"`) -- they are compared as numbers ([[WhenNumbers]]). An
      *     ingest processor keeps its (persisted) bytes unless a value is a string literal it now
      *     reads.
      */
    private[this] def whenComparison(
      expr: PainlessScript,
      context: Option[PainlessContext]
    ): WhenComparison = {
      def dateCarrying(t: SQLType): Boolean =
        t == SQLTypes.Date || t == SQLTypes.DateTime || t == SQLTypes.Timestamp
      val (declared, readings) = whenReadings(expr)
      val processor = context.exists(_.isProcessor)
      // the type the two sides are converted to ([[WhenAsResult]]): the old `out`
      val compared = comparisonType
      if (receiverIsTime) WhenAsResult
      else if (dateCarrying(declared)) {
        val temporalReading =
          readings.exists(_.exists(_.isInstanceOf[ComparisonRule.TemporalReading]))
        // a KNOWN mismatch only: with no schema attached a type is unknown, and the statement
        // keeps the rendering it always had
        val mismatch =
          (!compared.isInstanceOf[SQLTemporal] && !compared.isUnknown) ||
          conditions.exists { case (cond, _) =>
            val rendered = javaTypeOf(cond)
            !rendered.isUnknown && rendered != javaTypeOf(expr)
          }
        if (temporalReading || (!processor && mismatch)) WhenInstants else WhenAsResult
      } else if (declared.isNumber && !compared.isNumber && !compared.isUnknown) {
        if (!processor || readings.exists(_.isDefined)) WhenNumbers else WhenAsResult
      } else WhenAsResult
    }

    /** The CASE expression's DECLARED type and what each `WHEN` value is read as beside it
      * (`ComparisonRule.readingOf`), computed ONCE per CASE and memoised (`ComparisonRule` says why
      * with no lock): its type rule, the comparison it is rendered as and every `WHEN` arm ask it,
      * and a reading is a parse. Attaching the schema copies the CASE (`update`), so a resolved
      * statement's readings are read against the resolved types.
      */
    private[this] def expressionReadings: (SQLType, List[Option[ComparisonRule.Reading]]) = {
      var readings = _expressionReadings
      if (readings eq null) {
        readings = expression match {
          case Some(e) => readingsBeside(e)
          case None    => (SQLTypes.Any, conditions.map(_ => None))
        }
        _expressionReadings = readings
      }
      readings
    }

    private[this] var _expressionReadings: (SQLType, List[Option[ComparisonRule.Reading]]) = null

    private[this] def readingsBeside(
      expr: PainlessScript
    ): (SQLType, List[Option[ComparisonRule.Reading]]) = {
      val declared = ComparisonRule.declaredTypeOf(expr)
      (declared, conditions.map { case (cond, _) => ComparisonRule.readingOf(cond, declared) })
    }

    /** [[expressionReadings]] when `expr` IS the CASE expression -- the only one the renderings
      * pass -- and computed for `expr` otherwise.
      */
    private[this] def whenReadings(
      expr: PainlessScript
    ): (SQLType, List[Option[ComparisonRule.Reading]]) =
      if (expression.exists(_ eq expr)) expressionReadings else readingsBeside(expr)

    /** The boolean CONSTANT a `WHEN` value is, if it is one: a string literal read as a boolean
      * (`CASE b WHEN '1'`, `ComparisonRule.BooleanReading`), or the literal `TRUE` / `FALSE`.
      */
    private[this] def booleanConstantOf(
      value: PainlessScript,
      reading: Option[ComparisonRule.Reading]
    ): Option[Boolean] =
      reading match {
        case Some(ComparisonRule.BooleanReading(b)) => Some(b)
        case Some(_)                                => None
        case None =>
          value match {
            case b: BooleanValue => Some(b.value)
            case i: Identifier if i.name.trim.isEmpty =>
              i.functions match {
                case (b: BooleanValue) :: Nil => Some(b.value)
                case _                        => None
              }
            case _ => None
          }
      }

    /** The JAVA type an operand renders (#384's `Identifier.renderedType`): a DATE column's doc
      * value is a `ZonedDateTime`, a cast to DATE a `LocalDate`.
      */
    private[this] def javaTypeOf(operand: PainlessScript): SQLType = operand match {
      case i: Identifier => i.renderedType
      case other         => other.out
    }

    /** A `THEN` result, converted to the CASE's type -- the one rendering both comparisons share.
      */
    private[this] def whenResult(
      cond: PainlessScript,
      res: PainlessScript,
      context: Option[PainlessContext]
    ): String = {
      val name =
        cond match {
          case e: Expression             => e.identifier.name
          case f: FunctionWithIdentifier => f.identifier.name
          case i: Identifier             => i.name
          case _                         => ""
        }
      boxWhenNoDefault(res match {
        case i: Identifier if i.name == name && name.nonEmpty =>
          i.withNullable(false)
          SQLTypeUtils.coerce(i, out, context)
        case _ =>
          SQLTypeUtils.coerce(res, out, context)
      })
    }

    /** The `WHEN` arms of a [[WhenInstants]] or [[WhenNumbers]] comparison. The expression is
      * rendered ONCE (a rendering is not idempotent) and, for instants, converted once; every value
      * is guarded like the historical candidate (`checkCase`), and a NULL on either side matches
      * nothing -- SQL's `NULL = NULL` is unknown, where Painless's `null == null` is true.
      */
    private[this] def comparedWhens(
      expr: PainlessScript,
      kind: WhenComparison,
      ctx: PainlessContext,
      context: Option[PainlessContext]
    ): String = {
      val (_, readings) = whenReadings(expr)
      def bind(rendered: String): String = ctx.addParam(LiteralParam(rendered)).getOrElse(rendered)
      def instant(operand: PainlessScript, ref: String): String =
        bind(
          s"($ref != null ? (def)(${ArithmeticExpression.comparedMillis(operand, None, ref, context)}) : null)"
        )
      val e = bind(expr.painless(context))
      val key = if (kind == WhenInstants) instant(expr, e) else e
      conditions
        .zip(readings)
        .map { case ((cond, res), reading) =>
          val r = whenResult(cond, res, context)
          val test = reading match {
            case Some(t: ComparisonRule.TemporalReading) => s"$key == ${t.epochMillis}L"
            case Some(scalar) =>
              s"$key == ${ComparisonRule.scalarPainless(scalar).getOrElse(cond.painless(context))}"
            case None =>
              val c = bind(cond.painless(context))
              val value = if (kind == WhenInstants) instant(cond, c) else c
              s"$value != null && $key == $value"
          }
          s"$key != null && $test ? $r"
        }
        .mkString(" : ")
    }

    override def painless(context: Option[PainlessContext] = None): String = {
      context match {
        case Some(ctx) =>
          var cases =
            expression match {
              case Some(expr) if whenComparison(expr, context) != WhenAsResult =>
                comparedWhens(expr, whenComparison(expr, context), ctx, context)
              case Some(expr) => // case with expression to evaluate
                // the operand and each value are converted to the type they are COMPARED in; the
                // results, below, to the CASE's own (`out`)
                val compared = comparisonType
                val e = SQLTypeUtils.coerce(expr, compared, context)
                val expParam = ctx.addParam(
                  LiteralParam(e)
                )
                conditions
                  .zip(whenReadings(expr)._2)
                  .map { case ((cond, res), whenReading) =>
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
                    // A string literal is the value the expression READS it as -- the number or
                    // the boolean it spells (`ComparisonRule.readingOf`), brought to the type the
                    // two are compared in like any other value. `CASE b WHEN 'true'` used to parse
                    // `"true"` as a long (`Long.parseLong`) and fail the shard.
                    //
                    // A boolean CONSTANT -- such a reading, or `TRUE` / `FALSE` -- is that number
                    // itself (`1L` / `0L`): converted like a value, `(true ? 1L : 0L)` is a
                    // conditional over a constant, which Elasticsearch 6.8's Painless refuses to
                    // compile ("Extraneous conditional statement").
                    val c =
                      booleanConstantOf(cond, whenReading)
                        .flatMap(SQLTypeUtils.booleanConstant(_, compared))
                        .getOrElse(
                          whenReading
                            .flatMap(reading =>
                              ComparisonRule
                                .scalarPainless(reading)
                                .map(
                                  SQLTypeUtils
                                    .coerce(_, reading.sqlType, compared, nullable = false, context)
                                )
                            )
                            .getOrElse(SQLTypeUtils.coerce(cond, compared, context))
                        )
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
                          case Some(bound) =>
                            checkCase(e, bound, r, candidateNullable = true, compared)
                          case _ => checkCase(e, c, r, candidateNullable = false, compared)
                        }
                      case _ =>
                        checkCase(e, c, r, candidateNullable = false, compared)
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
                        // A VALUE condition -- a boolean column, a function of one -- is NULL
                        // where the document has no value, and a NULL condition is not true: the
                        // branch does not apply (SQL), where `param1 ? 1 : 0` threw on that
                        // document (a query failed; a computed column stored NULL). `==` is
                        // null-safe in Painless. A predicate already renders NULL as false.
                        case Some(c) if cond.isInstanceOf[Identifier] => s"$c == true ? $r"
                        case Some(c)                                  => s"$c ? $r"
                        case _                                        => s"$c ? $r"
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

  /** N-ary reducer shared by `GREATEST` / `LEAST`, over numbers or over dates and timestamps.
    *
    * Emits a right-folded nested ternary so a NULL argument is skipped and the whole expression
    * yields NULL only when every argument is NULL: pairwise(x, y) = (x == null ? y : (y == null ? x
    * : Math.{max|min}(x, y)))
    *
    * Over numbers the type is the common type of ALL the arguments ([[numericType]]), as PostgreSQL
    * types it, and every row answers it: per document the value is converted to it on every path
    * ([[typedResult]]). Over dates and timestamps (the lead's ruling of 2026-10-05: every SQL
    * engine compares them, and they failed in Elasticsearch before) the same fold runs over the
    * epoch milliseconds each argument denotes -- a DATE its UTC day's start, through the conversion
    * date arithmetic uses (`ArithmeticExpression.instantMillis`) -- so dates compare as instants
    * whatever each one renders as (a doc value, a `LocalDate`, the clock), and a NULL is skipped
    * exactly as it is for numbers. The result is the instant that wins, a UTC `ZonedDateTime`
    * (TIMESTAMP is what it renders); per group, where a date is the number its metric is, it is
    * that number. Its reported type is its arguments' common one: DATE for dates, TIMESTAMP when
    * they mix.
    */
  sealed trait NumericReducer
      extends TransformFunction[SQLAny, SQLNumeric]
      with FunctionWithIdentifier {
    def values: List[PainlessScript]
    def operator: ConditionalOp
    protected def mathFn: String // "Math.max" | "Math.min"
    protected def greatest: Boolean // GREATEST (the larger wins) or LEAST

    /** The comparison under which the left argument wins. */
    private def keepsLeft: String = if (greatest) ">=" else "<="

    override def fun: Option[ConditionalOp] = Some(operator)

    override def args: List[PainlessScript] = values

    override def outputType: SQLNumeric = SQLTypes.Numeric

    override def identifier: Identifier = Identifier()

    override def inputType: SQLAny = SQLTypes.Any

    /** Are the arguments dates and timestamps? Decided on what each one RENDERS (`renderedTypeOf`,
      * the types the type rule reads), once the column types are known: until then a column is
      * `Any`, and the reducer is the numeric one it always was.
      */
    lazy val overDates: Boolean = values.exists(v => NumericReducer.dateCarrying(renderedTypeOf(v)))

    /** The numeric type of `GREATEST` / `LEAST`: the common type of ALL its arguments, as
      * PostgreSQL resolves it (its CASE / UNION rule), which every row then has.
      *   - whole numbers only: the widest of them -- an INT beside a BIGINT is a BIGINT;
      *   - any other number -- a DOUBLE, a REAL, a NUMERIC of no known kind (`SQRT`, `ROUND`) --
      *     makes it fractional: a DOUBLE, or a REAL when every fractional argument is a REAL.
      *
      * Each argument is read by the type it is DECLARED with (`ComparisonRule.declaredTypeOf`: its
      * reported type, the metric for an aggregate), never by `out` ([[argTypes]]): a column
      * resolved against its schema answers its COLUMN's type there whatever function it wears, so
      * `CAST(n AS DOUBLE)` over an INT `n` read INT, the arguments looked whole, and the winner was
      * chosen as a whole number -- the literal `1` itself where `n` is below 1 or NULL. A bare NULL
      * has no type and is left out.
      *
      * `None` when an argument is not a number (dates take [[overDates]]), or when one's type is
      * still unknown (no schema attached) and no fractional argument decides the type: an unknown
      * column may hold fractions, so it is never read as a whole number. The reducer then renders
      * as it always did.
      */
    private[cond] lazy val numericType: Option[SQLNumeric] = {
      val declared = values.map(ComparisonRule.declaredTypeOf).filterNot(_ == SQLTypes.Null)
      val fractional = declared.filter(t => t.isNumber && !NumericReducer.wholeNumber(t))
      if (declared.isEmpty || declared.exists(t => !t.isNumber && !t.isUnknown)) None
      else if (declared.forall(NumericReducer.wholeNumber))
        SQLTypeUtils.leastCommonSuperType(declared) match {
          case whole: SQLNumeric => Some(whole)
          case _                 => None
        }
      else if (fractional.isEmpty) None
      else if (fractional.forall(_ == SQLTypes.Real) && !declared.exists(_.isUnknown))
        Some(SQLTypes.Real)
      else Some(SQLTypes.Double)
    }

    override def baseType: SQLType =
      if (overDates) SQLTypes.Timestamp
      else
        numericType.getOrElse(SQLTypeUtils.leastCommonSuperType(argTypes) match {
          case n: SQLNumeric => n
          case _             => outputType
        })

    override def sql: String = s"$operator(${values.map(_.sql).mkString(", ")})"

    /** An aggregate among the arguments makes this a per-group calculation, as it makes an
      * arithmetic over them one (`BinaryFunction.hasAggregation`): `GREATEST(MAX(d), MAX(d2))`
      * beside a GROUP BY is computed per group, where it used to be refused as a non-aggregated
      * field while `GREATEST(MAX(d), MAX(d2)) - MIN(d)` was accepted.
      */
    override def hasAggregation: Boolean = values.exists(_.hasAggregation)

    override def checkIfNullable: Boolean = false

    override def validate(): Either[String, Unit] =
      if (values.isEmpty) Left(s"$operator requires at least one argument") else Right(())

    /** Numbers, or dates and timestamps -- one kind, as every SQL engine requires -- asked once the
      * column types are known (the lead's ruling of 2026-10-05) and on what each argument RENDERS
      * (`renderedTypeOf`). An argument whose type is still unknown (SQLAny / NULL, which extends
      * SQLAny) is accepted: Elasticsearch answers. An argument of any other kind (a string, a
      * boolean, a TIME), or numbers mixed with dates, is refused by name.
      */
    override def typeError: Option[String] = {
      val known = values
        .map(v => (v: PainlessScript) -> renderedTypeOf(v))
        .filterNot(_._2.isInstanceOf[SQLAny])
      val numbers = known.filter(_._2.isInstanceOf[SQLNumeric])
      val dates = known.filter(a => NumericReducer.dateCarrying(a._2))
      val offenders =
        known.find(a =>
          !a._2.isInstanceOf[SQLNumeric] && !NumericReducer.dateCarrying(a._2)
        ) match {
          case Some(other)                                => Seq(other)
          case None if numbers.nonEmpty && dates.nonEmpty => Seq(dates.head, numbers.head)
          case None                                       => Nil
        }
      // each argument is NAMED by the type DESCRIBE shows (a DATE column RENDERS as a TIMESTAMP)
      def named(arg: PainlessScript): String = (arg match {
        case aggregate: Identifier if aggregate.isAggregation => renderedTypeOf(aggregate)
        case other                                            => other.reportedType
      }).typeId
      if (offenders.isEmpty) None
      else
        Some(
          s"$operator requires numeric arguments, or date and timestamp arguments, but got " +
          offenders.map { case (arg, _) => s"${named(arg)} for ${arg.sql}" }.mkString(" and ")
        )
    }

    /** Over numbers, [[numericType]] -- the type every row answers, so DESCRIBE, a `CREATE TABLE …
      * AS SELECT` and JDBC metadata declare what is stored; otherwise the least common supertype of
      * the arguments' REPORTED types, as `COALESCE` reports.
      */
    override def reportedType: SQLType =
      numericType.getOrElse(SQLTypeUtils.leastCommonSuperType(reportedArgTypes))

    override def nullable: Boolean = values.forall(_.nullable)

    /** Can this argument's rendering be NULL here?
      *
      * 🔴 In a group filter (`HAVING`), an argument that reads a metric is NULL whenever the group
      * has none of its values -- the filter reads every aggregate as SELECT returns it
      * (`MetricSelectorScript.nullAwareSelectorScript`) -- so it is skipped like any other NULL
      * argument, the rule the docs state and WHERE applies to a missing column. `nullable` alone
      * answers `false` for an aggregate, and the reducer emitted `Math.max(params.m, k)`: only the
      * comparison's guard kept it from throwing, and that guard dropped the group as soon as ONE
      * argument was NULL. MEASURED on Elasticsearch 8.18.3: 5 of 6 shapes kept the wrong groups.
      *
      * Asked of the group filter alone ([[query.MetricSelectorScript.rendersGroupFilter]]): the
      * SELECT list's `bucket_script` renders this same function context-free, and its gap policy
      * never hands it a NULL metric, so its script does not move.
      */
    private def nullableArgument(argument: PainlessScript, context: Option[PainlessContext]) =
      argument.nullable || (context.isEmpty && query.MetricSelectorScript.rendersGroupFilter &&
      (argument match {
        case id: Identifier => id.bucketMetrics.nonEmpty
        case _              => false
      }))

    // Pair each rendered arg with its nullability so non-nullable args (e.g.
    // numeric literals, which render inline as primitives) are NOT guarded
    // with `== null` — Painless rejects `<primitive> == null` at compile time.
    // Right-fold pairwise: pairwise(a, pairwise(b, pairwise(c, …))).
    // The combined sub-tree is itself nullable only when BOTH sides can be
    // null, which lets us drop the guard on parent levels too.
    private def fold(args: List[(String, Boolean)], integral: Boolean): (String, Boolean) =
      args match {
        case Nil =>
          throw new IllegalArgumentException(s"$operator requires at least one argument")
        case (s, n) :: Nil => (s.trim, n)
        case (s, xNullable) :: rest =>
          val x = s.trim
          val (y, yNullable) = fold(rest, integral)
          val best = if (integral) winner(x, y) else s"$mathFn($x, $y)"
          val expr =
            (xNullable, yNullable) match {
              case (true, true)   => s"($x == null ? $y : ($y == null ? $x : $best))"
              case (true, false)  => s"($x == null ? $y : $best)"
              case (false, true)  => s"($y == null ? $x : $best)"
              case (false, false) => best
            }
          // result is null only if every branch can be null
          (expr, xNullable && yNullable)
      }

    /** The argument that wins, chosen by a comparison: Painless's `Math.max` / `Math.min` take
      * `double`s only, so over whole numbers they answered `3.0` for `GREATEST(n, 2)` over an
      * INTEGER `n` -- the right number in the wrong type (issue #380) -- and lost the precision of
      * a BIGINT past 2^53. The winner is then converted with every other path ([[typedResult]]).
      * Two whole-number literals are compared now, so the winner is the literal itself -- never a
      * conditional over constants, which Elasticsearch 6.8 refuses as a whole script.
      */
    private def winner(x: String, y: String): String = {
      def literal(v: String): Option[BigInt] =
        if (WholeLiteral.pattern.matcher(v).matches())
          scala.util.Try(BigInt(v.stripSuffix("L").stripSuffix("l"))).toOption
        else None
      (literal(x), literal(y)) match {
        case (Some(a), Some(b)) => if (if (greatest) a >= b else a <= b) x else y
        case _                  => s"($x $keepsLeft $y ? $x : $y)"
      }
    }

    /** Are the arguments whole numbers -- [[numericType]] is one -- each rendered as a name, a
      * literal or one parenthesised group, compared per document? Then the winner is chosen by
      * [[winner]]. Per group -- no context, or a view's per-group calculation -- every metric is
      * the double Elasticsearch computed, and the reducer keeps `Math.max` / `Math.min`; so does an
      * argument rendered as a bare conditional (a function's NULL guard), which a comparison would
      * read wrongly, and a bare `NULL`, which has no type and no comparison.
      */
    private def overWholeNumbers(
      callArgs: List[String],
      context: Option[PainlessContext]
    ): Boolean =
      context.exists(!_.isTransform) && numericType.exists(NumericReducer.wholeNumber) &&
      !values.exists(ArithmeticExpression.bareNull) &&
      callArgs.forall(a => NumericReducer.oneValue(a.trim))

    /** The value converted to [[numericType]], per document: ONE conversion every path goes through
      * -- the winner, each NULL-skipping branch, a nested `GREATEST` / `LEAST`, a literal argument.
      *
      * The fold hands back the argument that survives as it is (`x == null ? y : …` answers `y`
      * itself), and the winner as it is, so a column answered two classes: `GREATEST(n, x)` gave
      * the INT `n` where the DOUBLE `x` is NULL (`Integer` -2 beside `Double` values), and
      * `GREATEST(GREATEST(n, 0), x)` the `Integer` 0. MEASURED on Elasticsearch 8.18.3 and 6.8.23.
      * `Math.max` hid it while it answered a double, except in the NULL-skipping branches.
      *
      * Per group -- no context, or a view's per-group calculation -- every metric is the double
      * Elasticsearch computed, and the rendering is the one it always was. With a number literal
      * among the arguments the value is never NULL and is converted as it stands; otherwise it is
      * read once, under a name, behind a NULL guard whose NULL branch is that name -- a `def`, as
      * the converted value is, where a `null` literal would make the conditional unusable as the
      * argument of an outer `Math.max` (`Cannot cast null to a primitive type [double]`).
      */
    private def typedResult(value: String, context: Option[PainlessContext]): String =
      perDocumentType(context) match {
        case Some((ctx, t)) =>
          val cast = SQLTypeUtils.painlessType(t)
          val v = value.trim
          if (values.exists(NumericReducer.numberLiteral))
            s"(($cast) ${if (NumericReducer.oneValue(v)) v else s"($v)"})"
          else {
            val isName =
              ArithmeticExpression.isBareName(v) && (v.head.isLetter || v.head == '_')
            val name = if (isName) v else ctx.bindLocal(v, "lv")
            s"($name == null ? $name : (def) (($cast) $name))"
          }
        case _ => value
      }

    /** The context a value is converted in, and the type it is converted to: per document only. */
    private def perDocumentType(
      context: Option[PainlessContext]
    ): Option[(PainlessContext, SQLNumeric)] =
      for {
        ctx <- context.filterNot(_.isTransform)
        t   <- numericType
      } yield (ctx, t)

    /** A literal argument as a literal of [[numericType]] where its own spelling is not one: a
      * whole number past the range of an `int` is a `long` literal (`3000000000L`) beside a BIGINT,
      * which Painless refuses to compile without its suffix (`Invalid int constant`).
      */
    private def typedArgument(
      rendered: String,
      argument: PainlessScript,
      context: Option[PainlessContext]
    ): String =
      perDocumentType(context) match {
        case Some((_, SQLTypes.BigInt)) if NumericReducer.numberLiteral(argument) =>
          val r = rendered.trim
          if (
            WholeLiteral.pattern.matcher(r).matches() && !r.endsWith("L") && !r.endsWith("l") &&
            !BigInt(r).isValidInt
          ) s"${r}L"
          else rendered
        case _ => rendered
      }

    override def toPainlessCall(
      callArgs: List[String],
      context: Option[PainlessContext]
    ): String =
      if (overDates) overDatesPainless(callArgs, context)
      else {
        val typedArgs =
          callArgs.zip(values).map { case (rendered, argument) =>
            typedArgument(rendered, argument, context)
          }
        typedResult(
          typedArgs match {
            case Nil =>
              throw new IllegalArgumentException(s"$operator requires at least one argument")
            case x :: Nil => x
            case _ =>
              fold(
                typedArgs.zip(values.map(nullableArgument(_, context))),
                overWholeNumbers(typedArgs, context)
              )._1
          },
          context
        )
      }

    /** The same fold over the epoch milliseconds each date denotes, then the instant that wins.
      *
      * Per document each argument is converted once, under a name, behind its own NULL guard (a
      * NULL is skipped by the fold, never converted), and the winner is handed back as the UTC
      * `ZonedDateTime` it denotes. Per group -- no context, or a view's per-group calculation -- a
      * metric IS the epoch milliseconds Elasticsearch computed for its date, and may be NULL in a
      * group filter: it is read through the same conversion (a DATE metric is its UTC day's start,
      * as date arithmetic reads it per group: a `date` field accepts a date-time, and its MAX keeps
      * that time), behind a NULL guard where it can be NULL, and the winner stays that number, as a
      * date aggregate is there. A NULL literal can never win: it is left out.
      */
    private def overDatesPainless(
      callArgs: List[String],
      context: Option[PainlessContext]
    ): String = {
      if (callArgs.isEmpty)
        throw new IllegalArgumentException(s"$operator requires at least one argument")
      val perDocument = context.filterNot(_.isTransform)
      val millis = callArgs.zip(values).filterNot(a => ArithmeticExpression.nullLiteral(a._2)).map {
        case (ref, argument) =>
          val nullable = nullableArgument(argument, context)
          perDocument match {
            case Some(ctx) =>
              val name =
                if (ArithmeticExpression.isBareName(ref.trim)) ref.trim
                else ctx.bindLocal(ref, "lv")
              val converted = ArithmeticExpression.instantMillis(argument, name, context)
              val guarded =
                if (nullable) s"($name == null ? null : (def)($converted))" else converted
              (ctx.bindLocal(guarded, "lv"), nullable)
            case None if argument.isAggregation =>
              val metric = ref.trim
              val converted = ArithmeticExpression.instantMillis(argument, metric, context)
              // A NULL metric (a group filter's, over a group with none of its values) is handed
              // back as it is -- the same NULL, which the fold skips. The group filter's gate
              // (`MetricSelectorScript.disqualifyRendering`) refuses a rendering that spells
              // `? null :` or reads `def`, so neither is written: the conditional is a `def` by its
              // first branch, the metric itself.
              if (converted == metric) (metric, nullable)
              else if (nullable) (s"($metric == null ? $metric : $converted)", nullable)
              else (converted, nullable)
            case None =>
              (ArithmeticExpression.instantMillis(argument, ref.trim, context), nullable)
          }
      }
      if (millis.isEmpty) return if (perDocument.isEmpty) "((def) null)" else "null"
      val (winner, nullable) = fold(millis, integral = false)
      perDocument match {
        case Some(ctx) =>
          val name = if (nullable) ctx.bindLocal(winner, "lv") else s"($winner)"
          val instant = s"Instant.ofEpochMilli((long) $name).atZone(ZoneId.of('Z'))"
          if (nullable) s"($name == null ? null : $instant)" else instant
        case None => winner
      }
    }
  }

  case class Greatest(values: List[PainlessScript]) extends NumericReducer {
    override def operator: ConditionalOp = Greatest
    override protected def mathFn: String = "Math.max"
    override protected def greatest: Boolean = true

    override def update(request: query.SingleSearch): Greatest =
      this.copy(values = values.map {
        case u: Updateable => u.update(request).asInstanceOf[PainlessScript]
        case other         => other
      })
  }

  case class Least(values: List[PainlessScript]) extends NumericReducer {
    override def operator: ConditionalOp = Least
    override protected def mathFn: String = "Math.min"
    override protected def greatest: Boolean = false

    override def update(request: query.SingleSearch): Least =
      this.copy(values = values.map {
        case u: Updateable => u.update(request).asInstanceOf[PainlessScript]
        case other         => other
      })
  }

  object NumericReducer {

    /** A type that carries a date -- DATE, TIMESTAMP, DATETIME, or a temporal function's TEMPORAL
      * -- which `GREATEST` / `LEAST` compare as instants. A TIME carries none.
      */
    private[sql] def dateCarrying(t: SQLType): Boolean =
      t == SQLTypes.Date || t == SQLTypes.Timestamp || t == SQLTypes.DateTime ||
      t == SQLTypes.Temporal

    /** A whole-number type: TINYINT, SMALLINT, INT or BIGINT. */
    private[cond] def wholeNumber(t: SQLType): Boolean = t match {
      case _: SQLTinyInt | _: SQLSmallInt | _: SQLInt | _: SQLBigInt => true
      case _                                                         => false
    }

    /** A number literal -- bare, or the one function of the nameless identifier the parser wraps an
      * operand in -- which is never NULL.
      */
    private[cond] def numberLiteral(v: PainlessScript): Boolean = v match {
      case _: NumericValue[_] => true
      case i: Identifier if i.name.trim.isEmpty =>
        i.functions match {
          case (_: NumericValue[_]) :: Nil => true
          case _                           => false
        }
      case _ => false
    }

    /** A rendering that is ONE value wherever it is placed: a name, a literal, or a single
      * parenthesised group.
      */
    private[cond] def oneValue(rendered: String): Boolean =
      simpleOperand.pattern.matcher(rendered).matches() || (rendered.startsWith("(") && {
        var depth = 0
        var i = 0
        var closedEarly = false
        while (i < rendered.length && !closedEarly) {
          rendered.charAt(i) match {
            case '(' => depth += 1
            case ')' =>
              depth -= 1
              if (depth == 0 && i < rendered.length - 1) closedEarly = true
            case _ =>
          }
          i += 1
        }
        !closedEarly && depth == 0
      })

    /** Is this operand -- or the nameless identifier carrying it -- `GREATEST` / `LEAST` over
      * dates? Per group its value is the epoch milliseconds of the date that wins
      * (`ArithmeticExpression.epochMillis`).
      */
    private[sql] def overDates(operand: PainlessScript): Boolean = operand match {
      case reducer: NumericReducer => reducer.overDates
      case i: Identifier if i.name.trim.isEmpty =>
        i.functions match {
          case (reducer: NumericReducer) :: Nil => reducer.overDates
          case _                                => false
        }
      case _ => false
    }
  }
}
