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

package app.softnetwork.elastic.sql.operator.math

import app.softnetwork.elastic.sql._
import app.softnetwork.elastic.sql.`type`._
import app.softnetwork.elastic.sql.function.{BinaryFunction, FunctionChain, TransformFunction}
import app.softnetwork.elastic.sql.function.aggregate.AggregateFunction
import app.softnetwork.elastic.sql.function.cond.{functionsOf, NumericReducer}
import app.softnetwork.elastic.sql.function.time.DateDiff
import app.softnetwork.elastic.sql.parser.Validator
import app.softnetwork.elastic.sql.query.NestedElement

import scala.util.Try

case class ArithmeticExpression(
  left: PainlessScript,
  operator: ArithmeticOperator,
  right: PainlessScript,
  group: Boolean = false
) extends TransformFunction[SQLNumeric, SQLNumeric]
    with BinaryFunction[SQLNumeric, SQLNumeric, SQLNumeric]
    with Updateable {

  override def fun: Option[ArithmeticOperator] = Some(operator)

  override def inputType: SQLNumeric = SQLTypes.Numeric
  override def outputType: SQLNumeric = SQLTypes.Numeric

  /** Date arithmetic hands its OWN type to the chain it heads, so that a cast over it converts from
    * what it computes: `(d - d2)::TIMESTAMP` is a number of days cast to TIMESTAMP, which is core's
    * epoch-millisecond conversion, as for any other number. Every other arithmetic keeps handing on
    * the type it is given, as it always has.
    */
  override def applyType(in: SQLType): SQLType = dateArithmetic.flatMap(_.runtimeType).getOrElse(in)

  /** A cast OVER this expression converts what it computes; it does not change what it computes.
    *
    * 🔴 A conversion's constructor casts its operand (`value.cast(targetType)`), and on an
    * arithmetic that set `out` -- the type the arithmetic coerces its OWN operands to. So `(x *
    * 2)::INT` truncated `x` BEFORE multiplying (1.6 * 2 answered 2, not 3), and `(n + x)::BIGINT`
    * -- whose operands are nullable, so not coerced at all -- answered 3.25 while the conversion,
    * reading the cast type back as its source, emitted nothing. MEASURED on main. The arithmetic
    * now keeps its own type and the conversion applies core's cast rule to its RESULT
    * (`convert.Conversion`), as SQL engines do: `(n + x)::BIGINT` is 3 for 1 + 2.25.
    */
  override def cast(targetType: SQLType): SQLType = out

  override def sql: String = {
    val expr = s"${left.sql}$operator${right.sql}"
    if (group)
      s"($expr)"
    else
      expr
  }

  override def args: List[PainlessScript] = List(left, right)

  /** The type an operand's rendering really PRODUCES — the question [[toPainless]] already asks of
    * the SOURCE of each coercion, asked here of the TARGET as well (issue #382, the lead's OQ-2).
    *
    * 🔴 `FunctionN.argTypes` answers `_.out`, and a schema-resolved `Identifier.out` reports the
    * COLUMN's type, not the chain's. So `CAST(name AS BIGINT) + m` over a KEYWORD column reported
    * `List(KEYWORD, BIGINT)`, `leastCommonSuperType` came out VARCHAR, and `coerce`'s `(_, _:
    * SQLLiteral)` arm wrapped BOTH operands in `String.valueOf`: Painless CONCATENATED them, a
    * BIGINT mapping coerced the result happily, and `SCRIPT AS (CAST(raw AS BIGINT) + m)` stored
    * **1257** where 132 was asked for. HTTP 200 — the #205 silent-wrong-answer family.
    *
    * The rule was already written down one scaladoc below: *"Coerced FROM what the operand RENDERS,
    * not from its column's type"* (#367). It was applied to the FROM and never to the TO.
    *
    * Repaired HERE rather than at a second local in `toPainless`, because `argTypes` is the single
    * input to [[baseType]], `baseType` feeds `out`, and `out` is the coercion target in BOTH
    * renderings — `toPainless` for the nullable path and `painless` for the other. One derivation
    * fixes both; a local would have left `painless` broken. It also stops the expression DESCRIBING
    * itself as a string: `out` is what a CTAS writes into the target mapping.
    *
    * The non-`Identifier` arm keeps today's `_.out` exactly: a nested arithmetic renders what its
    * own `out` says, and a literal's `out` already carries any cast applied to it.
    */
  private def argTypeOf(operand: PainlessScript): SQLType =
    literalNumber(operand)
      .map(_.sqlType)
      .getOrElse(operand match {
        case i: Identifier => ArithmeticExpression.identifierType(i)
        case other         => other.out
      })

  /** A string LITERAL beside a NUMBER is the number it spells -- the ONE comparison rule's reading
    * (`ComparisonRule.readingOf`), as PostgreSQL reads `n + '1'`: 4 for n = 3, where the
    * concatenation `"31"` was answered. Beside a whole-number operand only a whole number is one:
    * PostgreSQL refuses `n + '1.5'` over an integer `n` (`invalid input syntax for type integer`),
    * and so does the operand match here, the literal staying text. Beside anything else it stays
    * what it is (a temporal beside it is date arithmetic, which reads it itself).
    *
    * Each operand's reading is computed ONCE per expression and memoised (`ComparisonRule` says why
    * with no lock): every `argTypes` (so every `out`) and every rendering asks it, and a reading is
    * a parse.
    */
  private def literalNumber(operand: PainlessScript): Option[ComparisonRule.NumberReading] =
    if (operand eq left) {
      var number = _leftNumber
      if (number eq null) {
        number = numberBeside(left, right)
        _leftNumber = number
      }
      number
    } else if (operand eq right) {
      var number = _rightNumber
      if (number eq null) {
        number = numberBeside(right, left)
        _rightNumber = number
      }
      number
    } else numberBeside(operand, left)

  private[this] var _leftNumber: Option[ComparisonRule.NumberReading] = null
  private[this] var _rightNumber: Option[ComparisonRule.NumberReading] = null

  private def numberBeside(
    operand: PainlessScript,
    other: PainlessScript
  ): Option[ComparisonRule.NumberReading] =
    ComparisonRule
      .readingOf(operand, ComparisonRule.declaredTypeOf(other))
      .collect { case number: ComparisonRule.NumberReading => number }

  /** The temporal a string literal operand is read as beside the other one
    * (`ArithmeticExpression.temporalString`: `d - '2024-01-31'`), computed ONCE per expression and
    * memoised -- the date-arithmetic rendering asks it per operand and per guard.
    */
  private def literalTemporal(operand: PainlessScript): Option[ComparisonRule.TemporalReading] =
    if (operand eq left) {
      var temporal = _leftTemporal
      if (temporal eq null) {
        temporal = ArithmeticExpression.temporalString(left, right)
        _leftTemporal = temporal
      }
      temporal
    } else if (operand eq right) {
      var temporal = _rightTemporal
      if (temporal eq null) {
        temporal = ArithmeticExpression.temporalString(right, left)
        _rightTemporal = temporal
      }
      temporal
    } else ArithmeticExpression.temporalString(operand, left)

  private[this] var _leftTemporal: Option[ComparisonRule.TemporalReading] = null
  private[this] var _rightTemporal: Option[ComparisonRule.TemporalReading] = null

  override def argTypes: List[SQLType] = args.map(argTypeOf)

  /** 🔴 `/` decides its OWN result type: a division of numbers is FLOATING-POINT whatever its
    * operands are (issue #382, the lead's ruling of 2026-09-23). The result type of a division is a
    * property of the OPERATOR, not of its arguments -- `+`, `-`, `*` and `%` keep deriving theirs
    * from what the operands render, which is what [[argTypeOf]] above repaired.
    *
    * Two defects close with it, both MEASURED on real Elasticsearch 8.18.3:
    *
    *   - `SELECT n / m` over two INTEGER columns answered `3` for 7/2 while the SAME statement
    *     inside a JOIN answered `3.5`, because `JoinPlanner` hands the SELECT list to DuckDB
    *     (`JoinPlanner.scala:1093`) and DuckDB -- like MySQL -- treats `/` as always-decimal. The
    *     divergence therefore existed on `main` for a plain `n / m`; aligning `/` removes the class
    *     rather than one instance of it.
    *   - `CREATE TABLE u (n INTEGER, m INTEGER, c DOUBLE SCRIPT AS (n / m))` emitted `param1 /
    *     param2` with no coercion at all, so a column the user DECLARED `DOUBLE` stored `3.0` for
    *     7/2. The declared type was ignored; now the expression itself is DOUBLE, so it no longer
    *     has to be consulted.
    *
    * The fold must be NUMERIC or UNKNOWN. `s / 2` over a KEYWORD column folds to VARCHAR and is
    * left exactly as it was, so this ruling widens nothing outside arithmetic.
    *
    * 🔴 UNKNOWN is not a refinement, it is half the population. A column carries a type only once a
    * schema is attached, and `SearchApi.resolveWithSchema` DECLINES to attach one for a wildcard
    * FROM, a comma-separated FROM, a client that is not an `IndicesApi`, and any mapping whose load
    * failed or is negatively cached. Every column is then `SQLTypes.Any`, and
    * `leastCommonSuperType` short-circuits `[Any, Any]` to `Any` -- which is not an `SQLNumeric`.
    * So on a numeric-fold reading alone `SELECT n / m FROM logs_2025` answered 3.5 while the SAME
    * statement over `logs-*` answered 3, and, worse, [[divisorMayBeZero]] 's guard was not emitted
    * either: a zero divisor stayed an `arithmetic_exception` in a search and a REJECTED DOCUMENT on
    * ingest, which is the data-loss bug the guard exists to close. `operators.md` promises both
    * unconditionally, so the rule has to hold unconditionally. (Found by review; `[Any, BigInt]`
    * folds to `BigInt` and was already floating, which is why only the both-unresolved shape was
    * wrong -- and why no schema-bearing test could see it.)
    *
    * ⚠️ There is NO truncating-division spelling today, and this comment says so rather than
    * implying one. Keying [[floatingDivision]] on `out` means an explicit cast over the whole
    * division would turn the rule back off with no arm of its own -- but the grammar rejects an
    * ARITHMETIC EXPRESSION as the operand of a function, of a `CAST` or of a `CASE` branch, so that
    * is a property of the design, not a reachable path:
    * {{{
    * CAST(a / b AS INTEGER)   CAST(a + b AS INTEGER)   CAST((a / b) AS INTEGER)   REJECTED
    * FLOOR(a / b)             COALESCE(a / b, 0)       CASE WHEN … THEN a / b END REJECTED
    * FLOOR(x)                 a / NULLIF(b, 0)                                    ACCEPTED
    * }}}
    * The rule is DIRECTIONAL -- `f(<arithmetic>)` is rejected, `<arithmetic> f(…)` is fine -- and
    * it is not about division: ANY arithmetic operand is refused, and parenthesising does not help.
    * Measured on a clean clone at `origin/main`, so it is pre-existing and UNFILED. The documented
    * way to truncate is to compute the quotient into a column and cast THAT column.
    */
  override def baseType: SQLType = dateArithmetic.flatMap(_.runtimeType).getOrElse {
    val types = argTypes
    // 🔴 `+` and `-` answer UNKNOWN while an operand's type is unknown -- a column before its
    // schema is attached, a field no schema describes. They are the two operators a DATE may
    // take, so the fold could not know the answer: `d + 7` folded `[ANY, BIGINT]` to BIGINT, and
    // `d + 7 > CURRENT_DATE` was refused as BIGINT against DATE before the column's type was ever
    // read. `*`, `/` and `%` keep their fold: no date survives them.
    if ((operator == ADD || operator == SUBTRACT) && types.exists(_.isUnknown)) SQLTypes.Any
    else {
      val folded = SQLTypeUtils.leastCommonSuperType(types)
      if (operator == DIVIDE && (folded.isNumber || folded.isUnknown)) SQLTypes.Double else folded
    }
  }

  /** What the date-arithmetic rules make of this expression (see [[ArithmeticExpression]] 's
    * companion), or `None` when neither operand is a DATE, a TIMESTAMP or a DATETIME -- every such
    * expression keeps the derivation and the rendering it always had.
    *
    * 🔴 Decided on the operands' DECLARED types, never on `out`. Elasticsearch stores DATE,
    * TIMESTAMP and DATETIME alike as a `date`, so the type a query hands Painless (`runtimeType`)
    * is TIMESTAMP for all three, and the difference between a DATE and a TIMESTAMP -- days between
    * two dates, or a fraction of a day between two instants -- lives only in the declaration.
    * Folding `out` is what typed `MAX(d) - MIN(d)` TIMESTAMP and answered it in milliseconds.
    *
    * Computed once per instance: `out` reads `baseType` on every call, and the operands of an
    * instance do not change -- resolution (`update`) builds a new one.
    */
  private[sql] lazy val dateArithmetic: Option[ArithmeticExpression.DateArithmetic] =
    ArithmeticExpression.dateArithmetic(this)

  /** The type a consumer DECLARING a column for this expression reports: DATE for a date moved by
    * whole days, although Painless renders it as the UTC instant that day starts at.
    */
  override def reportedType: SQLType = dateArithmetic.flatMap(_.reportedType).getOrElse(out)

  /** The by-name refusal the DATE-ARITHMETIC rules give this expression, if any: a temporal operand
    * under `*`, `/` or `%`, two temporals added, a number minus a temporal, or a temporal combined
    * with something that is neither a number nor a temporal.
    */
  def dateArithmeticError: Option[String] =
    dateArithmetic.collect { case ArithmeticExpression.Refused(message) => message }

  /** The by-name refusal this expression earns once the types of its operands are known, if any:
    * [[dateArithmeticError]], or -- for any other arithmetic -- two operands whose types do not
    * match, the rule `validate()` used to apply when the statement was parsed, where every column
    * is still `Any`.
    *
    * 🔴 Asked AFTER the schema is attached (`SingleSearch.validateResolved`), never at parse time:
    * no type is refused before the column types are known (the lead's ruling of 2026-10-05). Each
    * operand's type is the one it RENDERS ([[ArithmeticExpression.identifierType]]), not `out`,
    * which a resolved column answers with its column's type whatever function it wears.
    */
  override def typeError: Option[String] =
    dateArithmetic match {
      case Some(_) => dateArithmeticError
      case None =>
        Validator.validateTypesMatching(argTypeOf(left), argTypeOf(right)).left.toOption
    }

  /** [[dateArithmeticError]], here and in every arithmetic among the operands, however deep. */
  private[sql] def dateArithmeticErrors: Seq[String] =
    dateArithmeticError.toSeq ++ args.flatMap {
      case nested: ArithmeticExpression => nested.dateArithmeticErrors
      case chain: FunctionChain         => ArithmeticExpression.typeErrorsOf(chain)
      case _                            => Nil
    }

  /** Is this the numeric division the ruling above governs? Keyed on the coercion TARGET rather
    * than on [[baseType]] so that an explicit `CAST(a / b AS INTEGER)` -- which sets `out` -- is
    * integer division again, with no arm of its own.
    *
    * 🔴 The target is PASSED, never re-read, everywhere below. `out` folds
    * `leastCommonSuperType(argTypes)` on every call, and the three division decisions plus the two
    * coercions would otherwise ask for it EIGHT times per rendering (the site asked four times
    * before this ruling). One `val` per rendering measurably undoes that.
    */
  private def floatingDivision(target: SQLType): Boolean =
    operator == DIVIDE && target == SQLTypes.Double

  /** 🔴 Painless integer division by zero THROWS; floating division yields `Infinity`, and
    * Elasticsearch REFUSES to index a non-finite number. MEASURED on 8.18.3, for the same `n / m`
    * with `m = 0`:
    * {{{
    * integer   ingest (ignore_failure: true)  document indexed, column ABSENT
    * integer   search (script_fields)         HTTP 400, "all shards failed", / by zero
    * floating  ingest                         HTTP 400 document_parsing_exception -- the WHOLE
    *                                           document is REJECTED: "[double] supports only
    *                                           finite values, but got [Infinity]"
    * floating  search                         the string "Infinity" in the result column
    * }}}
    * So the ruling, unguarded, would turn "one column missing" into "the document is lost" on every
    * bulk ingest with a zero divisor -- and the second row shows it was never the documented `NULL`
    * either. The guard below restores what `documentation/sql/operators.md` has always promised, in
    * EVERY venue, and fixes the floating case that was already losing documents on `main` (a
    * `double` column divided by a zero `double` needs no cast to reach `Infinity`).
    *
    * It costs nothing where it cannot fire: a divisor that is a NON-ZERO numeric literal -- `/ 2`,
    * `/ 100`, the overwhelming majority -- is proved safe here and emits no guard at all.
    */
  private def divisorMayBeZero: Boolean = numericLiteral(right).forall(_.toDouble == 0.0)

  /** The divisor is a LITERAL zero, so the division is NULL for every document and no guard has to
    * decide it at runtime.
    *
    * 🔴 Folding it is not an optimisation, it is what makes the ruling work on Elasticsearch 6.8
    * (found by the es6 integration leg, which is the only place it shows). The guard renders as a
    * parenthesised conditional, and where the division is the WHOLE script -- a constant expression
    * like `SELECT 1/0`, with no `def param…` statements in front of it -- 6.8's Painless refuses a
    * body that is nothing but a conditional:
    * {{{
    * ((((double) 0) == 0) ? null : (def)(((double) 1) / ((double) 0)))
    *   illegal_argument_exception: Extraneous conditional statement.     <- ES 6.8 only
    * }}}
    * A division over COLUMNS is unaffected on every version, because the parameter declarations
    * precede the conditional (measured: `n / m` runs on 6.8). So the narrow case is folded rather
    * than the guard being reshaped for one release, and the answer is identical everywhere.
    */
  private def divisorIsLiteralZero: Boolean = numericLiteral(right).exists(_.toDouble == 0.0)

  /** 🔴 A numeric literal operand is NOT a `NumericValue`: `TypeParser.identifierWithValue` is
    * `(value ^^ functionAsIdentifier) >> cast`, so `/ 2` arrives as a `GenericIdentifier` with an
    * EMPTY name carrying `LongValue(2)` as its only function. Matching `NumericValue` directly --
    * the obvious spelling, and the one written first -- proved every divisor un-provable and
    * emitted `((double) 2) == 0` on every `/ 2` in the corpus. Measured, not reasoned.
    *
    * The unwrap is deliberately strict: an empty name and EXACTLY one function, so a cast or any
    * other chain (`>> cast` appends one) falls through to "may be zero" rather than being read as
    * the bare literal.
    */
  private def numericLiteral(operand: PainlessScript): Option[NumericValue[_]] = operand match {
    case n: NumericValue[_] => Some(n)
    case i: Identifier if i.name.isEmpty =>
      i.functions match {
        case (n: NumericValue[_]) :: Nil => Some(n)
        case _                           => None
      }
    case _ => None
  }

  /** The type an operand RENDERS as, before this site coerces anything.
    *
    * 🔴 Coerced FROM what the operand RENDERS, not from its column's type (#367's rule): `YEAR(d)`
    * renders an `int`, and reading `baseType` asked for a TIMESTAMP -> BIGINT conversion that
    * emitted `.toInstant()` on that `int`.
    *
    * 🔴 This and [[argTypeOf]] agree on the `Identifier` arm -- which is the whole of #382 -- and
    * DELIBERATELY still differ on the other one: this reads `baseType`, that one reads `out`.
    * Unifying them is arguably more accurate (a nested operand renders coerced to its own `out`,
    * not to its `baseType`) but it is an UNGUARDED change: restoring `baseType` in `argTypeOf`
    * leaves the whole estate green, so nothing here distinguishes the two for a non-`Identifier`
    * operand. Measured, recorded on #382, and not smuggled in.
    */
  private def renderedType(operand: PainlessScript): SQLType =
    literalNumber(operand)
      .map(_.sqlType)
      .getOrElse(operand match {
        case i: Identifier => ArithmeticExpression.identifierType(i)
        case other         => other.baseType
      })

  /** An operand as it renders, a string literal read as a number ([[literalNumber]]) as that
    * number.
    */
  private def operandPainless(
    operand: PainlessScript,
    idx: Int,
    context: Option[PainlessContext]
  ): String =
    literalNumber(operand)
      .map(_.painless)
      .getOrElse(operand match {
        case t: TransformFunction[_, _] => t.toPainless("", idx + 1, context)
        case _                          => operand.painless(context)
      })

  /** 🔴 A numeric coercion is skipped only over a NULLABLE operand, and the reason is exactly what
    * Painless refuses -- a primitive cast over a null-guarded expression, in BOTH directions,
    * MEASURED on ES 8.18:
    * {{{
    * ((double) (param1 != null ? … : null))  Cannot cast null to a primitive type [double].
    * ((long)   (param1 != null ? … : null))  Cannot cast null to a primitive type [long].
    * }}}
    * and the coercion TARGET is derived from the COLUMN (`out` folds the operands' `out`), so
    * `CAST(n AS DOUBLE) + 1` over an `INT` column asked DOUBLE -> BIGINT and emitted a cast that
    * both truncates and fails to compile.
    *
    * 🔴 It must NOT be skipped over a NON-nullable operand: a LITERAL carries no guard, and its
    * coercion is the only thing that makes the division floating-point. Skipping it turned `CAST(n
    * AS DOUBLE) / 2` into INTEGER division -- `[0.5, 2.5, 4.5]` became `[0, 2, 4]`, HTTP 200, in
    * script fields, `terms` keys, sort scripts AND predicates (a row silently vanished from `WHERE
    * … / 2 > 2`). Found by review, MEASURED on the schema-LESS rendering path -- which production
    * reaches whenever `resolveWithSchema` declines: a wildcard or multi-index FROM, a JOIN, or a
    * mapping that cannot be loaded. #205's silent-wrong-answer family, and NULLABILITY is the
    * discriminator that separates the two failures.
    *
    * 🔴 That reasoning still holds after the `/` ruling, and it is WHY the ruling needs
    * [[needsDoubleCast]]: the operand of `n / m` is skipped here, so making `out` DOUBLE alone
    * would leave `param1 / param2` -- two `def`s holding `Integer` -- dividing as integers. The
    * cast that fixes it therefore cannot live at this site; it goes INSIDE the null guard, where
    * the operand is known non-null.
    */
  private def coercionSkipped(operand: PainlessScript, target: SQLType): Boolean =
    operand.nullable && renderedType(operand).isNumber && target.isNumber

  private def isFloating(t: SQLType): Boolean = t == SQLTypes.Double || t == SQLTypes.Real

  /** What the operand will really be EMITTED as: its own rendering where the coercion is skipped,
    * and `out` where it is not.
    *
    * 🔴 "Where it is not" is NOT the same as "`target`", and the difference is a silent wrong
    * answer. [[SQLTypeUtils.coerce]] has no arm for an UNKNOWN source -- `(Any, Double)` falls to
    * its `case _ => return expr` -- so an operand whose rendering is `Any` is emitted UNCHANGED
    * while this method, reading `target`, believed it came back carrying the float. Visible in the
    * shipped bridge expectation `(param1 / ((double) 2))`: `param1` is bare, and the answer is
    * right only because the OTHER operand happens to carry the float.
    *
    * Reachable wrong answer: an `unsigned_long` field has no [[SQLTypes]] arm and therefore reports
    * `Any`, while its doc value is a `long`. Divided by an `integer` column the fold is numeric, so
    * [[floatingDivision]] is true, but this method answered DOUBLE for the unknown operand,
    * [[needsDoubleCast]] stood down, and `param1 / param2` divided as INTEGERS inside an expression
    * the engine reports as DOUBLE -- with the zero guard present, so it looks correct.
    *
    * So an unknown rendering answers UNKNOWN, which [[isFloating]] rejects, and the `(double)` is
    * emitted defensively. It costs a primitive cast on a `def` that already holds a number.
    *
    * ⚠️ This is also what kept the nullable ternary safe without a `(def)`: the only non-numeric
    * rendering that survives `Validator.validateTypesMatching` in a numeric fold is `Any`, and `Any
    * -> Double` was the identity, so the branch yielded a `def`. Giving `coerce` an `(Any, Double)`
    * arm later would produce a PRIMITIVE there and break it -- the two facts are one fact, and they
    * are written down together on purpose.
    */
  private def emittedType(operand: PainlessScript, target: SQLType): SQLType = {
    val rendered = renderedType(operand)
    if (coercionSkipped(operand, target)) rendered
    else if (rendered.isUnknown) rendered
    else target
  }

  /** Does the division need an explicit `(double)` on its left operand?
    *
    * Only when NEITHER operand already emits a floating value -- i.e. integer ÷ integer, both
    * null-guarded. Every other shape is already floating because `out` is DOUBLE and a non-skipped
    * operand is coerced to it (`x / 2` emits `param1 / ((double) 2)`), or because an operand
    * renders DOUBLE by itself (`x / y`).
    *
    * 🔴 Deliberately minimal, because `ScriptProcessor.source` is PERSISTED in `_meta` and
    * `IngestPipeline.diff` compares it: an unconditional cast would rewrite every stored computed
    * column that divides, including the ones that always answered correctly, and each would report
    * `ProcessorChanged` on the next ALTER. Only the shapes whose ANSWER changes move.
    */
  private def needsDoubleCast(target: SQLType): Boolean =
    floatingDivision(target) && !args.exists(operand => isFloating(emittedType(operand, target)))

  /** STRUCTURE only: the operands. Every TYPE rule of this expression -- the date-arithmetic rules
    * and the operand match alike -- is [[typeError]], asked once the column types are known (the
    * lead's ruling of 2026-10-05: no refusal before the column types are known). Asked here, at
    * parse time, it refused `d + 7 > CURRENT_DATE` as BIGINT against DATE before `d` had a type.
    */
  override def validate(): Either[String, Unit] =
    for {
      _ <- left.validate()
      _ <- right.validate()
    } yield ()

  /** Is an operand a NULL literal, bare or cast, in date arithmetic? Then the expression is NULL on
    * every row, whatever the other operand holds.
    */
  private def nullValued: Boolean =
    dateArithmetic.exists {
      case ArithmeticExpression.NullOperand => true
      case _: ArithmeticExpression.Computed =>
        ArithmeticExpression.nullLiteral(left) || ArithmeticExpression.nullLiteral(right)
      case _ => false
    }

  /** A NULL literal beside a temporal makes the expression NULL on every row, whatever the other
    * operand holds, so a consumer must guard it.
    */
  override def nullable: Boolean = left.nullable || right.nullable || nullValued

  override def toPainless(base: String, idx: Int, context: Option[PainlessContext]): String = {
    context match {
      case Some(ctx) =>
        ctx.addParam(left)
        ctx.addParam(right)
      case _ =>
    }
    if (nullValued) return s"$base${nullOperandPainless(context)}"
    dateArithmetic match {
      case Some(computed: ArithmeticExpression.Computed) =>
        return s"$base${dateArithmeticPainless(computed, idx, context)}"
      case _ =>
    }
    if (nullable) {
      // The coercion target, asked ONCE -- see `floatingDivision` for why that matters.
      val target = out
      // 🔴 The operand is RENDERED, never read back as a parameter NAME (issue #373, item 5).
      // `ctx.get(left)` answers with the name of the parameter the identifier registered -- the
      // RAW doc-value -- so the function chain never reached the expression and
      // `YEAR(d) * 100 + MONTH(d)` computed `d * 100 + d`. It is #367's defect one layer up: the
      // shortcut is sound only where the parameter IS the whole operand, which is exactly what a
      // bare-name rendering means. MEASURED on the real DDL path, an ingest processor stored
      // `ctx.c = (param1 == null || param1 == null) ? null : (param1 + param1)` with
      // `param1 = ctx.d` -- the duplicated null guard is the fingerprint of the collapse.
      def coerced(rendered: String, operand: PainlessScript): String =
        if (coercionSkipped(operand, target)) rendered
        else
          SQLTypeUtils.coerce(rendered, renderedType(operand), target, nullable = false, context)

      def render(operand: PainlessScript): String =
        coerced(operandPainless(operand, idx, context), operand)

      /** A rendering that IS a Painless name needs no local of its own. */
      def isBareName(e: String): Boolean =
        e.nonEmpty && e.forall(c => c.isLetterOrDigit || c == '_')

      /** 🔴 The parameter shortcut is KEPT where there is no chain to drop, and losing it is what
        * broke five working predicates in the first version of this commit (found by review).
        * `render` always coerces, so a bare `INT` column against a BIGINT target emitted `((long)
        * param1)` -- not a name -- and a `def lv0 = …; ` was prepended into a slot that must be a
        * single EXPRESSION: `WHERE n + 1 > 2` became `def left1 = def lv0 = …; …`, a compile error,
        * MEASURED on ES 8.18 where it returned rows before.
        *
        * The defect this commit fixes is the chain being DROPPED, so bypassing the shortcut is
        * needed exactly when the operand HAS a chain -- and only then.
        */
      def shortcut(operand: PainlessScript): Option[String] = operand match {
        case i: Identifier if i.functions.isEmpty => context.flatMap(_.get(i))
        case _                                    => None
      }

      /** A rendering that is not an EXPRESSION cannot be placed anywhere an operand goes -- not in
        * a prologue local, not inline -- so the operand falls back to its registered parameter.
        * That is what this site did before, and it is why `TRY_CAST(name AS BIGINT) + 1` worked:
        * Painless has no expression-level `try`, so a safe cast renders as a STATEMENT and coercing
        * it produced `def lv1 = String.valueOf(try { … } catch …);`, which ES rejects. Dropping the
        * chain is wrong, but a compile error is strictly worse -- recorded as a residual.
        */
      def placeable(rendered: String): Boolean = PainlessOperandForm.placeable(rendered)

      def operandOf(operand: PainlessScript): String =
        shortcut(operand).getOrElse {
          val rendered = render(operand)
          if (placeable(rendered)) rendered
          else context.flatMap(_.get(operand)).getOrElse(rendered)
        }

      val l = operandOf(left)
      val r = operandOf(right)
      var expr = ""

      /** 🔴 A Painless `def x = …;` is a STATEMENT: it cannot sit inside a parenthesised operand. A
        * non-bare rendering is therefore declared in the script's PROLOGUE, beside the `param`
        * assignments, and the operand becomes its NAME. `PainlessContext.bindLocal` exists for
        * exactly this (story BIDC-8 AD-9; #367 added `bindLocalWith` for `TRY_CAST`), and splicing
        * the declaration INLINE instead is what broke every CAST operand in the earlier versions of
        * this commit -- found by review, twice. `WHERE CAST(n AS BIGINT) + 1 > 2` emitted `def
        * left1 = def lv0 = …;` and ES answered `invalid_argument_exception: invalid sequence of
        * tokens near ['def']`.
        *
        * Only the side the null guard NAMES is bound: binding the other would leave a declaration
        * nothing reads. With NO context there is no prologue to hoist into, so that rendering keeps
        * the inline form it has always had -- pre-existing, and not this commit's to change.
        */
      def bind(rendered: String, fallback: String): String =
        context match {
          case Some(ctx) => ctx.bindLocal(rendered, "lv")
          case None =>
            expr += s"def $fallback = $rendered; "
            fallback
        }

      val leftParam =
        if (left.nullable && !isBareName(l)) bind(l, s"lv$idx") else l

      /** 🔴 The divisor is bound when it is not a bare NAME, even if it is not nullable -- because
        * the zero guard reads it a SECOND time. `$rightParam` appears in `($rightParam == 0)` and
        * again inside the division, so an operand that is not a name is EVALUATED TWICE, and if it
        * is not pure the guard tests a different value than the division uses: `x / RANDOM()`
        * emitted `Math.random() == 0 ? null : (param1 / Math.random())`, two independent draws, so
        * the guard was decorative. Nullability was the right condition while the guard was the only
        * reader; it stopped being so when [[divisorMayBeZero]] added a second one.
        *
        * A non-nullable, non-name LEFT operand needs no such binding: it is read once.
        */
      val rightParam =
        if (!isBareName(r) && (right.nullable || (floatingDivision(out) && divisorMayBeZero)))
          bind(r, s"rv$idx")
        else r

      /** 🔴 ONE guard list, replacing three hand-written branches that said the same thing.
        * `leftParam` IS `l` when the left operand is not nullable and `rightParam` IS `r` when the
        * right one is not, so the three arms were already the same string with different subsets of
        * the same conditions -- byte-identical output for `+`, `-`, `*` and `%`, and the only shape
        * that can carry a THIRD condition is the division's zero divisor, which no two-branch form
        * had room for.
        *
        * 🔴 The `(double)` goes INSIDE the guard, on the left operand, where the value is known
        * non-null -- a primitive cast over the guarded expression itself is what Painless refuses
        * (see [[coercionSkipped]]). One side is enough: Painless promotes `double / def` to double.
        */
      val lhs = if (needsDoubleCast(target)) s"((double) $leftParam)" else leftParam
      val guards =
        (if (left.nullable) List(s"$leftParam == null") else Nil) :::
        (if (right.nullable) List(s"$rightParam == null") else Nil) :::
        (if (floatingDivision(target) && divisorMayBeZero && !divisorIsLiteralZero)
           List(s"$rightParam == 0")
         else Nil)
      if (floatingDivision(target) && divisorIsLiteralZero)
        expr += "null"
      else
        expr += s"(${guards.mkString(" || ")}) ? null : ($lhs ${operator
          .painless(context)} $rightParam)"
      if (group)
        expr = s"($expr)"
      return s"$base$expr"
    }
    s"$base${painless(context)}"
  }

  override def painless(context: Option[PainlessContext]): String =
    if (nullValued) nullOperandPainless(context)
    else
      dateArithmetic match {
        case Some(computed: ArithmeticExpression.Computed) =>
          dateArithmeticPainless(computed, 0, context)
        case _ => numericPainless(context)
      }

  /** A NULL literal beside a temporal: NULL. A `bucket_script` returns a Number, and a bare `null`
    * is an Object to Painless -- `Cannot cast from [java.lang.Object] to [java.lang.Number]` at
    * compile time (MEASURED, ES 8.18.3); typed `def`, it compiles and the group has no value, as it
    * has for any NULL operand.
    */
  private def nullOperandPainless(context: Option[PainlessContext]): String = {
    val value = if (context.isEmpty) "((def) null)" else "null"
    if (group) s"($value)" else value
  }

  /** Date arithmetic, rendered in the milliseconds Elasticsearch stores a `date` in -- ONE formula
    * per rule for every venue, which differ only in how an operand becomes a number of milliseconds
    * and how a temporal result is handed back.
    *
    *   - Per group (`bucket_script`, `bucket_selector`, a view's per-group calculation), which is
    *     every rendering without a context, an aggregate is the metric Elasticsearch computed: a
    *     number, epoch millis for a date. A temporal result is handed back as epoch millis too,
    *     which is what a date aggregate is there and what a `bucket_selector` compares a date with.
    *   - Per document, a column is the `date` value Elasticsearch hands the script (`toInstant()`
    *     exists on the `JodaCompatibleZonedDateTime` of 6.8 as on the `ZonedDateTime` of 7 and up),
    *     or, in an ingest processor, the raw value of the document, parsed as a CAST parses it.
    *     Anything else is read by its RUNTIME type (`DateDiff.utcInstant`): `CURRENT_DATE` is a
    *     `LocalDate` in a query and the current instant in a processor. A temporal result is the
    *     UTC `ZonedDateTime` it denotes -- the one temporal an ingest processor may assign to a
    *     field.
    *
    * A DATE operand is its UTC calendar day: the days between two dates are the days between their
    * days, and a date moved by days starts at the start of its day. A NULL operand makes the result
    * NULL, behind the same guard every arithmetic emits.
    */
  private def dateArithmeticPainless(
    shape: ArithmeticExpression.Computed,
    idx: Int,
    context: Option[PainlessContext]
  ): String = {
    import ArithmeticExpression.{
      isBareName,
      DayMillis,
      DaysBetween,
      DaysShift,
      FractionalDayMillis,
      FractionalDaysBetween
    }

    var declarations = ""

    def grouped(e: String): String = if (group) s"($e)" else e

    // the operand as it renders -- no coercion: the conversion below starts from what it IS
    // A string literal that is a number counts as that number (`d + '1'`), so it renders as one.
    def rendered(operand: PainlessScript): String =
      (operand match {
        case i: Identifier if i.functions.isEmpty => context.flatMap(_.get(i))
        case _ => ArithmeticExpression.numericString(operand).map(_._1)
      }).getOrElse {
        val r = operand match {
          case t: TransformFunction[_, _] => t.toPainless("", idx + 1, context)
          case _                          => operand.painless(context)
        }
        if (PainlessOperandForm.placeable(r)) r else context.flatMap(_.get(operand)).getOrElse(r)
      }

    // An operand that can be NULL when it runs: one whose own nullability says so, or one that
    // READS a document field through a function. The nameless identifier around a `CASE`, a
    // `COALESCE` or a `NULLIF` answers `nullable = false` whatever its branches hold, so it went
    // unguarded and a NULL branch failed the script on `.toInstant()` instead of answering NULL.
    // (An AGGREGATE is the metric Elasticsearch computed: a group with no value never runs the
    // per-group script -- the `bucket_script`'s gap policy skips it -- so it keeps its own answer.)
    def mayBeNull(operand: PainlessScript): Boolean =
      operand.nullable || (operand match {
        case i: Identifier if !i.isAggregation =>
          i.dependencies.nonEmpty || i.functions.exists(_.nullable)
        case _ => false
      })

    // A string literal read as the temporal it spells beside the other operand (`d -
    // '2024-01-31'`): the instant it denotes, known here, so it is a constant -- never NULL, never
    // parsed per document (and read once per expression, `literalTemporal`).
    def literalInstant(operand: PainlessScript): Option[Long] =
      literalTemporal(operand).map(_.epochMillis)

    // a nullable operand the guard reads is evaluated once, under a name
    def named(operand: PainlessScript, fallback: String): String =
      literalInstant(operand).map(millis => s"${millis}L").getOrElse {
        val r = rendered(operand)
        if (!mayBeNull(operand) || isBareName(r)) r
        else
          context match {
            case Some(ctx) => ctx.bindLocal(r, "lv")
            case None =>
              declarations += s"def $fallback = $r; "
              fallback
          }
      }

    // Per group there is no document: every operand is a number already, and so is the result.
    val perGroup = context.isEmpty

    // the UTC day of a temporal operand, and the instant it denotes -- a DATE its day's start --
    // in epoch milliseconds: the conversion GREATEST / LEAST over dates shares
    def day(operand: PainlessScript, ref: String): String =
      literalInstant(operand)
        .map(millis => s"${Math.floorDiv(millis, 86400000L)}L")
        .getOrElse(ArithmeticExpression.epochDay(operand, ref, context))

    def instant(operand: PainlessScript, ref: String): String =
      literalInstant(operand)
        .map(millis => s"${millis}L")
        .getOrElse(ArithmeticExpression.instantMillis(operand, ref, context))

    def days(ref: String, fractional: Boolean): String =
      if (fractional) s"Math.round($ref * $FractionalDayMillis)" else s"$ref * $DayMillis"

    // Per document, a whole number of days or of milliseconds is a `long` again; per group it stays
    // the double a `bucket_script` returns anyway, and a `bucket_selector` compares as it is.
    def integral(e: String): String = if (perGroup) e else s"((long) $e)"

    val l = named(left, s"lv$idx")
    val r = named(right, s"rv$idx")

    val value = shape match {
      case DaysBetween => integral(s"(${day(left, l)} - ${day(right, r)})")
      case FractionalDaysBetween =>
        s"((${instant(left, l)} - ${instant(right, r)}) / $FractionalDayMillis)"
      case DaysShift(temporalLeft, _, fractional) =>
        val ((temporal, t), n) = if (temporalLeft) ((left, l), r) else ((right, r), l)
        val moved =
          s"(${instant(temporal, t)} ${operator.painless(context)} ${days(n, fractional)})"
        if (perGroup) moved else s"Instant.ofEpochMilli(${integral(moved)}).atZone(ZoneId.of('Z'))"
    }

    // (a literal instant is a primitive constant: never NULL, and `<long> == null` does not compile)
    val guards =
      (if (mayBeNull(left) && literalInstant(left).isEmpty) List(s"$l == null") else Nil) :::
      (if (mayBeNull(right) && literalInstant(right).isEmpty) List(s"$r == null") else Nil)
    val body =
      if (guards.isEmpty) value else s"(${guards.mkString(" || ")}) ? null : (def)($value)"
    s"$declarations${grouped(body)}"
  }

  private def numericPainless(context: Option[PainlessContext]): String = {
    val target = out
    // 🔴 An aggregate is coerced FROM what it computes (`AggregateFunction.outputTypeOf`), never
    // from the column it aggregates: per group it is the metric Elasticsearch already computed, a
    // number, and `COUNT(d) - 1` used to read the DATE column's type and emit
    // `params.count_d.toInstant().toEpochMilli() - 1`.
    def coerced(operand: PainlessScript): String = operand match {
      case metric: Identifier if metric.isAggregation =>
        SQLTypeUtils.coerce(
          metric.painless(context),
          ArithmeticExpression.identifierType(metric),
          target,
          nullable = false,
          context
        )
      case literal if literalNumber(literal).isDefined =>
        val number = literalNumber(literal).get
        SQLTypeUtils.coerce(number.painless, number.sqlType, target, nullable = false, context)
      case _ => SQLTypeUtils.coerce(operand, target, context)
    }
    val l = coerced(left)
    val r = coerced(right)
    val core = s"$l ${operator.painless(context)} $r"
    /* The non-nullable rendering: both operands are literals or system values, so each is coerced
     * to `out` and a DOUBLE `out` is all it takes to make `SELECT 10 / 3` answer 3.333 instead of
     * 3 -- no cast of our own is needed here. The zero guard still is: `SELECT 10 / 0` would
     * otherwise be `Infinity`. It is parenthesised so it can be spliced anywhere an operand goes,
     * and the live branch is `(def)`-cast because Painless types `cond ? null : <primitive>` as
     * `Object`: without it `SELECT 10 / 0` answered `class_cast_exception: Cannot cast from
     * [double] to [java.lang.Object]` instead of NULL -- MEASURED on 8.18.3, and the same trap
     * story 21.8 hit with a primitive method outside a null guard.
     */
    val expr =
      if (floatingDivision(target) && divisorIsLiteralZero) "null"
      else if (floatingDivision(target) && divisorMayBeZero) s"(($r == 0) ? null : (def)($core))"
      else core
    if (group)
      s"($expr)"
    else
      expr
  }

  override def update(request: query.SingleSearch): ArithmeticExpression = {
    (left, right) match {
      case (l: Updateable, r: Updateable) =>
        this.copy(
          left = l.update(request).asInstanceOf[PainlessScript],
          right = r.update(request).asInstanceOf[PainlessScript]
        )
      case (l: Updateable, _) =>
        this.copy(
          left = l.update(request).asInstanceOf[PainlessScript]
        )
      case (_, r: Updateable) =>
        this.copy(
          right = r.update(request).asInstanceOf[PainlessScript]
        )
      case _ =>
        this
    }
  }

  override def functionNestedElement: Option[NestedElement] =
    (left, right) match {
      case (l: Identifier, r: Identifier) =>
        l.nestedElement.orElse(r.nestedElement)
      case (l: Identifier, _) =>
        l.nestedElement
      case (_, r: Identifier) =>
        r.nestedElement
      case _ =>
        None
    }
}

/** The date-arithmetic rules: what `+` and `-` mean when an operand is a DATE, a TIMESTAMP or a
  * DATETIME, and which combinations are type errors.
  *
  * | Expression                                       | Result                                     |
  * |:-------------------------------------------------|:-------------------------------------------|
  * | DATE - DATE                                      | the calendar days between the two (BIGINT) |
  * | TIMESTAMP - TIMESTAMP, or a DATE and a TIMESTAMP | the milliseconds between, in days (DOUBLE) |
  * | DATE +/- a whole number n                        | the date n days later / earlier (DATE)     |
  * | DATE +/- a fractional number n                   | n days later / earlier, as an instant      |
  * | TIMESTAMP +/- a number n                         | n days later / earlier (TIMESTAMP)         |
  * | n + a temporal                                   | the temporal + n                           |
  * | a NULL literal beside a temporal                 | NULL                                       |
  * | two temporals added; a temporal under * / or %;  | a type error, refused by name before       |
  * | n - a temporal; a temporal and a non-number      | anything runs                              |
  *
  * They are the lead's ruling of 2026-10-04, aligned on the SQL engines a JDBC client knows:
  * PostgreSQL, DuckDB, Oracle and Snowflake subtract two dates into days; PostgreSQL, DuckDB,
  * Oracle and BigQuery move a date by a number of days; Oracle, whose DATE carries a time of day,
  * answers fractional days, and moves a date by a fractional number of days into a time of day;
  * PostgreSQL, DuckDB, Oracle, Trino, Snowflake and SQL Server refuse the other combinations.
  *
  * A temporal operand is recognised by its DECLARED type (`Token.reportedType`): a TIME, and an
  * operand whose type is unknown (a column with no schema attached), leave the expression to the
  * arithmetic it always was -- except under `*`, `/` and `%`, which refuse a temporal operand
  * whatever the other one is.
  */
object ArithmeticExpression {

  /** One day, in the milliseconds Elasticsearch stores a `date` in. */
  private val DayMillis = "86400000L"

  /** The same, as a floating-point number, for a fraction of a day. */
  private val FractionalDayMillis = "86400000.0"

  /** What the date-arithmetic rules make of an expression. */
  private[sql] sealed trait DateArithmetic {

    /** The type the rendering produces (`Token.baseType`): a temporal result is rendered as the
      * instant it denotes, so it is a TIMESTAMP whether it is reported as a DATE or not -- the same
      * collapse `SQLTypeUtils.runtimeType` applies to a DATE column.
      */
    def runtimeType: Option[SQLType] = None

    /** The type a consumer declaring a column reports (`Token.reportedType`). */
    def reportedType: Option[SQLType] = runtimeType
  }

  /** A value the date-arithmetic rendering computes -- as opposed to a NULL, or a refusal. */
  private[sql] sealed trait Computed extends DateArithmetic

  /** DATE - DATE: the calendar days between the UTC days of the two dates. */
  private[sql] case object DaysBetween extends Computed {
    override def runtimeType: Option[SQLType] = Some(SQLTypes.BigInt)
  }

  /** A difference with an instant on one side at least: the milliseconds between, in days. */
  private[sql] case object FractionalDaysBetween extends Computed {
    override def runtimeType: Option[SQLType] = Some(SQLTypes.Double)
  }

  /** A temporal moved by a number of days, the number on either side of a `+`.
    *
    * @param temporalLeft
    *   whether the temporal is the LEFT operand
    * @param calendarDate
    *   whether the temporal is a DATE, which is moved from the start of its day
    * @param fractional
    *   whether the number of days has a fractional type, whose fraction becomes a time of day
    */
  private[sql] final case class DaysShift(
    temporalLeft: Boolean,
    calendarDate: Boolean,
    fractional: Boolean
  ) extends Computed {
    override def runtimeType: Option[SQLType] = Some(SQLTypes.Timestamp)
    override def reportedType: Option[SQLType] =
      Some(if (calendarDate && !fractional) SQLTypes.Date else SQLTypes.Timestamp)
  }

  /** A NULL literal beside a temporal: NULL, with the type the expression derives otherwise. */
  private[sql] case object NullOperand extends DateArithmetic

  /** A combination no rule allows. */
  private[sql] final case class Refused(message: String) extends DateArithmetic

  /** One operand, as the rules see it. */
  private sealed trait Operand
  private case object CalendarDate extends Operand
  private case object Moment extends Operand
  private final case class Days(fractional: Boolean) extends Operand
  private case object NullLiteral extends Operand
  private case object Undecided extends Operand
  private case object NonNumber extends Operand

  private def temporal(operand: Operand): Boolean = operand == CalendarDate || operand == Moment

  /** The type an IDENTIFIER operand of an arithmetic computes: an aggregate is the metric it
    * produces ([[AggregateFunction.outputTypeOf]]: `COUNT(d)` counts, whatever `d` is), anything
    * else is what its chain renders (#382's `chainType`).
    *
    * 🔴 The ONE derivation the arithmetic asks, for its own type and for the coercion of each
    * operand alike (#292): reading the aggregated COLUMN's type typed `COUNT(d) - 1` as a TIMESTAMP
    * and emitted `params.count_d.toInstant()` on the count.
    */
  private[sql] def identifierType(i: Identifier): SQLType = renderedTypeOf(i)

  /** The type an operand is DECLARED with -- its REPORTED type, an aggregate's being the metric it
    * produces: the ONE derivation the comparison rule reads too (`ComparisonRule.declaredTypeOf`).
    *
    * An Elasticsearch `date` field elasticsql never declared is a TIMESTAMP at the declaration seam
    * itself (`schema.IndexField`), so it keeps its time here as everywhere else, and every chain
    * over it follows: `MAX(ts)`, `COALESCE(ts, ts2)`, `CASE … THEN ts …`.
    */
  private def declaredType(operand: PainlessScript): SQLType =
    ComparisonRule.declaredTypeOf(operand)

  /** `NULL`, bare or cast: its VALUE is NULL in every venue. */
  private[sql] def nullLiteral(operand: PainlessScript): Boolean = operand match {
    case Null          => true
    case i: Identifier => i.name.trim.isEmpty && i.functions.lastOption.contains(Null)
    case _             => false
  }

  /** A BARE `NULL`, which has no type of its own. A cast gives one -- `CAST(NULL AS DATE)` is a
    * DATE, `CAST(NULL AS INT)` a whole number -- and a typed NULL is then judged by its type,
    * whatever its value.
    */
  private def bareNull(operand: PainlessScript): Boolean = operand match {
    case Null          => true
    case i: Identifier => i.name.trim.isEmpty && i.functions == List(Null)
    case _             => false
  }

  /** A string LITERAL whose text is a number, as the Painless literal of that number and whether it
    * is fractional (the lead's ruling of 2026-10-05) -- the ONE reader the comparison rule uses too
    * (`ComparisonRule.numberOf`).
    *
    * Beside a DATE or a TIMESTAMP it is a number of days -- elasticsql's OWN rule, not
    * PostgreSQL's: `d + '1'` is the next day and `d - '1.5'` a day and a half earlier, where
    * PostgreSQL 16 refuses `d + '1'` (`operator is not unique: date + unknown`), reads the `'1'` of
    * `d - '1'` as a date (`invalid input syntax for type date`) and reads `ts + '1'` as one second.
    * A whole number is a `long` literal (`'99999999999'` does not fit an `int`), a fractional one
    * the `double` it denotes.
    */
  private[math] def numericString(operand: PainlessScript): Option[(String, Boolean)] =
    ComparisonRule
      .stringLiteral(operand)
      .flatMap(ComparisonRule.numberOf)
      .map(number => number.painless -> number.fractional)

  /** A string LITERAL read as the temporal it spells beside the temporal `other` -- the comparison
    * rule's reading (`ComparisonRule.readingOf`), so `d - '2024-01-31'` is DATE minus DATE, as
    * PostgreSQL reads it, and `ts - '2024-01-31T10:00:00Z'` TIMESTAMP minus TIMESTAMP. Beside
    * anything else, or spelling no date, the literal is no temporal.
    */
  private[math] def temporalString(
    operand: PainlessScript,
    other: PainlessScript
  ): Option[ComparisonRule.TemporalReading] =
    ComparisonRule.stringLiteral(operand).flatMap { _ =>
      operandOf(other) match {
        case CalendarDate | Moment =>
          ComparisonRule.readingOf(operand, declaredType(other)).collect {
            case t: ComparisonRule.TemporalReading => t
          }
        case _ => None
      }
    }

  private def operandOf(o: PainlessScript): Operand =
    if (bareNull(o)) NullLiteral
    else
      numericString(o) match {
        case Some((_, fractional)) => Days(fractional)
        case None =>
          declaredType(o) match {
            case SQLTypes.Date                                              => CalendarDate
            case SQLTypes.Timestamp | SQLTypes.DateTime | SQLTypes.Temporal => Moment
            case SQLTypes.TinyInt | SQLTypes.SmallInt | SQLTypes.Int | SQLTypes.BigInt =>
              Days(fractional = false)
            case SQLTypes.Double | SQLTypes.Real | SQLTypes.Numeric => Days(fractional = true)
            // a TIME is outside these rules, and an unknown type cannot be judged
            case t if t == SQLTypes.Time || t.isUnknown => Undecided
            case _                                      => NonNumber
          }
      }

  /** One operand beside the other: a string literal that spells a date or a timestamp beside a
    * temporal is that temporal ([[temporalString]]); anything else is what it is alone.
    */
  private def operandOf(o: PainlessScript, other: PainlessScript): Operand =
    temporalString(o, other) match {
      case Some(t) if t.sqlType == SQLTypes.Date => CalendarDate
      case Some(_)                               => Moment
      case None                                  => operandOf(o)
    }

  private[math] def isCalendarDate(o: PainlessScript): Boolean = operandOf(o) == CalendarDate

  private[sql] def isBareName(e: String): Boolean =
    e.nonEmpty && e.forall(c => c.isLetterOrDigit || c == '_')

  /** A TEMPORAL operand as the epoch milliseconds it denotes, in the venue `context` renders for --
    * the ONE conversion date arithmetic and `GREATEST` / `LEAST` over dates share (#292):
    *
    *   - per group (`bucket_script`, `bucket_selector`: no context) an aggregate is the metric
    *     Elasticsearch computed, a number -- in a view's per-group calculation too -- and so is a
    *     value this module renders from metrics there: a date moved by days, `GREATEST` / `LEAST`
    *     over dates;
    *   - per document, a column is the `date` value Elasticsearch hands the script (`toInstant()`
    *     exists on the `JodaCompatibleZonedDateTime` of 6.8 as on the `ZonedDateTime` of 7 and up),
    *     or, in an ingest processor, whatever the incoming document holds -- the raw JSON string or
    *     number of a column, or what a function made of it -- read by its RUNTIME class
    *     (`SQLTypeUtils.processorInstant`), never by the type it is declared with;
    *   - anything else is read by its RUNTIME type (`DateDiff.utcInstant`): `CURRENT_DATE` is a
    *     `LocalDate` in a query and the current instant in a processor.
    *
    * `ref` is the operand's rendering: a name whenever the operand can be NULL (the caller guards
    * it).
    */
  private[sql] def epochMillis(
    operand: PainlessScript,
    ref: String,
    context: Option[PainlessContext]
  ): String = {
    val perGroup = context.isEmpty
    val processor = context.exists(_.isProcessor)
    operand match {
      case metric: Identifier
          if metric.isAggregation && (perGroup || context.exists(_.isTransform)) =>
        ref
      case nested: ArithmeticExpression
          if nested.dateArithmetic.exists(_.isInstanceOf[DaysShift]) =>
        if (perGroup) ref else s"$ref.toInstant().toEpochMilli()"
      case reducer
          if (perGroup || context.exists(_.isTransform)) && NumericReducer.overDates(reducer) =>
        ref
      case column: Identifier if column.name.trim.nonEmpty && column.functions.isEmpty =>
        if (processor) s"${SQLTypeUtils.processorInstant(ref)}.toInstant().toEpochMilli()"
        else s"$ref.toInstant().toEpochMilli()"
      case other =>
        context match {
          case None =>
            s"${SQLTypeUtils.coerce(ref, other.baseType, SQLTypes.Timestamp, nullable = false, None)}.toInstant().toEpochMilli()"
          case Some(ctx) =>
            val name =
              if (isBareName(ref)) ref
              else ctx.addParam(LiteralParam(ref)).getOrElse(s"((def) ($ref))")
            if (processor) s"${SQLTypeUtils.processorInstant(name)}.toInstant().toEpochMilli()"
            else s"${DateDiff.utcInstant(name)}.toInstant().toEpochMilli()"
        }
    }
  }

  /** The UTC day a temporal operand falls in, in days since the epoch. `Math.floor` is the one
    * floor Painless allows -- `Math.floorDiv` is on no major's allow-list -- and it is exact here:
    * a date's milliseconds are far below 2^53, and so is every quotient's distance to an integer.
    */
  private[sql] def epochDay(
    operand: PainlessScript,
    ref: String,
    context: Option[PainlessContext]
  ): String =
    s"Math.floor(${epochMillis(operand, ref, context)} / $FractionalDayMillis)"

  /** The instant a temporal operand denotes, in epoch milliseconds: a DATE is the instant its UTC
    * day starts at, so two dates compare -- and move -- by their days.
    */
  private[sql] def instantMillis(
    operand: PainlessScript,
    ref: String,
    context: Option[PainlessContext]
  ): String =
    if (isCalendarDate(operand)) s"${epochDay(operand, ref, context)} * $FractionalDayMillis"
    else epochMillis(operand, ref, context)

  /** A temporal operand of a COMPARISON -- a simple `CASE`, `NULLIF` -- as the instant it denotes:
    * [[instantMillis]], the conversion date arithmetic and `GREATEST` / `LEAST` share, so a DATE is
    * the instant its UTC day starts at and equals a TIMESTAMP exactly at midnight, as SQL engines
    * promote it. A string literal the comparison READS as a temporal (`ComparisonRule.readingOf`)
    * is its instant, computed here rather than parsed per document.
    */
  private[sql] def comparedMillis(
    operand: PainlessScript,
    reading: Option[ComparisonRule.Reading],
    ref: String,
    context: Option[PainlessContext]
  ): String =
    reading match {
      case Some(t: ComparisonRule.TemporalReading) => s"${t.epochMillis}L"
      case _                                       => instantMillis(operand, ref, context)
    }

  private[math] def dateArithmetic(e: ArithmeticExpression): Option[DateArithmetic] = {
    val l = operandOf(e.left, e.right)
    val r = operandOf(e.right, e.left)
    if (!temporal(l) && !temporal(r)) None
    else
      e.operator match {
        case MULTIPLY | DIVIDE | MODULO => Some(refused(e))
        case ADD =>
          (l, r) match {
            case (t, Days(fractional)) if temporal(t) =>
              Some(DaysShift(temporalLeft = true, t == CalendarDate, fractional))
            case (Days(fractional), t) if temporal(t) =>
              Some(DaysShift(temporalLeft = false, t == CalendarDate, fractional))
            case (NullLiteral, _) | (_, NullLiteral) => Some(NullOperand)
            case (Undecided, _) | (_, Undecided)     => None
            case _                                   => Some(refused(e))
          }
        case SUBTRACT =>
          (l, r) match {
            case (CalendarDate, CalendarDate)         => Some(DaysBetween)
            case (a, b) if temporal(a) && temporal(b) => Some(FractionalDaysBetween)
            case (t, Days(fractional)) if temporal(t) =>
              Some(DaysShift(temporalLeft = true, t == CalendarDate, fractional))
            case (NullLiteral, _) | (_, NullLiteral) => Some(NullOperand)
            case (Undecided, _) | (_, Undecided)     => None
            case _                                   => Some(refused(e))
          }
      }
  }

  /** The type a refusal names an operand by: the one DESCRIBE shows for it. */
  private def typeName(o: PainlessScript): String =
    if (bareNull(o)) SQLTypes.Null.typeId else declaredType(o).typeId

  private def refused(e: ArithmeticExpression): Refused =
    Refused(
      s"Type mismatch: operator ${e.operator.sql} cannot be applied to ${typeName(e.left)} and " +
      s"${typeName(e.right)} in expression: ${e.sql.trim}; subtract two dates for the days between " +
      "them, or add or subtract a number of days"
    )

  /** Every date-arithmetic type error in an expression, wherever an arithmetic sits in it.
    *
    * 🔴 Asked of a SCHEMA-RESOLVED expression, as `NullIf.mismatchesOf` is: a computed column's
    * resolved table (`schema.validateScriptReferences`) -- a statement asks every type rule at once
    * (`SingleSearch.typeErrors`). Before a schema is attached a column's type is unknown, which
    * these rules accept. `functionsOf` is the walk every such rule shares; an arithmetic nested
    * directly in another is not a chain, so each arithmetic walks its own operands
    * ([[ArithmeticExpression.dateArithmeticErrors]]).
    */
  def typeErrorsOf(chain: FunctionChain): Seq[String] =
    functionsOf(chain)
      .collect { case a: ArithmeticExpression => a }
      .flatMap(_.dateArithmeticErrors)
      .distinct
}
