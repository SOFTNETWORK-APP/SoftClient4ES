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
import app.softnetwork.elastic.sql.function.{BinaryFunction, TransformFunction}
import app.softnetwork.elastic.sql.parser.Validator
import app.softnetwork.elastic.sql.query.NestedElement

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

  override def applyType(in: SQLType): SQLType = in

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
  private def argTypeOf(operand: PainlessScript): SQLType = operand match {
    case i: Identifier => i.chainType
    case other         => other.out
  }

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
    * Restricted to a NUMERIC fold on purpose: `s / 2` over a KEYWORD column folds to VARCHAR and is
    * left exactly as it was, so this ruling widens nothing outside arithmetic.
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
  override def baseType: SQLType = {
    val folded = SQLTypeUtils.leastCommonSuperType(argTypes)
    if (operator == DIVIDE && folded.isInstanceOf[SQLNumeric]) SQLTypes.Double else folded
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
  private def renderedType(operand: PainlessScript): SQLType = operand match {
    case i: Identifier => i.chainType
    case other         => other.baseType
  }

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
    operand.nullable && renderedType(operand).isInstanceOf[SQLNumeric] &&
    target.isInstanceOf[SQLNumeric]

  private def isFloating(t: SQLType): Boolean = t == SQLTypes.Double || t == SQLTypes.Real

  /** What the operand will really be EMITTED as: its own rendering where the coercion is skipped,
    * and `out` where it is not (a non-nullable operand is always coerced to `out`).
    */
  private def emittedType(operand: PainlessScript, target: SQLType): SQLType =
    if (coercionSkipped(operand, target)) renderedType(operand) else target

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

  override def validate(): Either[String, Unit] = {
    for {
      _ <- left.validate()
      _ <- right.validate()
      _ <- Validator.validateTypesMatching(left.out, right.out)
    } yield ()
  }

  override def nullable: Boolean = left.nullable || right.nullable

  override def toPainless(base: String, idx: Int, context: Option[PainlessContext]): String = {
    context match {
      case Some(ctx) =>
        ctx.addParam(left)
        ctx.addParam(right)
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

      def render(operand: PainlessScript): String = operand match {
        case t: TransformFunction[_, _] => coerced(t.toPainless("", idx + 1, context), operand)
        case _                          => coerced(operand.painless(context), operand)
      }

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
      val rightParam =
        if (right.nullable && !isBareName(r)) bind(r, s"rv$idx") else r

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
        (if (floatingDivision(target) && divisorMayBeZero) List(s"$rightParam == 0") else Nil)
      expr += s"(${guards.mkString(" || ")}) ? null : ($lhs ${operator
        .painless(context)} $rightParam)"
      if (group)
        expr = s"($expr)"
      return s"$base$expr"
    }
    s"$base${painless(context)}"
  }

  override def painless(context: Option[PainlessContext]): String = {
    val target = out
    val l = SQLTypeUtils.coerce(left, target, context)
    val r = SQLTypeUtils.coerce(right, target, context)
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
      if (floatingDivision(target) && divisorMayBeZero) s"(($r == 0) ? null : (def)($core))"
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
