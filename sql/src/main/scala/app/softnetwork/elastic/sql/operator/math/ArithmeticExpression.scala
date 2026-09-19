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

  override def baseType: SQLType = SQLTypeUtils.leastCommonSuperType(argTypes)

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
      // 🔴 The operand is RENDERED, never read back as a parameter NAME (issue #373, item 5).
      // `ctx.get(left)` answers with the name of the parameter the identifier registered -- the
      // RAW doc-value -- so the function chain never reached the expression and
      // `YEAR(d) * 100 + MONTH(d)` computed `d * 100 + d`. It is #367's defect one layer up: the
      // shortcut is sound only where the parameter IS the whole operand, which is exactly what a
      // bare-name rendering means. MEASURED on the real DDL path, an ingest processor stored
      // `ctx.c = (param1 == null || param1 == null) ? null : (param1 + param1)` with
      // `param1 = ctx.d` -- the duplicated null guard is the fingerprint of the collapse.
      // 🔴 Coerced FROM what the operand RENDERS, not from its column's type (#367's rule, which
      // this site never applied): `YEAR(d)` renders an `int`, and reading `baseType` asked for a
      // TIMESTAMP -> BIGINT conversion that emitted `.toInstant()` on that `int`.
      def renderedType(operand: PainlessScript): SQLType = operand match {
        case i: Identifier => i.chainType
        case other         => other.baseType
      }

      /** 🔴 A numeric coercion is skipped only over a NULLABLE operand, and the reason is exactly
        * what Painless refuses -- a primitive cast over a null-guarded expression, in BOTH
        * directions, MEASURED on ES 8.18:
        * {{{
        * ((double) (param1 != null ? … : null))  Cannot cast null to a primitive type [double].
        * ((long)   (param1 != null ? … : null))  Cannot cast null to a primitive type [long].
        * }}}
        * and the coercion TARGET is derived from the COLUMN (`out` folds the operands' `out`), so
        * `CAST(n AS DOUBLE) + 1` over an `INT` column asked DOUBLE -> BIGINT and emitted a cast
        * that both truncates and fails to compile.
        *
        * 🔴 It must NOT be skipped over a NON-nullable operand: a LITERAL carries no guard, and its
        * coercion is the only thing that makes the division floating-point. Skipping it turned
        * `CAST(n AS DOUBLE) / 2` into INTEGER division -- `[0.5, 2.5, 4.5]` became `[0, 2, 4]`,
        * HTTP 200, in script fields, `terms` keys, sort scripts AND predicates (a row silently
        * vanished from `WHERE … / 2 > 2`). Found by review, MEASURED on the schema-LESS rendering
        * path -- which production reaches whenever `resolveWithSchema` declines: a wildcard or
        * multi-index FROM, a JOIN, or a mapping that cannot be loaded. #205's silent-wrong-answer
        * family, and NULLABILITY is the discriminator that separates the two failures.
        */
      def coerced(rendered: String, operand: PainlessScript): String =
        if (
          operand.nullable && renderedType(operand).isInstanceOf[SQLNumeric] &&
          out.isInstanceOf[SQLNumeric]
        ) rendered
        else
          SQLTypeUtils.coerce(rendered, renderedType(operand), out, nullable = false, context)

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
      if (left.nullable && right.nullable)
        expr += s"($leftParam == null || $rightParam == null) ? null : ($leftParam ${operator
          .painless(context)} $rightParam)"
      else if (left.nullable)
        expr += s"($leftParam == null) ? null : ($leftParam ${operator.painless(context)} $r)"
      else
        expr += s"($rightParam == null) ? null : ($l ${operator.painless(context)} $rightParam)"
      if (group)
        expr = s"($expr)"
      return s"$base$expr"
    }
    s"$base${painless(context)}"
  }

  override def painless(context: Option[PainlessContext]): String = {
    val l = SQLTypeUtils.coerce(left, out, context)
    val r = SQLTypeUtils.coerce(right, out, context)
    val expr = s"$l ${operator.painless(context)} $r"
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
