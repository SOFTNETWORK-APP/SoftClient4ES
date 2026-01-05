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
      val l = context
        .flatMap(ctx => ctx.get(left))
        .getOrElse(left match {
          case t: TransformFunction[_, _] =>
            SQLTypeUtils.coerce(
              t.toPainless("", idx + 1, context),
              left.baseType,
              out,
              nullable = false,
              context
            )
          case _ =>
            SQLTypeUtils
              .coerce(left.painless(context), left.baseType, out, nullable = false, context)
        })
      val r = context
        .flatMap(ctx => ctx.get(right))
        .getOrElse(right match {
          case t: TransformFunction[_, _] =>
            SQLTypeUtils.coerce(
              t.toPainless("", idx + 1, context),
              right.baseType,
              out,
              nullable = false,
              context
            )
          case _ =>
            SQLTypeUtils
              .coerce(right.painless(context), right.baseType, out, nullable = false, context)
        })
      var expr = ""
      val leftParam = context.flatMap(ctx => ctx.get(left)).getOrElse(s"lv$idx")
      val rightParam = context.flatMap(ctx => ctx.get(right)).getOrElse(s"rv$idx")
      if (left.nullable)
        expr += (if (context.exists(ctx => ctx.get(left).nonEmpty)) ""
                 else s"def $leftParam = $l; ")
      if (right.nullable)
        expr += (if (context.exists(ctx => ctx.get(right).nonEmpty)) ""
                 else s"def $rightParam = $r; ")
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
}
