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
  Expr,
  IntValue,
  PainlessContext,
  PainlessParam,
  PainlessScript,
  TokenRegex,
  Updateable
}
import app.softnetwork.elastic.sql.`type`.{
  SQLBigInt,
  SQLInt,
  SQLNumeric,
  SQLSmallInt,
  SQLTinyInt,
  SQLType,
  SQLTypes
}

package object math {

  sealed trait MathOp extends PainlessScript with TokenRegex {
    override def painless(context: Option[PainlessContext] = None): String =
      s"Math.${sql.toLowerCase()}"
    override def toString: String = s" $sql "

    override def baseType: SQLNumeric = SQLTypes.Numeric
  }

  case object Abs extends Expr("ABS") with MathOp
  case object Ceil extends Expr("CEIL") with MathOp {
    override def words: List[String] = List("CEILING", sql)
    override def baseType: SQLNumeric = SQLTypes.BigInt
  }
  case object Floor extends Expr("FLOOR") with MathOp {
    override def baseType: SQLNumeric = SQLTypes.BigInt
  }
  case object Round extends Expr("ROUND") with MathOp
  case object Exp extends Expr("EXP") with MathOp
  case object Log extends Expr("LOG") with MathOp
  case object Log10 extends Expr("LOG10") with MathOp
  case object Pow extends Expr("POW") with MathOp {
    override def words: List[String] = List("POWER", sql)
  }
  case object Sqrt extends Expr("SQRT") with MathOp
  case object Sign extends Expr("SIGN") with MathOp {
    override def baseType: SQLNumeric = SQLTypes.TinyInt
  }

  sealed trait Trigonometric extends MathOp {
    override def baseType: SQLNumeric = SQLTypes.Double
  }

  case object Sin extends Expr("SIN") with Trigonometric
  case object Asin extends Expr("ASIN") with Trigonometric
  case object Cos extends Expr("COS") with Trigonometric
  case object Acos extends Expr("ACOS") with Trigonometric
  case object Tan extends Expr("TAN") with Trigonometric
  case object Atan extends Expr("ATAN") with Trigonometric
  case object Atan2 extends Expr("ATAN2") with Trigonometric
  case object Degrees extends Expr("DEGREES") with Trigonometric {
    override def painless(context: Option[PainlessContext] = None): String = "Math.toDegrees"
  }
  case object Radians extends Expr("RADIANS") with Trigonometric {
    override def painless(context: Option[PainlessContext] = None): String = "Math.toRadians"
  }

  sealed trait MathematicalFunction extends TransformFunction[SQLNumeric, SQLNumeric] {
    override def inputType: SQLNumeric = SQLTypes.Numeric

    override def outputType: SQLNumeric = mathOp.baseType

    def mathOp: MathOp

    override def fun: Option[PainlessScript] = Some(mathOp)

    override def toPainlessCall(
      callArgs: List[String],
      context: Option[PainlessContext]
    ): String = {
      val ret = super.toPainlessCall(callArgs, context)
      s"Double.valueOf($ret)"
    }
  }

  case class MathematicalFunctionWithOp(
    mathOp: MathOp,
    arg: PainlessScript
  ) extends MathematicalFunction {
    override def args: List[PainlessScript] = List(arg)

    /** `ABS` is typed by its argument, as PostgreSQL's `abs(integer) -> integer`: `ABS(n)` over an
      * INT column is an INT, so a literal beside it is read as that type's value -- `ABS(n) =
      * '1.5'` is refused by name like `n = '1.5'`, where NUMERIC read `'1.5'` as a number -- and a
      * `CREATE TABLE … AS SELECT` declares an INT, where NUMERIC could not create the index. An
      * argument whose type is not a known number keeps NUMERIC.
      *
      * The type REPORTED, not the one Painless is handed (`out`, still NUMERIC): what a consumer
      * converts this value to is unchanged.
      */
    override def reportedType: SQLType = mathOp match {
      case Abs =>
        arg.reportedType match {
          case n: SQLNumeric => n
          case _             => super.reportedType
        }
      case _ => super.reportedType
    }

    /** `ABS` of a whole number answers a whole number of its argument's type per document, as its
      * type says -- Painless's `Math.abs` takes a `double` only, so it answered `3.0` for an INT
      * `-3`. The argument is a parameter or a literal, read at most twice. Per group -- no context,
      * or a view's per-group calculation -- every metric is the double Elasticsearch computed, and
      * the rendering is the one it always was.
      */
    override def toPainlessCall(
      callArgs: List[String],
      context: Option[PainlessContext]
    ): String =
      (mathOp, renderedTypeOf(arg), callArgs) match {
        case (Abs, _: SQLTinyInt | _: SQLSmallInt | _: SQLInt | _: SQLBigInt, List(a))
            if context.exists(!_.isTransform) =>
          val x = s"(${a.trim})"
          s"($x < 0 ? -$x : $x)"
        case _ => super.toPainlessCall(callArgs, context)
      }

    override def update(request: query.SingleSearch): MathematicalFunctionWithOp = {
      arg match {
        case updatable: Updateable =>
          this.copy(arg = updatable.update(request).asInstanceOf[PainlessScript])
        case _ => this
      }
    }
  }

  case class Pow(arg: PainlessScript, exponent: Int) extends MathematicalFunction {
    override def mathOp: MathOp = Pow
    override def args: List[PainlessScript] = List(arg, IntValue(exponent))
    override def nullable: Boolean = arg.nullable
    override def update(request: query.SingleSearch): Pow = {
      arg match {
        case updatable: Updateable =>
          this.copy(arg = updatable.update(request).asInstanceOf[PainlessScript])
        case _ => this
      }
    }
  }

  case class PowParam(scale: Int) extends PainlessParam with PainlessScript {
    override def param: String = s"Math.pow(10, $scale)"
    override def checkNotNull: String = ""
    override def sql: String = param
    override def nullable: Boolean = true

    /** Generate painless script for this token
      *
      * @param context
      *   the painless context
      * @return
      *   the painless script
      */
    override def painless(context: Option[PainlessContext]): String = param
  }

  case class Round(arg: PainlessScript, scale: Option[Int]) extends MathematicalFunction {
    override def mathOp: MathOp = Round

    override def args: List[PainlessScript] = List(arg, PowParam(scale.getOrElse(0)))

    override def toPainlessCall(callArgs: List[String], context: Option[PainlessContext]): String =
      callArgs match {
        case List(a, p) => s"Long.valueOf(${mathOp.painless(context)}(($a * $p) / $p))"
        case _ => throw new IllegalArgumentException("Round function requires exactly one argument")
      }

    override def sql: String = {
      s"${fun.map(_.sql).getOrElse("")}($arg${scale.map(s => s", $s").getOrElse("")})"
    }

    override def update(request: query.SingleSearch): Round = {
      arg match {
        case updatable: Updateable =>
          this.copy(arg = updatable.update(request).asInstanceOf[PainlessScript])
        case _ => this
      }
    }
  }

  case class Sign(arg: PainlessScript) extends MathematicalFunction {
    override def mathOp: MathOp = Sign

    override def args: List[PainlessScript] = List(arg)

    override def toPainlessCall(callArgs: List[String], context: Option[PainlessContext]): String =
      callArgs match {
        case List(a) => s"($a > 0 ? 1 : ($a < 0 ? -1 : 0))"
        case _ => throw new IllegalArgumentException("Sign function requires exactly one argument")
      }

    override def update(request: query.SingleSearch): Sign = {
      arg match {
        case updatable: Updateable =>
          this.copy(arg = updatable.update(request).asInstanceOf[PainlessScript])
        case _ => this
      }
    }
  }

  case class Atan2(y: PainlessScript, x: PainlessScript) extends MathematicalFunction {
    override def mathOp: MathOp = Atan2
    override def args: List[PainlessScript] = List(y, x)
    override def nullable: Boolean = y.nullable || x.nullable

    override def update(request: query.SingleSearch): Atan2 = {
      (y, x) match {
        case (updatableY: Updateable, updatableX: Updateable) =>
          this.copy(
            y = updatableY.update(request).asInstanceOf[PainlessScript],
            x = updatableX.update(request).asInstanceOf[PainlessScript]
          )
        case (updatableY: Updateable, _) =>
          this.copy(
            y = updatableY.update(request).asInstanceOf[PainlessScript]
          )
        case (_, updatableX: Updateable) =>
          this.copy(
            x = updatableX.update(request).asInstanceOf[PainlessScript]
          )
        case _ => this
      }
    }
  }

}
