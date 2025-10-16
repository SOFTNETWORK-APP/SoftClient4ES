package app.softnetwork.elastic.sql.function

import app.softnetwork.elastic.sql.{
  Expr,
  Identifier,
  IntValue,
  PainlessContext,
  PainlessParam,
  PainlessScript,
  TokenRegex
}
import app.softnetwork.elastic.sql.`type`.{SQLNumeric, SQLType, SQLTypes}

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

  sealed trait MathematicalFunction
      extends TransformFunction[SQLNumeric, SQLNumeric]
      with FunctionWithIdentifier {
    override def inputType: SQLNumeric = SQLTypes.Numeric

    override def outputType: SQLNumeric = mathOp.baseType

    def mathOp: MathOp

    override def fun: Option[PainlessScript] = Some(mathOp)

    override def identifier: Identifier = Identifier(this)

  }

  case class MathematicalFunctionWithOp(
    mathOp: MathOp,
    arg: PainlessScript
  ) extends MathematicalFunction {
    override def args: List[PainlessScript] = List(arg)
  }

  case class Pow(arg: PainlessScript, exponent: Int) extends MathematicalFunction {
    override def mathOp: MathOp = Pow
    override def args: List[PainlessScript] = List(arg, IntValue(exponent))
    override def nullable: Boolean = arg.nullable
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
        case List(a, p) => s"${mathOp.painless(context)}(($a * $p) / $p)"
        case _ => throw new IllegalArgumentException("Round function requires exactly one argument")
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

  }

  case class Atan2(y: PainlessScript, x: PainlessScript) extends MathematicalFunction {
    override def mathOp: MathOp = Atan2
    override def args: List[PainlessScript] = List(y, x)
    override def nullable: Boolean = y.nullable || x.nullable
  }

}
