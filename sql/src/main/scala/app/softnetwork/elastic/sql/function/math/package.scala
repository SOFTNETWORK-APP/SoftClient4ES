package app.softnetwork.elastic.sql.function

import app.softnetwork.elastic.sql.{Expr, Identifier, IntValue, PainlessScript, TokenRegex}
import app.softnetwork.elastic.sql.`type`.{SQLNumeric, SQLType, SQLTypes}

package object math {

  sealed trait MathOp extends PainlessScript with TokenRegex {
    override def painless(): String = s"Math.${sql.toLowerCase()}"
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

  case class Round(arg: PainlessScript, scale: Option[Int]) extends MathematicalFunction {
    override def mathOp: MathOp = Round

    override def args: List[PainlessScript] =
      List(arg) ++ scale.map(IntValue(_)).toList

    override def toPainlessCall(callArgs: List[String]): String =
      s"(def p = ${Pow(IntValue(10), scale.getOrElse(0))
        .painless()}; ${mathOp.painless()}((${callArgs.head} * p) / p))"
  }

  case class Sign(arg: PainlessScript) extends MathematicalFunction {
    override def mathOp: MathOp = Sign

    override def args: List[PainlessScript] = List(arg)

    override def painless(): String = {
      val ret = "arg0 > 0 ? 1 : (arg0 < 0 ? -1 : 0)"
      if (arg.nullable)
        s"(def arg0 = ${arg.painless()}; arg0 != null ? ($ret) : null)"
      else
        s"(def arg0 = ${arg.painless()}; $ret)"
    }
  }

  case class Atan2(y: PainlessScript, x: PainlessScript) extends MathematicalFunction {
    override def mathOp: MathOp = Atan2
    override def args: List[PainlessScript] = List(y, x)
    override def nullable: Boolean = y.nullable || x.nullable
  }

}
