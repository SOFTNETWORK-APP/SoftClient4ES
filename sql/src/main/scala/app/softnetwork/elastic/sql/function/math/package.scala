package app.softnetwork.elastic.sql.function

import app.softnetwork.elastic.sql.{Expr, GenericIdentifier, IntValue, PainlessScript, TokenRegex}
import app.softnetwork.elastic.sql.`type`.{SQLNumeric, SQLTypes}

package object math {

  sealed trait MathOp extends PainlessScript with TokenRegex {
    override def painless: String = s"Math.${sql.toLowerCase()}"
    override def toString: String = s" $sql "
  }

  case object Abs extends Expr("abs") with MathOp
  case object Ceil extends Expr("ceil") with MathOp
  case object Floor extends Expr("floor") with MathOp
  case object Round extends Expr("round") with MathOp
  case object Exp extends Expr("exp") with MathOp
  case object Log extends Expr("log") with MathOp
  case object Log10 extends Expr("log10") with MathOp
  case object Pow extends Expr("pow") with MathOp
  case object Sqrt extends Expr("sqrt") with MathOp
  case object Sign extends Expr("sign") with MathOp
  case object Pi extends Expr("pi") with MathOp {
    override def painless: String = "Math.PI"
  }

  sealed trait Trigonometric extends MathOp

  case object Sin extends Expr("sin") with Trigonometric
  case object Asin extends Expr("asin") with Trigonometric
  case object Cos extends Expr("cos") with Trigonometric
  case object Acos extends Expr("acos") with Trigonometric
  case object Tan extends Expr("tan") with Trigonometric
  case object Atan extends Expr("atan") with Trigonometric
  case object Atan2 extends Expr("atan2") with Trigonometric

  sealed trait MathematicalFunction
      extends TransformFunction[SQLNumeric, SQLNumeric]
      with FunctionWithIdentifier {
    override def inputType: SQLNumeric = SQLTypes.Numeric

    override def outputType: SQLNumeric = SQLTypes.Double

    def mathOp: MathOp

    override def fun: Option[PainlessScript] = Some(mathOp)

    override def identifier: GenericIdentifier = GenericIdentifier("", functions = this :: Nil)

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
      s"(def p = ${Pow(IntValue(10), scale.getOrElse(0)).painless}; ${mathOp.painless}((${callArgs.head} * p) / p))"
  }

  case class Sign(arg: PainlessScript) extends MathematicalFunction {
    override def mathOp: MathOp = Sign

    override def args: List[PainlessScript] = List(arg)

    override def outputType: SQLNumeric = SQLTypes.Int

    override def painless: String = {
      val ret = "arg0 > 0 ? 1 : (arg0 < 0 ? -1 : 0)"
      if (arg.nullable)
        s"(def arg0 = ${arg.painless}; arg0 != null ? ($ret) : null)"
      else
        s"(def arg0 = ${arg.painless}; $ret)"
    }
  }

  case class Atan2(y: PainlessScript, x: PainlessScript) extends MathematicalFunction {
    override def mathOp: MathOp = Atan2
    override def args: List[PainlessScript] = List(y, x)
    override def nullable: Boolean = y.nullable || x.nullable
  }

}
