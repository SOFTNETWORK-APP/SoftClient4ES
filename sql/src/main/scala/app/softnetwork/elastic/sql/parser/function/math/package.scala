package app.softnetwork.elastic.sql.parser.function

import app.softnetwork.elastic.sql.Identifier
import app.softnetwork.elastic.sql.function.math.{
  Abs,
  Acos,
  Asin,
  Atan,
  Atan2,
  Ceil,
  Cos,
  Exp,
  Floor,
  Log,
  Log10,
  MathOp,
  MathematicalFunction,
  MathematicalFunctionWithOp,
  Pow,
  Round,
  Sign,
  Sin,
  Sqrt,
  Tan,
  Trigonometric
}
import app.softnetwork.elastic.sql.parser.Parser

package object math {

  trait MathParser { self: Parser =>

    private[this] def abs: PackratParser[MathOp] = Abs.regex ^^ (_ => Abs)

    private[this] def ceil: PackratParser[MathOp] = Ceil.regex ^^ (_ => Ceil)

    private[this] def floor: PackratParser[MathOp] = Floor.regex ^^ (_ => Floor)

    private[this] def exp: PackratParser[MathOp] = Exp.regex ^^ (_ => Exp)

    private[this] def sqrt: PackratParser[MathOp] = Sqrt.regex ^^ (_ => Sqrt)

    private[this] def log: PackratParser[MathOp] = Log.regex ^^ (_ => Log)

    private[this] def log10: PackratParser[MathOp] = Log10.regex ^^ (_ => Log10)

    def arithmetic_function: PackratParser[MathematicalFunction] =
      (abs | ceil | exp | floor | log | log10 | sqrt) ~ start ~ valueExpr ~ end ^^ {
        case op ~ _ ~ v ~ _ => MathematicalFunctionWithOp(op, v)
      }

    private[this] def sin: PackratParser[Trigonometric] = Sin.regex ^^ (_ => Sin)

    private[this] def asin: PackratParser[Trigonometric] = Asin.regex ^^ (_ => Asin)

    private[this] def cos: PackratParser[Trigonometric] = Cos.regex ^^ (_ => Cos)

    private[this] def acos: PackratParser[Trigonometric] = Acos.regex ^^ (_ => Acos)

    private[this] def tan: PackratParser[Trigonometric] = Tan.regex ^^ (_ => Tan)

    private[this] def atan: PackratParser[Trigonometric] = Atan.regex ^^ (_ => Atan)

    private[this] def atan2: PackratParser[Trigonometric] = Atan2.regex ^^ (_ => Atan2)

    def atan2_function: PackratParser[MathematicalFunction] =
      atan2 ~ start ~ (double | valueExpr) ~ separator ~ (double | valueExpr) ~ end ^^ {
        case _ ~ _ ~ y ~ _ ~ x ~ _ => Atan2(y, x)
      }

    def trigonometric_function: PackratParser[MathematicalFunction] =
      atan2_function | ((sin | asin | cos | acos | tan | atan) ~ start ~ valueExpr ~ end ^^ {
        case op ~ _ ~ v ~ _ => MathematicalFunctionWithOp(op, v)
      })

    private[this] def round: PackratParser[MathOp] = Round.regex ^^ (_ => Round)

    def round_function: PackratParser[MathematicalFunction] =
      round ~ start ~ valueExpr ~ separator.? ~ long.? ~ end ^^ { case _ ~ _ ~ v ~ _ ~ s ~ _ =>
        Round(v, s.map(_.value.toInt))
      }

    private[this] def pow: PackratParser[MathOp] = Pow.regex ^^ (_ => Pow)

    def pow_function: PackratParser[MathematicalFunction] =
      pow ~ start ~ valueExpr ~ separator ~ long ~ end ^^ { case _ ~ _ ~ v1 ~ _ ~ e ~ _ =>
        Pow(v1, e.value.toInt)
      }

    private[this] def sign: PackratParser[MathOp] = Sign.regex ^^ (_ => Sign)

    def sign_function: PackratParser[MathematicalFunction] =
      sign ~ start ~ valueExpr ~ end ^^ { case _ ~ _ ~ v ~ _ => Sign(v) }

    def mathematical_function: PackratParser[MathematicalFunction] =
      arithmetic_function | trigonometric_function | round_function | pow_function | sign_function

    def mathematicalFunctionWithIdentifier: PackratParser[Identifier] =
      mathematical_function ^^ functionAsIdentifier

  }
}
