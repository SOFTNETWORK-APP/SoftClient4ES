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
  Degrees,
  Exp,
  Floor,
  Log,
  Log10,
  MathOp,
  MathematicalFunction,
  MathematicalFunctionWithOp,
  Pow,
  Radians,
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

    private[this] lazy val abs: PackratParser[MathOp] = Abs.regex ^^ (_ => Abs)

    private[this] lazy val ceil: PackratParser[MathOp] = Ceil.regex ^^ (_ => Ceil)

    private[this] lazy val floor: PackratParser[MathOp] = Floor.regex ^^ (_ => Floor)

    private[this] lazy val exp: PackratParser[MathOp] = Exp.regex ^^ (_ => Exp)

    private[this] lazy val sqrt: PackratParser[MathOp] = Sqrt.regex ^^ (_ => Sqrt)

    private[this] lazy val log: PackratParser[MathOp] = Log.regex ^^ (_ => Log)

    private[this] lazy val log10: PackratParser[MathOp] = Log10.regex ^^ (_ => Log10)

    lazy val arithmetic_function: PackratParser[MathematicalFunction] =
      (abs | ceil | exp | floor | log | log10 | sqrt) ~ start ~ valueExpr ~ end ^^ {
        case op ~ _ ~ v ~ _ => MathematicalFunctionWithOp(op, v)
      }

    private[this] lazy val sin: PackratParser[Trigonometric] = Sin.regex ^^ (_ => Sin)

    private[this] lazy val asin: PackratParser[Trigonometric] = Asin.regex ^^ (_ => Asin)

    private[this] lazy val cos: PackratParser[Trigonometric] = Cos.regex ^^ (_ => Cos)

    private[this] lazy val acos: PackratParser[Trigonometric] = Acos.regex ^^ (_ => Acos)

    private[this] lazy val tan: PackratParser[Trigonometric] = Tan.regex ^^ (_ => Tan)

    private[this] lazy val atan: PackratParser[Trigonometric] = Atan.regex ^^ (_ => Atan)

    private[this] lazy val atan2: PackratParser[Trigonometric] = Atan2.regex ^^ (_ => Atan2)

    private[this] lazy val degrees: PackratParser[Trigonometric] = Degrees.regex ^^ (_ => Degrees)

    private[this] lazy val radians: PackratParser[Trigonometric] = Radians.regex ^^ (_ => Radians)

    lazy val atan2_function: PackratParser[MathematicalFunction] =
      atan2 ~ start ~ (double | valueExpr) ~ separator ~ (double | valueExpr) ~ end ^^ {
        case _ ~ _ ~ y ~ _ ~ x ~ _ => Atan2(y, x)
      }

    lazy val trigonometric_function: PackratParser[MathematicalFunction] =
      atan2_function | ((sin | asin | cos | acos | tan | atan | degrees | radians) ~ start ~ valueExpr ~ end ^^ {
        case op ~ _ ~ v ~ _ => MathematicalFunctionWithOp(op, v)
      })

    private[this] lazy val round: PackratParser[MathOp] = Round.regex ^^ (_ => Round)

    lazy val round_function: PackratParser[MathematicalFunction] =
      round ~ start ~ valueExpr ~ separator.? ~ long.? ~ end ^^ { case _ ~ _ ~ v ~ _ ~ s ~ _ =>
        Round(v, s.map(_.value.toInt))
      }

    private[this] lazy val pow: PackratParser[MathOp] = Pow.regex ^^ (_ => Pow)

    lazy val pow_function: PackratParser[MathematicalFunction] =
      pow ~ start ~ valueExpr ~ separator ~ long ~ end ^^ { case _ ~ _ ~ v1 ~ _ ~ e ~ _ =>
        Pow(v1, e.value.toInt)
      }

    private[this] lazy val sign: PackratParser[MathOp] = Sign.regex ^^ (_ => Sign)

    lazy val sign_function: PackratParser[MathematicalFunction] =
      sign ~ start ~ valueExpr ~ end ^^ { case _ ~ _ ~ v ~ _ => Sign(v) }

    lazy val mathematical_function: PackratParser[MathematicalFunction] =
      arithmetic_function | trigonometric_function | round_function | pow_function | sign_function

    lazy val mathematicalFunctionWithIdentifier: PackratParser[Identifier] =
      mathematical_function ^^ functionAsIdentifier

  }
}
