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

package app.softnetwork.elastic.sql.parser.operator

import app.softnetwork.elastic.sql.function.{Function, FunctionWithIdentifier}
import app.softnetwork.elastic.sql.{Identifier, PainlessScript}
import app.softnetwork.elastic.sql.operator.math.{
  ADD,
  ArithmeticExpression,
  ArithmeticOperator,
  DIVIDE,
  MODULO,
  MULTIPLY,
  SUBTRACT
}
import app.softnetwork.elastic.sql.parser.Parser

package object math {

  trait ArithmeticParser { self: Parser =>
    lazy val add: PackratParser[ArithmeticOperator] = ADD.sql ^^ (_ => ADD)

    lazy val subtract: PackratParser[ArithmeticOperator] = SUBTRACT.sql ^^ (_ => SUBTRACT)

    lazy val multiply: PackratParser[ArithmeticOperator] = MULTIPLY.sql ^^ (_ => MULTIPLY)

    lazy val divide: PackratParser[ArithmeticOperator] = DIVIDE.sql ^^ (_ => DIVIDE)

    lazy val modulo: PackratParser[ArithmeticOperator] = MODULO.sql ^^ (_ => MODULO)

    lazy val factor: PackratParser[PainlessScript] =
      "(" ~> arithmeticExpressionLevel2 <~ ")" ^^ {
        case expr: ArithmeticExpression =>
          expr.copy(group = true)
        case other => other
      } | valueExpr

    def arithmeticExpressionLevel1: Parser[PainlessScript] =
      factor ~ rep((multiply | divide | modulo) ~ factor) ^^ { case left ~ list =>
        list.foldLeft(left) { case (acc, op ~ right) =>
          ArithmeticExpression(acc, op, right)
        }
      }

    def arithmeticExpressionLevel2: Parser[PainlessScript] =
      arithmeticExpressionLevel1 ~ rep((add | subtract) ~ arithmeticExpressionLevel1) ^^ {
        case left ~ list =>
          list.foldLeft(left) { case (acc, op ~ right) =>
            ArithmeticExpression(acc, op, right)
          }
      }

    def identifierWithArithmeticExpression: Parser[Identifier] =
      (arithmeticExpressionLevel2 >> {
        case af: ArithmeticExpression  => success(Identifier(af))
        case id: Identifier            => success(id)
        case f: FunctionWithIdentifier => success(f.identifier)
        case f: Function               => success(Identifier(f))
        // #250 - defensive arm: every `valueExpr` alternative yields an Identifier today, so this
        // is believed unreachable, which is exactly why it must not be a `throw`. Story 21.1 is
        // about to add alternatives to `valueExpr` (quoted identifiers), changing that
        // reachability. `^^` became `>>` for this one arm; the cost is one extra allocation per
        // application of the grammar's hottest production.
        case other => err(s"Unexpected expression $other")
      }) >> cast

  }
}
