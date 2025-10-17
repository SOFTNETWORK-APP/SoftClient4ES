/*
 * Copyright 2015 SOFTNETWORK
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
    def add: PackratParser[ArithmeticOperator] = ADD.sql ^^ (_ => ADD)

    def subtract: PackratParser[ArithmeticOperator] = SUBTRACT.sql ^^ (_ => SUBTRACT)

    def multiply: PackratParser[ArithmeticOperator] = MULTIPLY.sql ^^ (_ => MULTIPLY)

    def divide: PackratParser[ArithmeticOperator] = DIVIDE.sql ^^ (_ => DIVIDE)

    def modulo: PackratParser[ArithmeticOperator] = MODULO.sql ^^ (_ => MODULO)

    def factor: PackratParser[PainlessScript] =
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
      (arithmeticExpressionLevel2 ^^ {
        case af: ArithmeticExpression  => Identifier(af)
        case id: Identifier            => id
        case f: FunctionWithIdentifier => f.identifier
        case f: Function               => Identifier(f)
        case other                     => throw new Exception(s"Unexpected expression $other")
      }) >> cast

  }
}
