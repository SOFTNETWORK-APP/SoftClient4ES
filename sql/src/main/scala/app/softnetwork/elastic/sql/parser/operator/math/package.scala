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
      }) >> castOperator

  }
}
