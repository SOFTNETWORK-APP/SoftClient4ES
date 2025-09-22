package app.softnetwork.elastic.sql.parser.operator

import app.softnetwork.elastic.sql.function.{Function, FunctionWithIdentifier}
import app.softnetwork.elastic.sql.{Identifier, PainlessScript}
import app.softnetwork.elastic.sql.operator.math.{
  Add,
  ArithmeticExpression,
  ArithmeticOperator,
  Divide,
  Modulo,
  Multiply,
  Subtract
}
import app.softnetwork.elastic.sql.parser.Parser

package object math {

  trait ArithmeticParser { self: Parser =>
    def add: PackratParser[ArithmeticOperator] = Add.sql ^^ (_ => Add)

    def subtract: PackratParser[ArithmeticOperator] = Subtract.sql ^^ (_ => Subtract)

    def multiply: PackratParser[ArithmeticOperator] = Multiply.sql ^^ (_ => Multiply)

    def divide: PackratParser[ArithmeticOperator] = Divide.sql ^^ (_ => Divide)

    def modulo: PackratParser[ArithmeticOperator] = Modulo.sql ^^ (_ => Modulo)

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
      arithmeticExpressionLevel2 ^^ {
        case af: ArithmeticExpression  => Identifier(af)
        case id: Identifier            => id
        case f: FunctionWithIdentifier => f.identifier
        case f: Function               => Identifier(f)
        case other                     => throw new Exception(s"Unexpected expression $other")
      }

  }
}
