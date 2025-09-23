package app.softnetwork.elastic.sql.operator

import app.softnetwork.elastic.sql.Expr

package object math {

  sealed trait ArithmeticOperator extends Operator with BinaryOperator {
    override def toString: String = s" $sql "
  }

  case object ADD extends Expr("+") with ArithmeticOperator
  case object SUBTRACT extends Expr("-") with ArithmeticOperator
  case object MULTIPLY extends Expr("*") with ArithmeticOperator
  case object DIVIDE extends Expr("/") with ArithmeticOperator
  case object MODULO extends Expr("%") with ArithmeticOperator

}
