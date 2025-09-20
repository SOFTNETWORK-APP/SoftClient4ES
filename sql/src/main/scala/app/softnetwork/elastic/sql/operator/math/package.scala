package app.softnetwork.elastic.sql.operator

import app.softnetwork.elastic.sql.Expr

package object math {

  sealed trait ArithmeticOperator extends Operator with BinaryOperator {
    override def toString: String = s" $sql "
  }

  case object Add extends Expr("+") with ArithmeticOperator
  case object Subtract extends Expr("-") with ArithmeticOperator
  case object Multiply extends Expr("*") with ArithmeticOperator
  case object Divide extends Expr("/") with ArithmeticOperator
  case object Modulo extends Expr("%") with ArithmeticOperator

}
