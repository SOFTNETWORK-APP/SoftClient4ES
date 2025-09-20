package app.softnetwork.elastic.sql.operator

import app.softnetwork.elastic.sql.{Expr, MathScript}

package object time {

  sealed trait IntervalOperator extends Operator with BinaryOperator with MathScript {
    override def script: String = sql
    override def toString: String = s" $sql "
    override def painless: String = this match {
      case Plus  => ".plus"
      case Minus => ".minus"
      case _     => sql
    }
  }

  case object Plus extends Expr("+") with IntervalOperator {
    override def painless: String = ".plus"
  }
  case object Minus extends Expr("-") with IntervalOperator {
    override def painless: String = ".minus"
  }

}
