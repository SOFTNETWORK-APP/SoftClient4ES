package app.softnetwork.elastic.sql.operator

import app.softnetwork.elastic.sql.{Expr, MathScript}

package object time {

  sealed trait IntervalOperator extends Operator with BinaryOperator with MathScript {
    override def script: String = sql
    override def toString: String = s" $sql "
    override def painless: String = this match {
      case PLUS  => ".plus"
      case MINUS => ".minus"
      case _     => sql
    }
  }

  case object PLUS extends Expr("+") with IntervalOperator {
    override def painless: String = ".plus"
  }

  case object MINUS extends Expr("-") with IntervalOperator {
    override def painless: String = ".minus"
  }

}
