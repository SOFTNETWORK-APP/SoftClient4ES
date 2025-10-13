package app.softnetwork.elastic.sql.operator

import app.softnetwork.elastic.sql.{DateMathScript, Expr, PainlessContext}

package object time {

  sealed trait IntervalOperator extends Operator with BinaryOperator with DateMathScript {
    override def script: Option[String] = Some(sql)
    override def toString: String = s" $sql "
    override def painless(context: Option[PainlessContext]): String = this match {
      case PLUS  => ".plus"
      case MINUS => ".minus"
      case _     => sql
    }
  }

  case object PLUS extends Expr("+") with IntervalOperator {
    override def painless(context: Option[PainlessContext]): String = ".plus"
  }

  case object MINUS extends Expr("-") with IntervalOperator {
    override def painless(context: Option[PainlessContext]): String = ".minus"
  }

}
