package app.softnetwork.elastic.sql

trait SQLOperator extends SQLToken with PainlessScript {
  override def painless: String = this match {
    case And => "&&"
    case Or  => "||"
    case Not => "!"
    case _   => sql
  }
}

sealed trait ArithmeticOperator extends SQLOperator with MathScript {
  override def toString: String = s" $sql "
  override def script: String = sql
}
case object Add extends SQLExpr("+") with ArithmeticOperator
case object Subtract extends SQLExpr("-") with ArithmeticOperator
case object Multiply extends SQLExpr("*") with ArithmeticOperator
case object Divide extends SQLExpr("/") with ArithmeticOperator
case object Modulo extends SQLExpr("%") with ArithmeticOperator

sealed trait SQLExpressionOperator extends SQLOperator

sealed trait SQLComparisonOperator extends SQLExpressionOperator with PainlessScript {
  override def painless: String = this match {
    case Eq    => "=="
    case Ne    => "!="
    case other => other.sql
  }

  def not: SQLComparisonOperator = this match {
    case Eq        => Ne
    case Ne | Diff => Eq
    case Ge        => Lt
    case Gt        => Le
    case Le        => Gt
    case Lt        => Ge
  }
}

case object Eq extends SQLExpr("=") with SQLComparisonOperator
case object Ne extends SQLExpr("<>") with SQLComparisonOperator
case object Diff extends SQLExpr("!=") with SQLComparisonOperator
case object Ge extends SQLExpr(">=") with SQLComparisonOperator
case object Gt extends SQLExpr(">") with SQLComparisonOperator
case object Le extends SQLExpr("<=") with SQLComparisonOperator
case object Lt extends SQLExpr("<") with SQLComparisonOperator

sealed trait SQLLogicalOperator extends SQLExpressionOperator with SQLRegex

case object In extends SQLExpr("in") with SQLLogicalOperator
case object Like extends SQLExpr("like") with SQLLogicalOperator
case object Between extends SQLExpr("between") with SQLLogicalOperator
case object IsNull extends SQLExpr("is null") with SQLLogicalOperator
case object IsNotNull extends SQLExpr("is not null") with SQLLogicalOperator
case object Not extends SQLExpr("not") with SQLLogicalOperator
case object Match extends SQLExpr("match") with SQLLogicalOperator

case object Against extends SQLExpr("against") with SQLRegex

sealed trait SQLPredicateOperator extends SQLLogicalOperator

case object And extends SQLExpr("and") with SQLPredicateOperator
case object Or extends SQLExpr("or") with SQLPredicateOperator

case object Union extends SQLExpr("union") with SQLOperator with SQLRegex

sealed trait ElasticOperator extends SQLOperator with SQLRegex
case object Nested extends SQLExpr("nested") with ElasticOperator
case object Child extends SQLExpr("child") with ElasticOperator
case object Parent extends SQLExpr("parent") with ElasticOperator
