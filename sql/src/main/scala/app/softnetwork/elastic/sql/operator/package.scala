package app.softnetwork.elastic.sql

package object operator {

  trait Operator extends Token with PainlessScript with TokenRegex {
    override def painless: String = this match {
      case And          => "&&"
      case Or           => "||"
      case Not          => "!"
      case In           => ".contains"
      case Like | Match => ".matches"
      case Eq           => "=="
      case Ne           => "!="
      case IsNull       => " == null"
      case IsNotNull    => " != null"
      case _            => sql
    }
  }

  trait BinaryOperator extends Operator

  trait ExpressionOperator extends Operator

  sealed trait ComparisonOperator extends ExpressionOperator with PainlessScript {
    def not: ComparisonOperator = this match {
      case Eq        => Ne
      case Ne | Diff => Eq
      case Ge        => Lt
      case Gt        => Le
      case Le        => Gt
      case Lt        => Ge
    }
  }

  case object Eq extends Expr("=") with ComparisonOperator
  case object Ne extends Expr("<>") with ComparisonOperator
  case object Diff extends Expr("!=") with ComparisonOperator
  case object Ge extends Expr(">=") with ComparisonOperator
  case object Gt extends Expr(">") with ComparisonOperator
  case object Le extends Expr("<=") with ComparisonOperator
  case object Lt extends Expr("<") with ComparisonOperator
  case object In extends Expr("IN") with ComparisonOperator
  case object Like extends Expr("LIKE") with ComparisonOperator
  case object Between extends Expr("BETWEEN") with ComparisonOperator
  case object IsNull extends Expr("IS NULL") with ComparisonOperator
  case object IsNotNull extends Expr("IS NOT NULL") with ComparisonOperator

  case object Match extends Expr("MATCH") with ComparisonOperator
  case object Against extends Expr("AGAINST") with TokenRegex

  sealed trait LogicalOperator extends ExpressionOperator

  case object Not extends Expr("NOT") with LogicalOperator

  sealed trait PredicateOperator extends LogicalOperator

  case object And extends Expr("AND") with PredicateOperator
  case object Or extends Expr("OR") with PredicateOperator

  case object Union extends Expr("UNION") with Operator with TokenRegex

  sealed trait ElasticOperator extends Operator with TokenRegex

  case object Nested extends Expr("NESTED") with ElasticOperator
  case object Child extends Expr("CHILD") with ElasticOperator
  case object Parent extends Expr("PARENT") with ElasticOperator
}
