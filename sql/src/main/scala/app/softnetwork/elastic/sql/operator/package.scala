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
  case object In extends Expr("in") with ComparisonOperator
  case object Like extends Expr("like") with ComparisonOperator
  case object Between extends Expr("between") with ComparisonOperator
  case object IsNull extends Expr("is null") with ComparisonOperator
  case object IsNotNull extends Expr("is not null") with ComparisonOperator

  case object Match extends Expr("match") with ComparisonOperator
  case object Against extends Expr("against") with TokenRegex

  sealed trait LogicalOperator extends ExpressionOperator

  case object Not extends Expr("not") with LogicalOperator

  sealed trait PredicateOperator extends LogicalOperator

  case object And extends Expr("and") with PredicateOperator
  case object Or extends Expr("or") with PredicateOperator

  case object Union extends Expr("union") with Operator with TokenRegex

  sealed trait ElasticOperator extends Operator with TokenRegex

  case object Nested extends Expr("nested") with ElasticOperator
  case object Child extends Expr("child") with ElasticOperator
  case object Parent extends Expr("parent") with ElasticOperator
}
