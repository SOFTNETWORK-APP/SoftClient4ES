package app.softnetwork.elastic.sql

package object operator {

  trait Operator extends Token with PainlessScript with TokenRegex {
    override def painless(): String = this match {
      case AND                  => "&&"
      case OR                   => "||"
      case NOT                  => "!"
      case IN                   => ".contains"
      case LIKE | RLIKE | MATCH => ".matches"
      case EQ                   => "=="
      case NE                   => "!="
      case IS_NULL              => " == null"
      case IS_NOT_NULL          => " != null"
      case _                    => sql
    }
  }

  trait BinaryOperator extends Operator

  trait ExpressionOperator extends Operator

  sealed trait ComparisonOperator extends ExpressionOperator with PainlessScript {
    def not: ComparisonOperator = this match {
      case EQ        => NE
      case NE | DIFF => EQ
      case GE        => LT
      case GT        => LE
      case LE        => GT
      case LT        => GE
    }
  }

  case object EQ extends Expr("=") with ComparisonOperator
  case object NE extends Expr("<>") with ComparisonOperator
  case object DIFF extends Expr("!=") with ComparisonOperator
  case object GE extends Expr(">=") with ComparisonOperator
  case object GT extends Expr(">") with ComparisonOperator
  case object LE extends Expr("<=") with ComparisonOperator
  case object LT extends Expr("<") with ComparisonOperator
  case object IN extends Expr("IN") with ComparisonOperator
  case object LIKE extends Expr("LIKE") with ComparisonOperator
  case object RLIKE extends Expr("RLIKE") with ComparisonOperator
  case object BETWEEN extends Expr("BETWEEN") with ComparisonOperator
  case object IS_NULL extends Expr("IS NULL") with ComparisonOperator
  case object IS_NOT_NULL extends Expr("IS NOT NULL") with ComparisonOperator

  case object MATCH extends Expr("MATCH") with ComparisonOperator
  case object AGAINST extends Expr("AGAINST") with TokenRegex

  sealed trait LogicalOperator extends ExpressionOperator

  case object NOT extends Expr("NOT") with LogicalOperator

  sealed trait PredicateOperator extends LogicalOperator

  case object AND extends Expr("AND") with PredicateOperator
  case object OR extends Expr("OR") with PredicateOperator

  case object UNION extends Expr("UNION") with Operator with TokenRegex

  sealed trait ElasticOperator extends Operator with TokenRegex

  case object Nested extends Expr("NESTED") with ElasticOperator
  case object Child extends Expr("CHILD") with ElasticOperator
  case object Parent extends Expr("PARENT") with ElasticOperator
}
