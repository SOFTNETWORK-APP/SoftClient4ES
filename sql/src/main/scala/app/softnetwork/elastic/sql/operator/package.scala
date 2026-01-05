/*
 * Copyright 2025 SOFTNETWORK
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package app.softnetwork.elastic.sql

package object operator {

  trait Operator extends Token with PainlessScript with TokenRegex {
    override def painless(context: Option[PainlessContext]): String = this match {
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

  case object UNION extends Expr("UNION ALL") with Operator with TokenRegex

  sealed trait ElasticOperator extends Operator with TokenRegex

  case object Nested extends Expr("NESTED") with ElasticOperator
  case object Child extends Expr("CHILD") with ElasticOperator
  case object Parent extends Expr("PARENT") with ElasticOperator
}
