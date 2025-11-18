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

package app.softnetwork.elastic.sql.query

import app.softnetwork.elastic.sql.`type`.SQLTypes
import app.softnetwork.elastic.sql.operator._
import app.softnetwork.elastic.sql.{Expr, Identifier, LongValue, TokenRegex, Updateable}

case object GroupBy extends Expr("GROUP BY") with TokenRegex

case class GroupBy(buckets: Seq[Bucket]) extends Updateable {
  override def sql: String = s" $GroupBy ${buckets.mkString(", ")}"
  def update(request: SQLSearchRequest): GroupBy =
    this.copy(buckets = buckets.map(_.update(request)))
  lazy val bucketNames: Map[String, Bucket] = buckets.map { b =>
    b.identifier.identifierName -> b
  }.toMap

  override def validate(): Either[String, Unit] = {
    if (buckets.isEmpty) {
      Left("At least one bucket is required in GROUP BY clause")
    } else {
      Right(())
    }
  }

  def nestedElements: Seq[NestedElement] =
    buckets.flatMap(_.nestedElement).distinct
}

case class Bucket(
  identifier: Identifier,
  size: Option[Int] = None
) extends Updateable {
  override def sql: String = s"$identifier"
  def update(request: SQLSearchRequest): Bucket = {
    identifier.functions.headOption match {
      case Some(func: LongValue) =>
        if (func.value <= 0) {
          throw new IllegalArgumentException(s"Bucket index must be greater than 0: ${func.value}")
        } else if (request.select.fields.size < func.value) {
          throw new IllegalArgumentException(
            s"Bucket index ${func.value} is out of bounds [1, ${request.fields.size}]"
          )
        } else {
          val field = request.select.fields(func.value.toInt - 1)
          this.copy(identifier = field.identifier, size = request.limit.map(_.limit))
        }
      case _ =>
        this.copy(identifier = identifier.update(request), size = request.limit.map(_.limit))
    }
  }

  lazy val sourceBucket: String =
    if (identifier.nested) {
      identifier.tableAlias
        .map(a => s"$a.")
        .getOrElse("") + identifier.name.split("\\.").tail.mkString(".")
    } else {
      identifier.name
    }
  lazy val nested: Boolean = nestedElement.isDefined
  lazy val nestedElement: Option[NestedElement] = identifier.nestedElement
  lazy val nestedBucket: Option[String] =
    identifier.nestedElement.map(_.innerHitsName)

  lazy val name: String = identifier.fieldAlias.getOrElse(sourceBucket.replace(".", "_"))

  lazy val bucketPath: String = {
    identifier.nestedElement match {
      case Some(ne) => ne.bucketPath
      case None     => "" // Root level
    }
  }
}

object MetricSelectorScript {

  def metricSelector(expr: Criteria): String = expr match {
    case Predicate(left, op, right, maybeNot, group) =>
      val leftStr = metricSelector(left)
      val rightStr = metricSelector(right)

      // Filtering all "1 == 1"
      if (leftStr == "1 == 1" && rightStr == "1 == 1") {
        "1 == 1"
      } else if (leftStr == "1 == 1") {
        rightStr
      } else if (rightStr == "1 == 1") {
        leftStr
      } else {
        val opStr = op match {
          case AND | OR => op.painless(None)
          case _        => throw new IllegalArgumentException(s"Unsupported logical operator: $op")
        }
        val not = maybeNot.nonEmpty
        if (group || not)
          s"${maybeNot.map(_ => "!").getOrElse("")}($leftStr) $opStr ($rightStr)"
        else
          s"$leftStr $opStr $rightStr"
      }

    case relation: ElasticRelation => metricSelector(relation.criteria)

    case _: MultiMatchCriteria => "1 == 1"

    case e: Expression if e.aggregation =>
      // NO FILTERING: the script is generated for all metrics
      val painless = e.painless(None)
      e.maybeValue match {
        case Some(value) if e.operator.isInstanceOf[ComparisonOperator] =>
          value.out match {
            case SQLTypes.Date =>
              s"$painless.truncatedTo(ChronoUnit.DAYS).toInstant().toEpochMilli()"
            case SQLTypes.Time if e.operator.isInstanceOf[ComparisonOperator] =>
              s"$painless.truncatedTo(ChronoUnit.SECONDS).toInstant().toEpochMilli()"
            case SQLTypes.DateTime if e.operator.isInstanceOf[ComparisonOperator] =>
              s"$painless.toInstant().toEpochMilli()"
            case _ => painless
          }
        case _ => painless
      }
    case _ => "1 == 1"
  }
}

case class BucketIncludesExcludes(values: Set[String] = Set.empty, regex: Option[String] = None)
