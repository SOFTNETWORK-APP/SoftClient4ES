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
      case _ => this.copy(identifier = identifier.update(request))
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
}

object MetricSelectorScript {

  def extractMetricsPath(criteria: Criteria): Map[String, String] = criteria match {
    case Predicate(left, _, right, _, _) =>
      extractMetricsPath(left) ++ extractMetricsPath(right)
    case relation: ElasticRelation => extractMetricsPath(relation.criteria)
    case _: MatchCriteria          => Map.empty //MATCH is not supported in bucket_selector
    case e: Expression if e.aggregation =>
      import e._
      maybeValue match {
        case Some(v: Identifier) => identifier.metricsPath ++ v.metricsPath
        case _                   => identifier.metricsPath
      }
    case _ => Map.empty
  }

  def metricSelector(expr: Criteria): String = expr match {
    case Predicate(left, op, right, maybeNot, group) =>
      val leftStr = metricSelector(left)
      val rightStr = metricSelector(right)
      val opStr = op match {
        case AND | OR => op.painless(None)
        case _        => throw new IllegalArgumentException(s"Unsupported logical operator: $op")
      }
      val not = maybeNot.nonEmpty
      if (group || not)
        s"${maybeNot.map(_ => "!").getOrElse("")}($leftStr) $opStr ($rightStr)"
      else
        s"$leftStr $opStr $rightStr"

    case relation: ElasticRelation => metricSelector(relation.criteria)

    case _: MatchCriteria => "1 == 1" //MATCH is not supported in bucket_selector

    case e: Expression if e.aggregation =>
      val painless = e.painless(None)
      e.maybeValue match {
        case Some(value) if e.operator.isInstanceOf[ComparisonOperator] =>
          value.out match { // compare epoch millis
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

    case _ => "1 == 1" //throw new IllegalArgumentException(s"Unsupported SQLCriteria type: $expr")
  }
}

case class BucketIncludesExcludes(values: Set[String] = Set.empty, regex: Option[String] = None)
