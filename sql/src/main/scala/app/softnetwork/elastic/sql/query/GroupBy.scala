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
}

case class Bucket(
  identifier: Identifier
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
          this.copy(identifier = field.identifier)
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
  lazy val nested: Boolean = identifier.nested
  lazy val nestedBucket: Option[String] =
    identifier.nestedType.map(t => s"nested_$t")

  lazy val name: String = identifier.fieldAlias.getOrElse(sourceBucket.replace(".", "_"))
}

object BucketSelectorScript {

  def extractBucketsPath(criteria: Criteria): Map[String, String] = criteria match {
    case Predicate(left, _, right, _, _) =>
      extractBucketsPath(left) ++ extractBucketsPath(right)
    case relation: ElasticRelation => extractBucketsPath(relation.criteria)
    case _: MatchCriteria          => Map.empty //MATCH is not supported in bucket_selector
    case e: Expression if e.aggregation =>
      import e._
      maybeValue match {
        case Some(v: Identifier) if v.aggregation =>
          Map(identifier.aliasOrName -> identifier.aliasOrName, v.aliasOrName -> v.aliasOrName)
        case _ => Map(identifier.aliasOrName -> identifier.aliasOrName)
      }
    case _ => Map.empty
  }

  def toPainless(expr: Criteria): String = expr match {
    case Predicate(left, op, right, maybeNot, group) =>
      val leftStr = toPainless(left)
      val rightStr = toPainless(right)
      val opStr = op match {
        case AND | OR => op.painless
        case _        => throw new IllegalArgumentException(s"Unsupported logical operator: $op")
      }
      val not = maybeNot.nonEmpty
      if (group || not)
        s"${maybeNot.map(_ => "!").getOrElse("")}($leftStr) $opStr ($rightStr)"
      else
        s"$leftStr $opStr $rightStr"

    case relation: ElasticRelation => toPainless(relation.criteria)

    case _: MatchCriteria => "1 == 1" //MATCH is not supported in bucket_selector

    case e: Expression if e.aggregation =>
      val paramName = e.identifier.paramName
      e.out match {
        case SQLTypes.Date if e.operator.isInstanceOf[ComparisonOperator] =>
          // protect against null params and compare epoch millis
          s"($paramName != null) && (${e.painless}.truncatedTo(ChronoUnit.DAYS).toInstant().toEpochMilli())"
        case SQLTypes.Time if e.operator.isInstanceOf[ComparisonOperator] =>
          s"($paramName != null) && (${e.painless}.truncatedTo(ChronoUnit.SECONDS).toInstant().toEpochMilli())"
        case SQLTypes.DateTime if e.operator.isInstanceOf[ComparisonOperator] =>
          s"($paramName != null) && (${e.painless}.toInstant().toEpochMilli())"
        case _ =>
          e.painless
      }

    case _ => "1 == 1" //throw new IllegalArgumentException(s"Unsupported SQLCriteria type: $expr")
  }
}
