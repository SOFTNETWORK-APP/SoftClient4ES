package app.softnetwork.elastic.sql

case object GroupBy extends SQLExpr("group by") with SQLRegex

case class SQLGroupBy(buckets: Seq[SQLBucket]) extends Updateable {
  override def sql: String = s" $GroupBy ${buckets.mkString(",")}"
  def update(request: SQLSearchRequest): SQLGroupBy =
    this.copy(buckets = buckets.map(_.update(request)))
  lazy val bucketNames: Map[String, SQLBucket] = buckets.map { b =>
    b.identifier.identifierName -> b
  }.toMap
}

case class SQLBucket(
  identifier: SQLIdentifier
) extends Updateable {
  override def sql: String = s"$identifier"
  def update(request: SQLSearchRequest): SQLBucket =
    this.copy(identifier = identifier.update(request))
  lazy val sourceBucket: String =
    if (identifier.nested) {
      identifier.tableAlias
        .map(a => s"$a.")
        .getOrElse("") + identifier.name.split("\\.").tail.mkString(".")
    } else {
      identifier.name
    }
  lazy val nestedBucket: Option[String] =
    identifier.nestedType.map(t => s"nested_$t")

  lazy val name: String = identifier.fieldAlias.getOrElse(sourceBucket.replace(".", "_"))
}

object BucketSelectorScript {

  private[this] def painlessIn(param: String, values: Seq[SQLValue[_]], not: Boolean): String = {
    val ret = s"[${values.map { _.painlessValue }.mkString(", ")}].contains($param)"
    if (not) s"!$ret" else ret
  }

  private[this] def painlessBetween(
    param: String,
    lower: SQLValue[_],
    upper: SQLValue[_],
    not: Boolean
  ): String = {
    val ret = s"($param >= ${lower.painlessValue} && $param <= ${upper.painlessValue})"
    if (not) s"!$ret" else ret
  }

  private[this] def toPainless(
    param: String,
    operator: SQLOperator,
    value: SQLToken,
    not: Boolean
  ): String = {
    operator match {
      case _: SQLComparisonOperator =>
        val valueStr =
          value match {
            case v: SQLBoolean => v.painlessValue
            case v: SQLDouble  => v.painlessValue
            case v: SQLLiteral => v.painlessValue
            case v: SQLLong    => v.painlessValue
            case _ =>
              throw new IllegalArgumentException(
                s"Unsupported value type in bucket_selector: $value"
              )
          }
        if (not) {
          operator match {
            case Eq => s"$param != $valueStr"
            case Ne => s"$param == $valueStr"
            case Gt => s"$param <= $valueStr"
            case Ge => s"$param < $valueStr"
            case Lt => s"$param >= $valueStr"
            case Le => s"$param > $valueStr"
            case _ =>
              throw new IllegalArgumentException(
                s"Unsupported comparison operator in bucket_selector: $operator"
              )
          }
        } else
          operator match {
            case Eq => s"$param == $valueStr"
            case Ne => s"$param != $valueStr"
            case Gt => s"$param > $valueStr"
            case Ge => s"$param >= $valueStr"
            case Lt => s"$param < $valueStr"
            case Le => s"$param <= $valueStr"
            case _ =>
              throw new IllegalArgumentException(
                s"Unsupported comparison operator in bucket_selector: $operator"
              )
          }
      case In =>
        value match {
          case SQLDoubleValues(vals)  => painlessIn(param, vals, not)
          case SQLLiteralValues(vals) => painlessIn(param, vals, not)
          case SQLLongValues(vals)    => painlessIn(param, vals, not)
          case _                      => throw new IllegalArgumentException("IN requires a list")
        }
      case Between =>
        value match {
          case SQLDoubleFromTo(lower, upper)  => painlessBetween(param, lower, upper, not)
          case SQLLiteralFromTo(lower, upper) => painlessBetween(param, lower, upper, not)
          case SQLLongFromTo(lower, upper)    => painlessBetween(param, lower, upper, not)
          case _ => throw new IllegalArgumentException("BETWEEN requires two values")
        }
      case _ =>
        throw new IllegalArgumentException(s"Unsupported operator in bucket_selector: $operator")
    }
  }

  def extractBucketsPath(criteria: SQLCriteria): Map[String, String] = criteria match {
    case SQLPredicate(left, _, right, _, _) =>
      extractBucketsPath(left) ++ extractBucketsPath(right)
    case relation: ElasticRelation => extractBucketsPath(relation.criteria)
    case _: SQLMatch               => Map.empty //MATCH is not supported in bucket_selector
    case e: Expression =>
      import e._
      val name = identifier.fieldAlias.getOrElse(identifier.name)
      if (e.aggregation) {
        Map(name -> name)
      } /*else if (e.identifier.bucket.isDefined) {
        Map(name -> "_key")
      }*/
      else {
        Map.empty // for performance, we only allow aggregation here
      }
  }

  def toPainless(expr: SQLCriteria): String = expr match {
    case SQLPredicate(left, op, right, maybeNot, group) =>
      val leftStr = toPainless(left)
      val rightStr = toPainless(right)
      val opStr = op match {
        case And => "&&"
        case Or  => "||"
        case _   => throw new IllegalArgumentException(s"Unsupported logical operator: $op")
      }
      val not = maybeNot.nonEmpty
      if (group || not)
        s"${maybeNot.map(_ => "!").getOrElse("")}($leftStr) $opStr ($rightStr)"
      else
        s"$leftStr $opStr $rightStr"

    case relation: ElasticRelation => toPainless(relation.criteria)

    case _: SQLMatch => "1 == 1" //MATCH is not supported in bucket_selector

    case e: Expression =>
      if (e.aggregation /*|| e.identifier.bucket.isDefined*/ ) { // for performance, we only allow aggregation here
        val param =
          s"params.${e.identifier.fieldAlias.getOrElse(e.identifier.name)}"
        e.maybeValue match {
          case Some(v) => toPainless(param, e.operator, v, e.maybeNot.nonEmpty)
          case None =>
            e.operator match {
              case IsNull    => s"$param == null"
              case IsNotNull => s"$param != null"
              case _ =>
                throw new IllegalArgumentException(s"Operator ${e.operator} requires a value")
            }
        }
      } else {
        "1 == 1"
      }

    case _ => throw new IllegalArgumentException(s"Unsupported SQLCriteria type: $expr")
  }
}
