package app.softnetwork.elastic.sql

case object GroupBy extends SQLExpr("group by") with SQLRegex

case class SQLGroupBy(buckets: Seq[SQLBucket]) extends Updateable {
  override def sql: String = s" $GroupBy ${buckets.mkString(", ")}"
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
    val ret = s"[${values.map { _.painless }.mkString(", ")}].contains($param)"
    if (not) s"!$ret" else ret
  }

  private[this] def painlessBetween(
    param: String,
    lower: SQLValue[_],
    upper: SQLValue[_],
    not: Boolean
  ): String = {
    val ret = s"($param >= ${lower.painless} && $param <= ${upper.painless})"
    if (not) s"!$ret" else ret
  }

  private[this] def toPainless(
    param: String,
    operator: SQLOperator,
    value: SQLToken,
    not: Boolean
  ): String = {
    operator match {
      case o: SQLComparisonOperator =>
        val valueStr =
          value match {
            case v: SQLBoolean => v.painless
            case v: SQLDouble  => v.painless
            case v: SQLLiteral => v.painless
            case v: SQLLong    => v.painless
            case _ =>
              throw new IllegalArgumentException(
                s"Unsupported value type in bucket_selector: $value"
              )
          }
        if (not)
          s"$param ${o.not.painless} $valueStr"
        else
          s"$param ${o.painless} $valueStr"
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
    case b: BinaryExpression =>
      import b._
      if (left.aggregation && right.aggregation)
        Map(left.aliasOrName -> left.aliasOrName, right.aliasOrName -> right.aliasOrName)
      else if (left.aggregation)
        Map(left.aliasOrName -> left.aliasOrName)
      else if (right.aggregation)
        Map(right.aliasOrName -> right.aliasOrName)
      else
        Map.empty
    case e: Expression if e.aggregation =>
      import e._
      Map(identifier.aliasOrName -> identifier.aliasOrName)
    case _ => Map.empty
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

    case SQLComparisonDateMath(identifier, op, dateFunc, arithOp, interval, maybeNot)
        if identifier.aggregation =>
      val painlessOp = if (maybeNot.nonEmpty) op.not.painless else op.painless
      val paramName = identifier.aliasOrName
      // always use a correct "now" creation
      val now = "ZonedDateTime.now(ZoneId.of('Z'))"

      // build the RHS as a Painless ZonedDateTime (apply +/- interval using TimeInterval.painless)
      val rightBase = (arithOp, interval) match {
        case (Some(Add), Some(i))      => s"$now.plus(${i.painless})"
        case (Some(Subtract), Some(i)) => s"$now.minus(${i.painless})"
        case _                         => now
      }

      val rightZdt = dateFunc match {
        // truncate only after arithmetic for CurrentDate
        case _: CurrentDateFunction => s"$rightBase.truncatedTo(ChronoUnit.DAYS)"
        case _: CurrentTimeFunction => s"$rightBase.truncatedTo(ChronoUnit.SECONDS)"
        case _                      => rightBase
      }

      // protect against null params and compare epoch millis
      s"(params.$paramName != null) && (params.$paramName $painlessOp $rightZdt.toInstant().toEpochMilli())"

    case _: SQLMatch => "1 == 1" //MATCH is not supported in bucket_selector

    case e: Expression if e.aggregation =>
      val param =
        s"params.${e.identifier.aliasOrName}"
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

    case _ => "1 == 1" //throw new IllegalArgumentException(s"Unsupported SQLCriteria type: $expr")
  }
}
