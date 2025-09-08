package app.softnetwork.elastic.sql

case object Where extends SQLExpr("where") with SQLRegex

sealed trait SQLCriteria extends Updateable {
  def operator: SQLOperator

  def nested: Boolean = false

  def limit: Option[SQLLimit] = None

  def update(request: SQLSearchRequest): SQLCriteria

  def group: Boolean

  def matchCriteria: Boolean = false

  lazy val boolQuery: ElasticBoolQuery =
    ElasticBoolQuery(group = group, matchCriteria = matchCriteria)

  def asFilter(currentQuery: Option[ElasticBoolQuery]): ElasticFilter

  def asBoolQuery(currentQuery: Option[ElasticBoolQuery]): ElasticBoolQuery = {
    currentQuery match {
      case Some(q) if q.group && !group => q
      case Some(q)                      => boolQuery.copy(filtered = q.filtered && !matchCriteria)
      case _                            => boolQuery // FIXME should never be the case
    }
  }

}

case class SQLPredicate(
  leftCriteria: SQLCriteria,
  operator: SQLPredicateOperator,
  rightCriteria: SQLCriteria,
  not: Option[Not.type] = None,
  group: Boolean = false
) extends SQLCriteria {
  override def sql = s"${if (group) s"($leftCriteria"
  else leftCriteria} $operator${not
    .map(_ => " not")
    .getOrElse("")} ${if (group) s"$rightCriteria)" else rightCriteria}"
  override def update(request: SQLSearchRequest): SQLCriteria = {
    val updatedPredicate = this.copy(
      leftCriteria = leftCriteria.update(request),
      rightCriteria = rightCriteria.update(request)
    )
    if (updatedPredicate.nested) {
      val unnested = unnest(updatedPredicate)
      ElasticNested(unnested, unnested.limit)
    } else
      updatedPredicate
  }

  override lazy val limit: Option[SQLLimit] = leftCriteria.limit.orElse(rightCriteria.limit)

  private[this] def unnest(criteria: SQLCriteria): SQLCriteria = criteria match {
    case p: SQLPredicate =>
      p.copy(
        leftCriteria = unnest(p.leftCriteria),
        rightCriteria = unnest(p.rightCriteria)
      )
    case r: ElasticNested => r.criteria
    case _                => criteria
  }

  override def asFilter(currentQuery: Option[ElasticBoolQuery]): ElasticFilter = {
    val query = asBoolQuery(currentQuery)
    operator match {
      case And =>
        (not match {
          case Some(_) => query.not(rightCriteria.asFilter(Option(query)))
          case _       => query.filter(rightCriteria.asFilter(Option(query)))
        }).filter(leftCriteria.asFilter(Option(query)))
      case Or =>
        (not match {
          case Some(_) => query.not(rightCriteria.asFilter(Option(query)))
          case _       => query.should(rightCriteria.asFilter(Option(query)))
        }).should(leftCriteria.asFilter(Option(query)))
    }
  }

  override def nested: Boolean = leftCriteria.nested && rightCriteria.nested

  override def matchCriteria: Boolean = leftCriteria.matchCriteria || rightCriteria.matchCriteria

}

sealed trait ElasticFilter

sealed trait SQLCriteriaWithIdentifier extends SQLCriteria with SQLFunctionChain {
  def identifier: SQLIdentifier
  override def nested: Boolean = identifier.nested
  override def group: Boolean = false
  override lazy val limit: Option[SQLLimit] = identifier.limit
  override val functions: List[SQLFunction] = identifier.functions
}

case class ElasticBoolQuery(
  var innerFilters: Seq[ElasticFilter] = Nil,
  var mustFilters: Seq[ElasticFilter] = Nil,
  var notFilters: Seq[ElasticFilter] = Nil,
  var shouldFilters: Seq[ElasticFilter] = Nil,
  group: Boolean = false,
  filtered: Boolean = true,
  matchCriteria: Boolean = false
) extends ElasticFilter {
  def filter(filter: ElasticFilter): ElasticBoolQuery = {
    if (!filtered) {
      must(filter)
    } else if (filter != this)
      innerFilters = filter +: innerFilters
    this
  }

  def must(filter: ElasticFilter): ElasticBoolQuery = {
    if (filter != this)
      mustFilters = filter +: mustFilters
    this
  }

  def not(filter: ElasticFilter): ElasticBoolQuery = {
    if (filter != this)
      notFilters = filter +: notFilters
    this
  }

  def should(filter: ElasticFilter): ElasticBoolQuery = {
    if (filter != this)
      shouldFilters = filter +: shouldFilters
    this
  }

  def unfilteredMatchCriteria(): ElasticBoolQuery = {
    val query = ElasticBoolQuery().copy(
      mustFilters = this.mustFilters,
      notFilters = this.notFilters,
      shouldFilters = this.shouldFilters
    )
    innerFilters.reverse.map {
      case b: ElasticBoolQuery if b.matchCriteria =>
        b.innerFilters.reverse.foreach(query.must)
        b.mustFilters.reverse.foreach(query.must)
        b.notFilters.reverse.foreach(query.not)
        b.shouldFilters.reverse.foreach(query.should)
      case filter => query.filter(filter)
    }
    query
  }

}

sealed trait Expression extends SQLCriteriaWithIdentifier with ElasticFilter {
  def maybeValue: Option[SQLToken]
  def maybeNot: Option[Not.type]
  def notAsString: String = maybeNot.map(v => s"$v ").getOrElse("")
  def valueAsString: String = maybeValue.map(v => s" $v").getOrElse("")
  override def sql = s"$identifier $notAsString$operator$valueAsString"
}

case class SQLExpression(
  identifier: SQLIdentifier,
  operator: SQLExpressionOperator,
  value: SQLToken,
  maybeNot: Option[Not.type] = None
) extends Expression {
  override def maybeValue: Option[SQLToken] = Option(value)

  override def update(request: SQLSearchRequest): SQLCriteria = {
    val updated = this.copy(identifier = identifier.update(request))
    if (updated.nested) {
      ElasticNested(updated, limit)
    } else
      updated
  }

  override def asFilter(currentQuery: Option[ElasticBoolQuery]): ElasticFilter = this
}

case class SQLIsNull(identifier: SQLIdentifier) extends Expression {
  override val operator: SQLOperator = IsNull

  override def maybeValue: Option[SQLToken] = None

  override def maybeNot: Option[Not.type] = None

  override def update(request: SQLSearchRequest): SQLCriteria = {
    val updated = this.copy(identifier = identifier.update(request))
    if (updated.nested) {
      ElasticNested(updated, limit)
    } else
      updated
  }

  override def asFilter(currentQuery: Option[ElasticBoolQuery]): ElasticFilter = this
}

case class SQLIsNotNull(identifier: SQLIdentifier) extends Expression {
  override val operator: SQLOperator = IsNotNull

  override def maybeValue: Option[SQLToken] = None

  override def maybeNot: Option[Not.type] = None

  override def update(request: SQLSearchRequest): SQLCriteria = {
    val updated = this.copy(identifier = identifier.update(request))
    if (updated.nested) {
      ElasticNested(updated, limit)
    } else
      updated
  }

  override def asFilter(currentQuery: Option[ElasticBoolQuery]): ElasticFilter = this
}

case class SQLIn[R, +T <: SQLValue[R]](
  identifier: SQLIdentifier,
  values: SQLValues[R, T],
  maybeNot: Option[Not.type] = None
) extends Expression { this: SQLIn[R, T] =>
  private[this] lazy val id = functions.headOption match {
    case Some(f) => s"$f($identifier)"
    case _       => s"$identifier"
  }
  override def sql =
    s"$id $notAsString$operator $values"
  override def operator: SQLOperator = In
  override def update(request: SQLSearchRequest): SQLCriteria = {
    val updated = this.copy(identifier = identifier.update(request))
    if (updated.nested) {
      ElasticNested(updated, limit)
    } else
      updated
  }

  override def maybeValue: Option[SQLToken] = Some(values)

  override def asFilter(currentQuery: Option[ElasticBoolQuery]): ElasticFilter = this
}

case class SQLBetween[+T](
  identifier: SQLIdentifier,
  fromTo: SQLFromTo[T],
  maybeNot: Option[Not.type]
) extends Expression {
  private[this] lazy val id = functions.headOption match {
    case Some(f) => s"$f($identifier)"
    case _       => s"$identifier"
  }
  override def sql =
    s"$id $notAsString$operator $fromTo"
  override def operator: SQLOperator = Between
  override def update(request: SQLSearchRequest): SQLCriteria = {
    val updated = this.copy(identifier = identifier.update(request))
    if (updated.nested) {
      ElasticNested(updated, limit)
    } else
      updated
  }

  override def maybeValue: Option[SQLToken] = Some(fromTo)

  override def asFilter(currentQuery: Option[ElasticBoolQuery]): ElasticFilter = this
}

case class ElasticGeoDistance(
  identifier: SQLIdentifier,
  distance: SQLLiteral,
  lat: SQLDouble,
  lon: SQLDouble
) extends Expression {
  override def sql = s"$Distance($identifier,($lat,$lon)) $operator $distance"
  override val functions: List[SQLFunction] = List(Distance)
  override def operator: SQLOperator = Le
  override def update(request: SQLSearchRequest): ElasticGeoDistance =
    this.copy(identifier = identifier.update(request))

  override def maybeValue: Option[SQLToken] = Some(distance)

  override def maybeNot: Option[Not.type] = None

  override def asFilter(currentQuery: Option[ElasticBoolQuery]): ElasticFilter = this
}

case class SQLMatch(
  identifiers: Seq[SQLIdentifier],
  value: SQLLiteral
) extends SQLCriteria {
  override def sql: String =
    s"$operator (${identifiers.mkString(",")}) $Against ($value)"
  override def operator: SQLOperator = Match
  override def update(request: SQLSearchRequest): SQLCriteria =
    this.copy(identifiers = identifiers.map(_.update(request)))

  override lazy val nested: Boolean = identifiers.forall(_.nested)

  lazy val criteria: SQLCriteria = {
    identifiers.map(id => ElasticMatch(id, value, None)) match {
      case Nil           => throw new IllegalArgumentException("No identifiers for MATCH")
      case single :: Nil => single
      case first :: second :: rest =>
        val initial: SQLCriteria = SQLPredicate(first, Or, second)
        rest.foldLeft(initial) { (acc, next) =>
          SQLPredicate(acc, Or, next)
        }
    }
  }

  override def asFilter(currentQuery: Option[ElasticBoolQuery]): ElasticFilter = criteria match {
    case predicate: SQLPredicate => predicate.copy(group = true).asFilter(currentQuery)
    case _                       => criteria.asFilter(currentQuery)
  }

  override def matchCriteria: Boolean = true

  override def group: Boolean = false
}

case class SQLComparisonDateMath(
  identifier: SQLIdentifier,
  operator: SQLComparisonOperator, // Gt, Ge, Lt, Le, Eq, ...
  dateTimeFunction: CurrentDateTimeFunction, // CurrentDate, Now, CurrentTimestamp, CurrentTime, ...
  arithmeticOperator: Option[ArithmeticOperator] =
    None, // Plus or Minus between dateTimeFunction and interval
  interval: Option[TimeInterval] = None, // optional interval
  maybeNot: Option[Not.type] = None
) extends Expression
    with MathScript {
  override def sql: String = {
    s"$identifier ${operator.sql} $dateTimeFunction${asString(arithmeticOperator)}${asString(interval)}"
  }
  override def update(request: SQLSearchRequest): SQLCriteria =
    this.copy(identifier = identifier.update(request))

  override def maybeValue: Option[SQLToken] = Some(SQLScript(script))

  override def asFilter(currentQuery: Option[ElasticBoolQuery]): ElasticFilter = this

  override def script: String = {
    dateTimeFunction match {
      case _: CurrentTimeFunction =>
        val painlessOp = (if (maybeNot.isDefined) operator.not else operator).painless
        (arithmeticOperator, interval) match {
          case (Some(Add), Some(i)) => // compare doc time with now + interval
            s"return doc['${identifier.name}'].value.toLocalTime() $painlessOp LocalTime.now().plus(${i.value}, ${i.unit.painless});"

          case (Some(Subtract), Some(i)) => // compare doc time with now
            s"return doc['${identifier.name}'].value.toLocalTime() $painlessOp LocalTime.now().minus(${i.value}, ${i.unit.painless});"

          case _ =>
            s"return doc['${identifier.name}'].value.toLocalTime() $painlessOp LocalTime.now();"
        }
      case _ =>
        val base = s"${dateTimeFunction.script}"
        val dateMath =
          (arithmeticOperator, interval) match {
            case (Some(Add), Some(i))       => s"$base+${i.script}"
            case (Some(Subtract), Some(i)) => s"$base-${i.script}"
            case _                          => base
          }
        dateTimeFunction match {
          case _: CurrentDateFunction => s"$dateMath/d"
          case _                      => dateMath
        }
    }
  }
}

case class ElasticMatch(
  identifier: SQLIdentifier,
  value: SQLLiteral,
  options: Option[String]
) extends Expression {
  override def sql: String =
    s"$operator($identifier,$value${options.map(o => s""","$o"""").getOrElse("")})"
  override def operator: SQLOperator = Match
  override def update(request: SQLSearchRequest): SQLCriteria =
    this.copy(identifier = identifier.update(request))

  override def maybeValue: Option[SQLToken] = Some(value)

  override def maybeNot: Option[Not.type] = None

  override def asFilter(currentQuery: Option[ElasticBoolQuery]): ElasticFilter = this

  override def matchCriteria: Boolean = true
}

sealed abstract class ElasticRelation(val criteria: SQLCriteria, val operator: ElasticOperator)
    extends SQLCriteria
    with ElasticFilter {
  override def sql = s"$operator($criteria)"

  private[this] def rtype(criteria: SQLCriteria): Option[String] = criteria match {
    case SQLPredicate(left, _, right, _, _) => rtype(left).orElse(rtype(right))
    case c: Expression =>
      c.identifier.nestedType.orElse(c.identifier.name.split('.').headOption)
    case relation: ElasticRelation => relation.relationType
    case _                         => None
  }

  lazy val relationType: Option[String] = rtype(criteria)

  override def asFilter(currentQuery: Option[ElasticBoolQuery]): ElasticFilter = this

  override def group: Boolean = criteria.group

}

case class ElasticNested(override val criteria: SQLCriteria, override val limit: Option[SQLLimit])
    extends ElasticRelation(criteria, Nested) {
  override def update(request: SQLSearchRequest): ElasticNested =
    this.copy(criteria = criteria.update(request))

  override def nested: Boolean = true

  private[this] def name(criteria: SQLCriteria): Option[String] = criteria match {
    case SQLPredicate(left, _, right, _, _) => name(left).orElse(name(right))
    case c: SQLCriteriaWithIdentifier =>
      c.identifier.innerHitsName.orElse(c.identifier.name.split('.').headOption)
    case n: ElasticNested => name(n.criteria)
    case _                => None
  }

  lazy val innerHitsName: Option[String] = name(criteria)
}

case class ElasticChild(override val criteria: SQLCriteria)
    extends ElasticRelation(criteria, Child) {
  override def update(request: SQLSearchRequest): ElasticChild =
    this.copy(criteria = criteria.update(request))
}

case class ElasticParent(override val criteria: SQLCriteria)
    extends ElasticRelation(criteria, Parent) {
  override def update(request: SQLSearchRequest): ElasticParent =
    this.copy(criteria = criteria.update(request))
}

case class SQLWhere(criteria: Option[SQLCriteria]) extends Updateable {
  override def sql: String = criteria match {
    case Some(c) => s" $Where $c"
    case _       => ""
  }
  def update(request: SQLSearchRequest): SQLWhere =
    this.copy(criteria = criteria.map(_.update(request)))

}
