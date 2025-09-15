package app.softnetwork.elastic.sql

import scala.annotation.tailrec

case object Where extends SQLExpr("where") with SQLRegex

sealed trait SQLCriteria extends Updateable with PainlessScript {
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

  private[this] def asGroup(script: String): String = if (group) s"($script)" else script

  override def out: SQLType = SQLTypes.Boolean

  override def painless: String = this match {
    case SQLPredicate(left, op, right, maybeNot, group) =>
      val leftStr = left.painless
      val rightStr = right.painless
      val opStr = op match {
        case And | Or => op.painless
        case _        => throw new IllegalArgumentException(s"Unsupported logical operator: $op")
      }
      val not = maybeNot.nonEmpty
      if (group || not)
        s"${maybeNot.map(_.painless).getOrElse("")}($leftStr $opStr $rightStr)"
      else
        s"$leftStr $opStr $rightStr"
    case relation: ElasticRelation => asGroup(relation.criteria.painless)
    case m: SQLMatch               => asGroup(m.criteria.painless)
    case expr: Expression          => asGroup(expr.painless)
    case _ => throw new IllegalArgumentException(s"Unsupported criteria: $this")
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

  override def validate(): Either[String, Unit] =
    for {
      _ <- leftCriteria.validate()
      _ <- rightCriteria.validate()
    } yield ()
}

sealed trait ElasticFilter

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

sealed trait Expression extends SQLFunctionChain with ElasticFilter with SQLCriteria { // to fix output type as Boolean
  def identifier: SQLIdentifier
  override def nested: Boolean = identifier.nested
  override def group: Boolean = false
  override lazy val limit: Option[SQLLimit] = identifier.limit
  override val functions: List[SQLFunction] = identifier.functions
  def maybeValue: Option[SQLToken]
  def maybeNot: Option[Not.type]
  def notAsString: String = maybeNot.map(v => s"$v ").getOrElse("")
  def valueAsString: String = maybeValue.map(v => s" $v").getOrElse("")
  override def sql = s"$identifier $notAsString$operator$valueAsString"

  override lazy val aggregation: Boolean = maybeValue match {
    case Some(v: SQLFunctionChain) => identifier.aggregation || v.aggregation
    case _                         => identifier.aggregation
  }

  def painlessNot: String = operator match {
    case _: SQLComparisonOperator => ""
    case _                        => maybeNot.map(_.painless).getOrElse("")
  }

  def painlessOp: String = operator match {
    case o: SQLComparisonOperator if maybeNot.isDefined => o.not.painless
    case _                                              => operator.painless
  }

  def painlessValue: String = maybeValue
    .map {
      case v: SQLValue[_]     => v.painless
      case v: SQLValues[_, _] => v.painless
      case v: SQLIdentifier   => v.painless
      case v                  => v.sql
    }
    .getOrElse("") /*{
      operator match {
        case IsNull | IsNotNull => "null"
        case _                  => ""
      }
    }*/

  protected lazy val left: String = {
    val targetedType = maybeValue match {
      case Some(v) =>
        v match {
          case value: SQLValue[_]      => value.out
          case values: SQLValues[_, _] => values.out
          case other                   => other.out
        }
      case None => identifier.out
    }
    SQLTypeUtils.coerce(identifier, targetedType)
  }

  protected lazy val check: String =
    operator match {
      case _: SQLComparisonOperator => s" $painlessOp $painlessValue"
      case _                        => s"$painlessOp($painlessValue)"
    }

  override def painless: String = {
    if (identifier.nullable) {
      return s"def left = $left; left == null ? false : ${painlessNot}left$check"
    }
    s"$painlessNot$left$check"
  }

  override def validate(): Either[String, Unit] = {
    for {
      _ <- identifier.validate()
      _ <- maybeValue match {
        case Some(v) =>
          v.validate() match {
            case Left(err) => Left(s"$err in expression: $this")
            case Right(_) =>
              SQLValidator.validateTypesMatching(identifier.out, v.out) match {
                case Left(_) =>
                  Left(
                    s"Type mismatch: '${out.typeId}' is not compatible with '${v.out.typeId}' in expression: $this"
                  )
                case Right(_) => Right(())
              }
          }
        case _ => Right(())
      }
    } yield ()
  }
}

case class SQLExpression(
  identifier: SQLIdentifier,
  operator: SQLExpressionOperator,
  value: SQLToken,
  maybeNot: Option[Not.type] = None
) extends Expression {
  override def maybeValue: Option[SQLToken] = Option(value)

  override def update(request: SQLSearchRequest): SQLCriteria = {
    val updated =
      value match {
        case id: SQLIdentifier =>
          this.copy(identifier = identifier.update(request), value = id.update(request))
        case _ => this.copy(identifier = identifier.update(request))
      }
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

sealed trait SQLCriteriaWithConditionalFunction[In <: SQLType] extends Expression {
  def conditionalFunction: SQLConditionalFunction[In]
  override def maybeValue: Option[SQLToken] = None
  override def maybeNot: Option[Not.type] = None
  override def asFilter(currentQuery: Option[ElasticBoolQuery]): ElasticFilter = this
  override val functions: List[SQLFunction] = List(conditionalFunction)
  override def sql = s"${conditionalFunction.sql}($identifier)"
}

object SQLConditionalFunctionAsCriteria {
  def unapply(f: SQLConditionalFunction[_]): Option[SQLCriteria] = f match {
    case SQLIsNullFunction(id)    => Some(SQLIsNullCriteria(id))
    case SQLIsNotNullFunction(id) => Some(SQLIsNotNullCriteria(id))
    case _                        => None
  }
}

case class SQLIsNullCriteria(identifier: SQLIdentifier)
    extends SQLCriteriaWithConditionalFunction[SQLAny] {
  override val conditionalFunction: SQLConditionalFunction[SQLAny] = SQLIsNullFunction(identifier)
  override val operator: SQLOperator = IsNull
  override def update(request: SQLSearchRequest): SQLCriteria = {
    val updated = this.copy(identifier = identifier.update(request))
    if (updated.nested) {
      ElasticNested(updated, limit)
    } else
      updated
  }
  override def painless: String = {
    if (identifier.nullable) {
      return s"def left = $left; left == null"
    }
    s"$painlessNot$left$check"
  }

}

case class SQLIsNotNullCriteria(identifier: SQLIdentifier)
    extends SQLCriteriaWithConditionalFunction[SQLAny] {
  override val conditionalFunction: SQLConditionalFunction[SQLAny] = SQLIsNotNullFunction(
    identifier
  )
  override val operator: SQLOperator = IsNotNull
  override def update(request: SQLSearchRequest): SQLCriteria = {
    val updated = this.copy(identifier = identifier.update(request))
    if (updated.nested) {
      ElasticNested(updated, limit)
    } else
      updated
  }

  override def painless: String = {
    if (identifier.nullable) {
      return s"def left = $left; left != null"
    }
    s"$painlessNot$left$check"
  }

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

  override def painless: String = s"$painlessNot${identifier.painless}$painlessOp($painlessValue)"
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
  distance: SQLStringValue,
  lat: SQLDoubleValue,
  lon: SQLDoubleValue
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
  value: SQLStringValue
) extends SQLCriteria {
  override def sql: String =
    s"$operator (${identifiers.mkString(",")}) $Against ($value)"
  override def operator: SQLOperator = Match
  override def update(request: SQLSearchRequest): SQLCriteria =
    this.copy(identifiers = identifiers.map(_.update(request)))

  override lazy val nested: Boolean = identifiers.forall(_.nested)

  @tailrec
  private[this] def toCriteria(matches: List[ElasticMatch], curr: SQLCriteria): SQLCriteria =
    matches match {
      case Nil           => curr
      case single :: Nil => SQLPredicate(curr, Or, single)
      case first :: rest => toCriteria(rest, SQLPredicate(curr, Or, first))
    }

  lazy val criteria: SQLCriteria =
    (identifiers.map(id => ElasticMatch(id, value, None)) match {
      case Nil           => throw new IllegalArgumentException("No identifiers for MATCH")
      case single :: Nil => single
      case first :: rest => toCriteria(rest, first)
    }) match {
      case p: SQLPredicate => p.copy(group = true)
      case other           => other
    }

  override def asFilter(currentQuery: Option[ElasticBoolQuery]): ElasticFilter = criteria match {
    case predicate: SQLPredicate => predicate.copy(group = true).asFilter(currentQuery)
    case _                       => criteria.asFilter(currentQuery)
  }

  override def matchCriteria: Boolean = true

  override def group: Boolean = false
}

case class ElasticMatch(
  identifier: SQLIdentifier,
  value: SQLStringValue,
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

  override def painless: String = s"$painlessNot${identifier.painless}$painlessOp($painlessValue)"
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
    case c: Expression =>
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

  override def validate(): Either[String, Unit] = criteria match {
    case Some(c) => c.validate()
    case _       => Right(())
  }

}
