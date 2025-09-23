package app.softnetwork.elastic.sql.query

import app.softnetwork.elastic.sql.`type`.{SQLAny, SQLType, SQLTypeUtils, SQLTypes}
import app.softnetwork.elastic.sql.function._
import app.softnetwork.elastic.sql.function.cond.{
  ConditionalFunction,
  IsNotNullFunction,
  IsNullFunction
}
import app.softnetwork.elastic.sql.function.geo.Distance
import app.softnetwork.elastic.sql.parser.Validator
import app.softnetwork.elastic.sql.operator._
import app.softnetwork.elastic.sql._

import scala.annotation.tailrec

case object Where extends Expr("WHERE") with TokenRegex

sealed trait Criteria extends Updateable with PainlessScript {
  def operator: Operator

  def nested: Boolean = false

  def limit: Option[Limit] = None

  def update(request: SQLSearchRequest): Criteria

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
    case Predicate(left, op, right, maybeNot, group) =>
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
    case m: MatchCriteria          => asGroup(m.criteria.painless)
    case expr: Expression          => asGroup(expr.painless)
    case _ => throw new IllegalArgumentException(s"Unsupported criteria: $this")
  }
}

case class Predicate(
  leftCriteria: Criteria,
  operator: PredicateOperator,
  rightCriteria: Criteria,
  not: Option[Not.type] = None,
  group: Boolean = false
) extends Criteria {
  override def sql = s"${if (group) s"($leftCriteria"
  else leftCriteria} $operator${not
    .map(_ => " not")
    .getOrElse("")} ${if (group) s"$rightCriteria)" else rightCriteria}"
  override def update(request: SQLSearchRequest): Criteria = {
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

  override lazy val limit: Option[Limit] = leftCriteria.limit.orElse(rightCriteria.limit)

  private[this] def unnest(criteria: Criteria): Criteria = criteria match {
    case p: Predicate =>
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

sealed trait Expression extends FunctionChain with ElasticFilter with Criteria { // to fix output type as Boolean
  def identifier: Identifier
  override def nested: Boolean = identifier.nested
  override def group: Boolean = false
  override lazy val limit: Option[Limit] = identifier.limit
  override val functions: List[Function] = identifier.functions
  def maybeValue: Option[Token]
  def maybeNot: Option[Not.type]
  def notAsString: String = maybeNot.map(v => s"$v ").getOrElse("")
  def valueAsString: String = maybeValue.map(v => s" $v").getOrElse("")
  override def sql = s"$identifier $notAsString$operator$valueAsString"

  override lazy val aggregation: Boolean = maybeValue match {
    case Some(v: FunctionChain) => identifier.aggregation || v.aggregation
    case _                      => identifier.aggregation
  }

  def painlessNot: String = operator match {
    case _: ComparisonOperator => ""
    case _                     => maybeNot.map(_.painless).getOrElse("")
  }

  def painlessOp: String = operator match {
    case o: ComparisonOperator if maybeNot.isDefined => o.not.painless
    case _                                           => operator.painless
  }

  def painlessValue: String = maybeValue
    .map {
      case v: Value[_]     => v.painless
      case v: Values[_, _] => v.painless
      case v: Identifier   => v.painless
      case v               => v.sql
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
          case value: Value[_]      => value.out
          case values: Values[_, _] => values.out
          case other                => other.out
        }
      case None => identifier.out
    }
    SQLTypeUtils.coerce(identifier, targetedType)
  }

  protected lazy val check: String =
    operator match {
      case _: ComparisonOperator => s" $painlessOp $painlessValue"
      case _                     => s"$painlessOp($painlessValue)"
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
              Validator.validateTypesMatching(identifier.out, v.out) match {
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

case class GenericExpression(
  identifier: Identifier,
  operator: ExpressionOperator,
  value: Token,
  maybeNot: Option[Not.type] = None
) extends Expression {
  override def maybeValue: Option[Token] = Option(value)

  override def update(request: SQLSearchRequest): Criteria = {
    val updated =
      value match {
        case id: Identifier =>
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

case class IsNullExpr(identifier: Identifier) extends Expression {
  override val operator: Operator = IsNull

  override def maybeValue: Option[Token] = None

  override def maybeNot: Option[Not.type] = None

  override def update(request: SQLSearchRequest): Criteria = {
    val updated = this.copy(identifier = identifier.update(request))
    if (updated.nested) {
      ElasticNested(updated, limit)
    } else
      updated
  }

  override def asFilter(currentQuery: Option[ElasticBoolQuery]): ElasticFilter = this
}

case class IsNotNullExpr(identifier: Identifier) extends Expression {
  override val operator: Operator = IsNotNull

  override def maybeValue: Option[Token] = None

  override def maybeNot: Option[Not.type] = None

  override def update(request: SQLSearchRequest): Criteria = {
    val updated = this.copy(identifier = identifier.update(request))
    if (updated.nested) {
      ElasticNested(updated, limit)
    } else
      updated
  }

  override def asFilter(currentQuery: Option[ElasticBoolQuery]): ElasticFilter = this
}

sealed trait CriteriaWithConditionalFunction[In <: SQLType] extends Expression {
  def conditionalFunction: ConditionalFunction[In]
  override def maybeValue: Option[Token] = None
  override def maybeNot: Option[Not.type] = None
  override def asFilter(currentQuery: Option[ElasticBoolQuery]): ElasticFilter = this
  override val functions: List[Function] = List(conditionalFunction)
  override def sql: String = conditionalFunction.sql
}

object ConditionalFunctionAsCriteria {
  def unapply(f: ConditionalFunction[_]): Option[Criteria] = f match {
    case IsNullFunction(id)    => Some(IsNullCriteria(id))
    case IsNotNullFunction(id) => Some(IsNotNullCriteria(id))
    case _                     => None
  }
}

case class IsNullCriteria(identifier: Identifier) extends CriteriaWithConditionalFunction[SQLAny] {
  override val conditionalFunction: ConditionalFunction[SQLAny] = IsNullFunction(identifier)
  override val operator: Operator = IsNull
  override def update(request: SQLSearchRequest): Criteria = {
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

case class IsNotNullCriteria(identifier: Identifier)
    extends CriteriaWithConditionalFunction[SQLAny] {
  override val conditionalFunction: ConditionalFunction[SQLAny] = IsNotNullFunction(
    identifier
  )
  override val operator: Operator = IsNotNull
  override def update(request: SQLSearchRequest): Criteria = {
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

case class InExpr[R, +T <: Value[R]](
  identifier: Identifier,
  values: Values[R, T],
  maybeNot: Option[Not.type] = None
) extends Expression { this: InExpr[R, T] =>
  private[this] lazy val id = functions.headOption match {
    case Some(f) => s"$f($identifier)"
    case _       => s"$identifier"
  }
  override def sql =
    s"$id $notAsString$operator $values"
  override def operator: Operator = In
  override def update(request: SQLSearchRequest): Criteria = {
    val updated = this.copy(identifier = identifier.update(request))
    if (updated.nested) {
      ElasticNested(updated, limit)
    } else
      updated
  }

  override def maybeValue: Option[Token] = Some(values)

  override def asFilter(currentQuery: Option[ElasticBoolQuery]): ElasticFilter = this

  override def painless: String = s"$painlessNot${identifier.painless}$painlessOp($painlessValue)"
}

case class BetweenExpr[+T](
  identifier: Identifier,
  fromTo: FromTo[T],
  maybeNot: Option[Not.type]
) extends Expression {
  private[this] lazy val id = functions.headOption match {
    case Some(f) => s"$f($identifier)"
    case _       => s"$identifier"
  }
  override def sql =
    s"$id $notAsString$operator $fromTo"
  override def operator: Operator = Between
  override def update(request: SQLSearchRequest): Criteria = {
    val updated = this.copy(identifier = identifier.update(request))
    if (updated.nested) {
      ElasticNested(updated, limit)
    } else
      updated
  }

  override def maybeValue: Option[Token] = Some(fromTo)

  override def asFilter(currentQuery: Option[ElasticBoolQuery]): ElasticFilter = this
}

case class ElasticGeoDistance(
  identifier: Identifier,
  distance: StringValue,
  lat: DoubleValue,
  lon: DoubleValue
) extends Expression {
  override def sql = s"$Distance($identifier,($lat,$lon)) $operator $distance"
  override val functions: List[Function] = List(Distance)
  override def operator: Operator = Le
  override def update(request: SQLSearchRequest): ElasticGeoDistance =
    this.copy(identifier = identifier.update(request))

  override def maybeValue: Option[Token] = Some(distance)

  override def maybeNot: Option[Not.type] = None

  override def asFilter(currentQuery: Option[ElasticBoolQuery]): ElasticFilter = this
}

case class MatchCriteria(
  identifiers: Seq[Identifier],
  value: StringValue
) extends Criteria {
  override def sql: String =
    s"$operator (${identifiers.mkString(",")}) $Against ($value)"
  override def operator: Operator = Match
  override def update(request: SQLSearchRequest): Criteria =
    this.copy(identifiers = identifiers.map(_.update(request)))

  override lazy val nested: Boolean = identifiers.forall(_.nested)

  @tailrec
  private[this] def toCriteria(matches: List[ElasticMatch], curr: Criteria): Criteria =
    matches match {
      case Nil           => curr
      case single :: Nil => Predicate(curr, Or, single)
      case first :: rest => toCriteria(rest, Predicate(curr, Or, first))
    }

  lazy val criteria: Criteria =
    (identifiers.map(id => ElasticMatch(id, value, None)) match {
      case Nil           => throw new IllegalArgumentException("No identifiers for MATCH")
      case single :: Nil => single
      case first :: rest => toCriteria(rest, first)
    }) match {
      case p: Predicate => p.copy(group = true)
      case other        => other
    }

  override def asFilter(currentQuery: Option[ElasticBoolQuery]): ElasticFilter = criteria match {
    case predicate: Predicate => predicate.copy(group = true).asFilter(currentQuery)
    case _                    => criteria.asFilter(currentQuery)
  }

  override def matchCriteria: Boolean = true

  override def group: Boolean = false
}

case class ElasticMatch(
  identifier: Identifier,
  value: StringValue,
  options: Option[String]
) extends Expression {
  override def sql: String =
    s"$operator($identifier,$value${options.map(o => s""","$o"""").getOrElse("")})"
  override def operator: Operator = Match
  override def update(request: SQLSearchRequest): Criteria =
    this.copy(identifier = identifier.update(request))

  override def maybeValue: Option[Token] = Some(value)

  override def maybeNot: Option[Not.type] = None

  override def asFilter(currentQuery: Option[ElasticBoolQuery]): ElasticFilter = this

  override def matchCriteria: Boolean = true

  override def painless: String = s"$painlessNot${identifier.painless}$painlessOp($painlessValue)"
}

sealed abstract class ElasticRelation(val criteria: Criteria, val operator: ElasticOperator)
    extends Criteria
    with ElasticFilter {
  override def sql = s"$operator($criteria)"

  private[this] def rtype(criteria: Criteria): Option[String] = criteria match {
    case Predicate(left, _, right, _, _) => rtype(left).orElse(rtype(right))
    case c: Expression =>
      c.identifier.nestedType.orElse(c.identifier.name.split('.').headOption)
    case relation: ElasticRelation => relation.relationType
    case _                         => None
  }

  lazy val relationType: Option[String] = rtype(criteria)

  override def asFilter(currentQuery: Option[ElasticBoolQuery]): ElasticFilter = this

  override def group: Boolean = criteria.group

}

case class ElasticNested(override val criteria: Criteria, override val limit: Option[Limit])
    extends ElasticRelation(criteria, Nested) {
  override def update(request: SQLSearchRequest): ElasticNested =
    this.copy(criteria = criteria.update(request))

  override def nested: Boolean = true

  private[this] def name(criteria: Criteria): Option[String] = criteria match {
    case Predicate(left, _, right, _, _) => name(left).orElse(name(right))
    case c: Expression =>
      c.identifier.innerHitsName.orElse(c.identifier.name.split('.').headOption)
    case n: ElasticNested => name(n.criteria)
    case _                => None
  }

  lazy val innerHitsName: Option[String] = name(criteria)
}

case class ElasticChild(override val criteria: Criteria) extends ElasticRelation(criteria, Child) {
  override def update(request: SQLSearchRequest): ElasticChild =
    this.copy(criteria = criteria.update(request))
}

case class ElasticParent(override val criteria: Criteria)
    extends ElasticRelation(criteria, Parent) {
  override def update(request: SQLSearchRequest): ElasticParent =
    this.copy(criteria = criteria.update(request))
}

case class Where(criteria: Option[Criteria]) extends Updateable {
  override def sql: String = criteria match {
    case Some(c) => s" $Where $c"
    case _       => ""
  }
  def update(request: SQLSearchRequest): Where =
    this.copy(criteria = criteria.map(_.update(request)))

  override def validate(): Either[String, Unit] = criteria match {
    case Some(c) => c.validate()
    case _       => Right(())
  }

}
