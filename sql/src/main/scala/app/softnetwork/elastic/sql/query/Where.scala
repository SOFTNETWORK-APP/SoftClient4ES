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

import app.softnetwork.elastic.sql.`type`.{
  SQLAny,
  SQLArray,
  SQLTemporal,
  SQLType,
  SQLTypeUtils,
  SQLTypes
}
import app.softnetwork.elastic.sql.function._
import app.softnetwork.elastic.sql.function.cond.{ConditionalFunction, IsNotNull, IsNull}
import app.softnetwork.elastic.sql.function.geo.Distance
import app.softnetwork.elastic.sql.parser.Validator
import app.softnetwork.elastic.sql.operator._
import app.softnetwork.elastic.sql._

import scala.annotation.tailrec

case object Where extends Expr("WHERE") with TokenRegex

sealed trait Criteria extends Updateable with PainlessScript {
  def operator: Operator

  def dependencies: Seq[Identifier] = this match {
    case Predicate(left, _, right, _, _) => left.dependencies ++ right.dependencies
    case c: Expression                   => c.dependencies
    case relation: ElasticRelation       => relation.criteria.dependencies
    case m: MultiMatchCriteria           => m.dependencies
    case _                               => Nil
  }

  /** Every identifier this criteria names directly — both operands of an equality included.
    * Distinct from [[dependencies]], which returns the identifiers reached *through* an
    * identifier's functions and so is empty for a plain `a = 1`.
    */
  def referencedIdentifiers: Seq[Identifier] = this match {
    case Predicate(left, _, right, _, _) =>
      left.referencedIdentifiers ++ right.referencedIdentifiers
    case e: Expression =>
      e.identifier +: (e.maybeValue match {
        case Some(id: Identifier) => Seq(id)
        case _                    => Nil
      })
    case relation: ElasticRelation => relation.criteria.referencedIdentifiers
    case m: MultiMatchCriteria     => m.identifiers
    case _                         => Nil
  }

  def nested: Boolean = false

  def nestedElement: Option[NestedElement]

  def nestedElements: Seq[NestedElement] =
    this match {
      case p: Predicate          => p.nestedElements
      case r: ElasticRelation    => r.criteria.nestedElements
      case e: Expression         => e.nestedElement.toSeq
      case m: MultiMatchCriteria => m.criteria.nestedElements
      case _                     => Nil
    }

  def nestedCriteria(innerHitsName: String): Seq[Criteria] = {
    this match {
      case e: ElasticNested => e.criteria.nestedCriteria(innerHitsName)
      case _ =>
        nestedElements
          .find(n => n.innerHitsName == innerHitsName)
          .map(_ => this)
          .toSeq
    }
  }

  def extractAllMetricsPath: Map[String, String] =
    this match {
      case Predicate(left, _, right, _, _) =>
        left.extractAllMetricsPath ++ right.extractAllMetricsPath
      case relation: ElasticRelation => relation.criteria.extractAllMetricsPath
      case _: MultiMatchCriteria     => Map.empty
      case e: Expression             => e.extractAllMetricsPath
      case _                         => Map.empty
    }

  /** Extracts aggregation fields from criteria expressions (e.g. HAVING COUNT(*) > 1). Used to
    * ensure aggregations referenced only in HAVING/WHERE are included in the query. Note: returned
    * Fields are not updated against SingleSearch — they carry only the identifier and a computed
    * alias (metricName), which is sufficient for metric aggregation creation via
    * SQLAggregation.fromField.
    */
  def extractAggregationFields: Seq[Field] =
    this match {
      case Predicate(left, _, right, _, _) =>
        left.extractAggregationFields ++ right.extractAggregationFields
      case relation: ElasticRelation => relation.criteria.extractAggregationFields
      case e: Expression =>
        val identifiers = Seq(e.identifier) ++ e.maybeValue.collect { case id: Identifier => id }
        identifiers
          .filter(_.aggregations.nonEmpty)
          .flatMap { id =>
            id.metricName.map(name => Field(id, Some(Alias(name))))
          }
      case _ => Seq.empty
    }

  def includes(
    bucket: Bucket,
    not: Boolean,
    bucketIncludesExcludes: BucketIncludesExcludes
  ): BucketIncludesExcludes =
    this match {
      case Predicate(left, _, right, n, _) =>
        right.includes(
          bucket,
          (!not && n.isDefined) || (not && n.isEmpty),
          left.includes(bucket, not, bucketIncludesExcludes)
        )
      case relation: ElasticRelation =>
        relation.criteria.includes(bucket, not, bucketIncludesExcludes)
      case m: MultiMatchCriteria => m.criteria.includes(bucket, not, bucketIncludesExcludes)
      case e: Expression         => e.includes(bucket, not, bucketIncludesExcludes)
      case _                     => bucketIncludesExcludes
    }

  def excludes(
    bucket: Bucket,
    not: Boolean,
    bucketIncludesExcludes: BucketIncludesExcludes
  ): BucketIncludesExcludes =
    includes(bucket, !not, bucketIncludesExcludes)

  def limit: Option[Limit] = None

  def update(request: SingleSearch): Criteria

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

  /** A criterion's Painless is a comparison, so the value it emits is a BOOLEAN — and `baseType` is
    * what every `SQLTypeUtils.coerce` call site reads to learn the type of the string it was handed
    * (`coerce(in, to, ctx)` takes `expr = in.painless(ctx)` and `from = in.baseType`).
    *
    * Without this override an `Expression` inherits `FunctionChain.baseType`, which walks the
    * IDENTIFIER's function chain: `1 = 1` reported `BIGINT` (the left operand), `descr = 'x'`
    * reported the column's type. That has always been wrong, but it was INERT while no `(_,
    * BOOLEAN)` arm existed in `coerce` — every such pair fell to the identity fallback and the
    * comparison was emitted untouched, which happened to be right.
    *
    * 🔴 Story 21.8 part A added the `-> BOOLEAN` arms, and the lie became live. `CASE WHEN 1 = 1`
    * coerced an already-boolean comparison FROM `BIGINT`, so the numeric arm fired AND — because
    * the arm returns a primitive — the end-of-method null guard wrapped it, emitting
    *
    * {{{def param1 = (1 == 1 != null ? (def)((1 == 1 != 0)) : null); param1 ? "a" : "b"}}}
    *
    * which Elasticsearch refuses to compile. All five clients emitted that same script and were
    * rejected with `search_phase_execution_exception: all shards failed; compile error` (HTTP 400);
    * the REST-high-level legs' raw response carries the detail — `script_exception: compile error`
    * with the pointer at offset 14 and `caused_by class_cast_exception: "Cannot cast from [boolean]
    * to [java.lang.Object]."`. So every `CASE WHEN <comparison>` failed on every ES major — the
    * shape Superset and Tableau generate constantly, and the connection handshake's own `SELECT
    * CASE WHEN 1 = 1 …` probe, which is what caught it.
    *
    * Fixing it inside `coerce` is not possible: by the time an arm matches, a genuine `CAST(1 AS
    * BOOLEAN)` and this comparison are the same `(BIGINT, BOOLEAN)` pair. The source type has to be
    * right before `coerce` is called.
    *
    * `out` above already fixes the OUTPUT type to boolean for exactly this reason; the two facts
    * were simply never stated together. `Criteria` is the one place both belong, and `baseType`
    * here overrides `FunctionChain`'s by linearization for `Expression` (`Criteria` is its last
    * mixin).
    */
  override def baseType: SQLType = SQLTypes.Boolean

  override def painless(context: Option[PainlessContext]): String = this match {
    case Predicate(left, op, right, maybeNot, group) =>
      // 🔴 Story BIDC-8 AD-9 — each operand is PARENTHESISED. An operand that carries a null guard
      // renders as a ternary, and `?:` has the LOWEST precedence in Painless, so the unparenthesised
      // composition `a == null ? false : (x) && b == null ? false : (y)` parses as
      // `a == null ? false : (((x) && b == null) ? false : (y))` — the second predicate silently
      // became part of the first one's condition. MEASURED at the baseline on `WHERE UPPER(status) =
      // 'A' AND id = 1` and on the all-bare-column twin; a filter cannot be composed from ternaries
      // without parentheses.
      val leftStr = s"(${left.painless(context)})"
      val rightStr = s"(${right.painless(context)})"
      val opStr = op match {
        case AND | OR => op.painless(context)
        case _        => throw new IllegalArgumentException(s"Unsupported logical operator: $op")
      }
      val not = maybeNot.nonEmpty
      if (group || not)
        s"${maybeNot.map(_.painless(context)).getOrElse("")}($leftStr $opStr $rightStr)"
      else
        s"$leftStr $opStr $rightStr"
    case relation: ElasticRelation => asGroup(relation.criteria.painless(context))
    case m: MultiMatchCriteria     => asGroup(m.criteria.painless(context))
    case expr: Expression          => asGroup(expr.painless(context))
    case _ => throw new IllegalArgumentException(s"Unsupported criteria: $this")
  }
}

case class Predicate(
  leftCriteria: Criteria,
  operator: PredicateOperator,
  rightCriteria: Criteria,
  not: Option[NOT.type] = None,
  group: Boolean = false
) extends Criteria {
  override def sql = s"${if (group) s"($leftCriteria"
  else leftCriteria} $operator${not
    .map(_ => " NOT")
    .getOrElse("")} ${if (group) s"$rightCriteria)" else rightCriteria}"
  override def update(request: SingleSearch): Criteria = {
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
      case AND =>
        (not match {
          case Some(_) => query.not(rightCriteria.asFilter(Option(query)))
          case _       => query.filter(rightCriteria.asFilter(Option(query)))
        }).filter(leftCriteria.asFilter(Option(query)))
      case OR =>
        (not match {
          case Some(_) => query.not(rightCriteria.asFilter(Option(query)))
          case _       => query.should(rightCriteria.asFilter(Option(query)))
        }).should(leftCriteria.asFilter(Option(query)))
    }
  }

  def nestedElement: Option[NestedElement] = None

  override def nestedElements: Seq[NestedElement] = {
    leftCriteria.nestedElements ++ rightCriteria.nestedElements
  }

  override def nested: Boolean =
    leftCriteria.nested && rightCriteria.nested

  override def matchCriteria: Boolean = leftCriteria.matchCriteria || rightCriteria.matchCriteria

  override def validate(): Either[String, Unit] =
    for {
      _ <- leftCriteria.validate()
      _ <- rightCriteria.validate()
    } yield ()

  override def nestedCriteria(innerHitsName: String): Seq[Criteria] =
    leftCriteria.nestedCriteria(innerHitsName) ++ rightCriteria.nestedCriteria(innerHitsName)
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
  def nestedElement: Option[NestedElement] = identifier.nestedElement
  override def nested: Boolean = identifier.nested
  override def group: Boolean = false
  override lazy val limit: Option[Limit] = identifier.limit
  override val functions: List[Function] = identifier.functions
  def maybeValue: Option[Token]
  def maybeNot: Option[NOT.type]
  def notAsString: String = maybeNot.map(v => s"$v ").getOrElse("")
  def valueAsString: String = maybeValue.map(v => s" $v").getOrElse("")
  override def sql = s"$identifier $notAsString$operator$valueAsString"

  override lazy val dependencies: Seq[Identifier] =
    maybeValue match {
      case Some(id: Identifier) => identifier.dependencies ++ id.dependencies
      case _                    => identifier.dependencies
    }

  override def extractAllMetricsPath: Map[String, String] =
    maybeValue match {
      case Some(v: Identifier) =>
        identifier.allMetricsPath ++ v.allMetricsPath
      case _ => identifier.allMetricsPath
    }

  override def includes(
    bucket: Bucket,
    not: Boolean,
    bucketIncludesExcludes: BucketIncludesExcludes
  ): BucketIncludesExcludes = {
    identifier.bucket.find(_.name == bucket.name) match {
      case Some(_) =>
        operator match {
          // These feed the ES `terms` include/exclude and its regex form, so they need the value
          // itself, never its SQL rendering: `.sql` carries the quote delimiters (which used to be
          // stripped for EQ/NE but not for LIKE/RLIKE, embedding `'` in the regex) and, since
          // string literals are escaped, would also carry `\\` for a value holding one backslash.
          case EQ =>
            if ((!not && maybeNot.isEmpty) || (not && maybeNot.isDefined))
              maybeValue match {
                case Some(v: Value[_]) if v.value.toString.nonEmpty =>
                  bucketIncludesExcludes.copy(values =
                    bucketIncludesExcludes.values ++ Set(v.value.toString)
                  )
                case _ => bucketIncludesExcludes
              }
            else bucketIncludesExcludes
          case NE | DIFF =>
            if ((not && maybeNot.isEmpty) || (!not && maybeNot.isDefined))
              maybeValue match {
                case Some(v: Value[_]) if v.value.toString.nonEmpty =>
                  bucketIncludesExcludes.copy(values =
                    bucketIncludesExcludes.values ++ Set(v.value.toString)
                  )
                case _ => bucketIncludesExcludes
              }
            else bucketIncludesExcludes
          case LIKE =>
            if ((!not && maybeNot.isEmpty) || (not && maybeNot.isDefined))
              maybeValue match {
                case Some(v: StringValue) if v.value.nonEmpty =>
                  bucketIncludesExcludes.copy(regex =
                    bucketIncludesExcludes.regex.orElse(Option(v.value.replaceAll("%", ".*")))
                  )
                case _ => bucketIncludesExcludes
              }
            else bucketIncludesExcludes
          case RLIKE =>
            if ((!not && maybeNot.isEmpty) || (not && maybeNot.isDefined))
              maybeValue match {
                case Some(v: StringValue) if v.value.nonEmpty =>
                  bucketIncludesExcludes.copy(regex =
                    bucketIncludesExcludes.regex.orElse(Option(v.value))
                  )
                case _ => bucketIncludesExcludes
              }
            else bucketIncludesExcludes
          case _ =>
            bucketIncludesExcludes
        }
      case _ => bucketIncludesExcludes
    }
  }

  override lazy val isAggregation: Boolean = maybeValue match {
    case Some(v) => identifier.isAggregation || v.isAggregation
    case _       => identifier.isAggregation
  }

  override lazy val hasAggregation: Boolean = maybeValue match {
    case Some(v) => identifier.hasAggregation || v.hasAggregation
    case _       => identifier.hasAggregation
  }

  def hasBucket: Boolean = identifier.hasBucket || maybeValue.exists {
    case v: Identifier => v.hasBucket
    case v: Expression => v.hasBucket
    case _             => false
  }

  def painlessNot: String = operator match {
    case _: ComparisonOperator => ""
    case _                     => maybeNot.map(_.painless(None)).getOrElse("")
  }

  def painlessOp: String = operator match {
    case o: ComparisonOperator if maybeNot.isDefined => o.not.painless(None)
    case _                                           => operator.painless(None)
  }

  def painlessValue(context: Option[PainlessContext]): String = maybeValue
    .map {
      case v: PainlessScript => v.painless(context)
      case v                 => v.sql
    }
    .getOrElse("") /*{
      operator match {
        case IsNull | IsNotNull => "null"
        case _                  => ""
      }
    }*/

  protected def left(context: Option[PainlessContext]): String = {
    val targetedType = maybeValue match {
      case Some(v) => SQLTypeUtils.leastCommonSuperType(List(identifier.out, v.out))
      case None    => identifier.out
    }
    context match {
      case Some(ctx) =>
        ctx.addParam(identifier) match {
          case Some(_) =>
            identifier.originalType match {
              case SQLTypes.Any => // in painless context, Any is ZonedDateTime
                maybeValue.map(_.out).getOrElse(SQLTypes.Any) match {
                  case SQLTypes.Date =>
                    identifier.addPainlessMethod(".toLocalDate()")
                  case SQLTypes.Time =>
                    identifier.addPainlessMethod(".toLocalTime()")
                  case _ =>
                }
              case _ =>
            }
          case _ => // do nothing
        }
      case _ => // do nothing
    }
    SQLTypeUtils.coerce(identifier, targetedType, context)
  }

  protected def check(context: Option[PainlessContext], param: String): String =
    check(context, param, painlessValue(context))

  /** `param <operator> value` in the operator's Painless spelling. `value` is the already-rendered
    * right-hand side, so a caller may adapt it first (the bucket-pipeline rendering converts a
    * temporal literal to epoch millis, the unit a date metric arrives in).
    */
  protected def check(context: Option[PainlessContext], param: String, value: String): String = {
    operator match {
      case _: ComparisonOperator =>
        // 🔴 Story BIDC-8 AD-9 — dispatch on the EFFECTIVE operator, i.e. with a `NOT` folded in,
        // exactly as `painlessOp` does for the generic fallback below. Each arm here returns a
        // HARD-CODED spelling (`compareTo(...) == 0`, `isEqual(...)`, …) chosen from the RAW
        // operator, so `WHERE NOT status = 'A'` and `WHERE NOT UPPER(status) = 'A'` both emitted
        // the same Painless as their un-negated twins and the NOT was silently lost — a wrong
        // answer, not an error (MEASURED at the baseline: `maybeNot = Some(NOT)` on the AST,
        // `compareTo("A") == 0` in the script).
        val comparison = operator match {
          case o: ComparisonOperator if maybeNot.isDefined => o.not
          case o                                           => o
        }
        comparison match {
          case LT =>
            maybeValue.map(v => v.out).getOrElse(SQLTypes.Any) match {
              case SQLTypes.Varchar =>
                return s"$param.compareTo($value) < 0"
              case _: SQLTemporal if !isAggregation && !hasBucket =>
                return s"$param.isBefore($value)"
              case _ =>
            }
          case GT =>
            maybeValue.map(v => v.out).getOrElse(SQLTypes.Any) match {
              case SQLTypes.Varchar =>
                return s"$param.compareTo($value) > 0"
              case _: SQLTemporal if !isAggregation && !hasBucket =>
                return s"$param.isAfter($value)"
              case _ =>
            }
          case EQ =>
            maybeValue.map(v => v.out).getOrElse(SQLTypes.Any) match {
              case SQLTypes.Varchar =>
                return s"$param.compareTo($value) == 0"
              case _: SQLTemporal if !isAggregation && !hasBucket =>
                return s"$param.isEqual($value)"
              case _ =>
            }
          case NE | DIFF =>
            maybeValue.map(v => v.out).getOrElse(SQLTypes.Any) match {
              case SQLTypes.Varchar =>
                return s"$param.compareTo($value) != 0"
              case _: SQLTemporal if !isAggregation && !hasBucket =>
                return s"$param.isEqual($value) == false"
              case _ =>
            }
          case GE =>
            maybeValue.map(v => v.out).getOrElse(SQLTypes.Any) match {
              case SQLTypes.Varchar =>
                return s"$param.compareTo($value) >= 0"
              case _: SQLTemporal if !isAggregation && !hasBucket =>
                return s"$param.isBefore($value) == false"
              case _ =>
            }
          case LE =>
            maybeValue.map(v => v.out).getOrElse(SQLTypes.Any) match {
              case SQLTypes.Varchar =>
                return s"$param.compareTo($value) <= 0"
              case _: SQLTemporal if !isAggregation && !hasBucket =>
                return s"$param.isAfter($value) == false"
              case _ =>
            }
          case _ =>
        }
        s"$param $painlessOp $value"
      case _ => s"$param$painlessOp($value)"
    }
  }

  /** The rendering of an aggregate predicate for a bucket pipeline (`bucket_selector` for HAVING).
    * There is no document here, only the metrics Elasticsearch already computed, read as
    * `params.<metricName>` (issue #223: the old rendering read `doc[...]` and re-applied the
    * transform the metric aggregation had already applied). The whole predicate is ONE
    * parenthesised expression whose first act is to null-guard every metric it compares -- lead
    * directive, BIDC-2 AC 4b: no emitted Painless compares a possibly-null value unguarded. An
    * expression (not a `def left = ...;` statement) is what composes under the `&&` / `||` the
    * HAVING tree is joined with: `? :` binds looser than `||`, so an unparenthesised guard would
    * swallow the right-hand side of an OR. A temporal literal on the right is converted to epoch
    * millis, which is what a date metric arrives in.
    */
  protected def bucketPipelinePainless: String = {
    val param = identifier.metricParam
    operator match {
      // A null test IS the guard -- guarding it again would render the contradiction
      // `(p == null ? false : (p == null))`. Unreachable from SQL today (`IS NULL` takes a bare
      // name), kept total so a grammar widening cannot ship it.
      case IS_NULL     => s"$param == null"
      case IS_NOT_NULL => s"$param != null"
      case _ =>
        val metrics: Seq[Identifier] =
          identifier +: maybeValue.collect { case id: Identifier if id.isAggregation => id }.toSeq
        val guard = metrics.map(id => s"${id.metricParam} == null").mkString(" || ")
        s"($guard ? false : $painlessNot(${bucketPipelineCheck(param)}))"
    }
  }

  /** The comparison body of the bucket-pipeline rendering, `param` (= `params.<metric>`) against
    * the right-hand side. BETWEEN and IN, whose `painless` never goes through `check`, override it.
    */
  protected def bucketPipelineCheck(param: String): String = {
    val rhs = painlessValue(None)
    val value = maybeValue match {
      case Some(v) if operator.isInstanceOf[ComparisonOperator] && !v.isAggregation =>
        v.out match {
          case SQLTypes.Date => s"$rhs.truncatedTo(ChronoUnit.DAYS).toInstant().toEpochMilli()"
          case SQLTypes.Time => s"$rhs.truncatedTo(ChronoUnit.SECONDS).toInstant().toEpochMilli()"
          case SQLTypes.DateTime => s"$rhs.toInstant().toEpochMilli()"
          case _                 => rhs
        }
      case _ => rhs
    }
    check(None, param, value)
  }

  /** True when this predicate reads a bucket-pipeline metric: an aggregate, or the alias of a
    * SELECT `bucket_script` item (`MAX(x) - MIN(x) AS d ... HAVING d > 3` -- the alias is resolved
    * by `Having.resolveAggregateAliases`, so the identifier is the arithmetic wrapper carrying `d`
    * as its alias; Elasticsearch lets a `bucket_selector` read a sibling pipeline aggregation by
    * name).
    */
  def referencesBucketMetric: Boolean =
    identifier.isAggregation || (identifier.hasAggregation && identifier.fieldAlias.isDefined)

  override def painless(context: Option[PainlessContext]): String = {
    // A context-free rendering of an aggregate predicate is a bucket-pipeline rendering.
    if (context.isEmpty && referencesBucketMetric) return bucketPipelinePainless
    val innerLeft = left(context)
    context match {
      case Some(ctx) =>
        ctx.get(identifier) match {
          case Some(p) =>
            if (identifier.nullable)
              return s"$p == null ? false : $painlessNot(${check(context, p)})"
            else
              return s"$painlessNot(${check(context, p)})"
          case _ =>
        }
      case _ =>
    }
    // 🔴 Story BIDC-8 AD-9 — a left-hand side that can be NULL must be bound to a local so the
    // comparison lands INSIDE the guard, and the guard must collapse to `false` exactly once, at the
    // top of THIS predicate.
    //
    // MEASURED at the baseline: `WHERE UPPER(status) <> 'ZZZ'` emitted
    // `(param1 == null) ? null : param1.toUpperCase().compareTo("ZZZ") != 0` — the function's own
    // null guard with the comparison appended OUTSIDE it. `?:` binds loosest, so the ternary's
    // branches became `null` (Object) and a primitive `boolean`, and Elasticsearch refused the whole
    // query at COMPILE time: `script_exception / compile error`, caused by
    // `class_cast_exception: Cannot cast from [boolean] to [java.lang.Object]` (live ES 8.18.3,
    // data-independent — it fails on an empty index). The same held for the numeric family
    // (`ABS(amount) > 10`); the date family escaped it only because `YEAR(...)` folds into the
    // parameter assignment and reports `nullable`.
    //
    // The `false` collapse is ANSI-safe here because NOT is already folded into the operator at this
    // point (`painlessOp` uses `o.not`), so `NOT UPPER(x) = 'A'` over a MISSING field yields `false`
    // = no match, which is what three-valued logic requires (`NOT NULL` is NULL, not TRUE). It is
    // applied per predicate, never to a composite: `NULL OR true` stays true and `NULL AND true`
    // stays false under `||`/`&&` composition. Only a QUERY (filter) context collapses — script
    // fields, sorts, aggregations and ingest processors keep `null`, where it is a legitimate value
    // (story 21.8 Part C).
    // What must be guarded is a rendering that READS A DOCUMENT FIELD, since that is what can be
    // missing. MEASURED (probe over the real parser): a bare column reports `name = status`,
    // `nullable = true`, no dependencies; a function-wrapped column reports an EMPTY name,
    // `nullable = false` and `dependencies = [status]`; a literal (`CASE WHEN 1 = 1`, which parses
    // to an identifier with an empty name and a ONE-element function chain — story 20.9) reports
    // neither. So `nullable || dependencies.nonEmpty` is the discriminator: a `functions.nonEmpty`
    // test alone guards constants and broke `BooleanCastSpec`'s 21.8 restoration pin.
    val guardInQuery = context.exists(_.context == PainlessContextType.Query)
    val readsDocumentField = identifier.nullable || identifier.dependencies.nonEmpty
    if (guardInQuery && readsDocumentField) {
      // The local is declared in the script PROLOGUE, never inline: `def x = …;` is a STATEMENT and
      // an operand of a composed predicate must be a pure expression.
      val v = context.map(_.bindLocal(innerLeft)).getOrElse(innerLeft)
      return s"($v == null ? false : ($painlessNot(${check(context, v)})))"
    }
    if (identifier.nullable) {
      return s"def left = $innerLeft; left == null ? false : $painlessNot(${check(context, "left")})"
    }
    s"$painlessNot${check(context, innerLeft)}"
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
                    s"Type mismatch: '${identifier.out.typeId}' is not compatible with '${v.out.typeId}' in expression: $this"
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
  maybeNot: Option[NOT.type] = None
) extends Expression {
  override def maybeValue: Option[Token] = Option(value)

  override def update(request: SingleSearch): Criteria = {
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

case class Comparison(
  identifier: Identifier,
  operator: ComparisonOperator,
  value: PainlessScript,
  maybeNot: Option[NOT.type] = None
) extends Expression {
  override def maybeValue: Option[Token] = Option(value)

  override def update(request: SingleSearch): Criteria = {
    val updated = this.copy(identifier = identifier.update(request))
    if (updated.nested) {
      ElasticNested(updated, limit)
    } else
      updated
  }

  override def asFilter(currentQuery: Option[ElasticBoolQuery]): ElasticFilter = this
}

case class IsNullExpr(identifier: Identifier) extends Expression {
  override val operator: Operator = IS_NULL

  override def maybeValue: Option[Token] = None

  override def maybeNot: Option[NOT.type] = None

  override def update(request: SingleSearch): Criteria = {
    val updated = this.copy(identifier = identifier.update(request))
    if (updated.nested) {
      ElasticNested(updated, limit)
    } else
      updated
  }

  override def asFilter(currentQuery: Option[ElasticBoolQuery]): ElasticFilter = this
}

case class IsNotNullExpr(identifier: Identifier) extends Expression {
  override val operator: Operator = IS_NOT_NULL

  override def maybeValue: Option[Token] = None

  override def maybeNot: Option[NOT.type] = None

  override def update(request: SingleSearch): Criteria = {
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
  override def maybeNot: Option[NOT.type] = None
  override def asFilter(currentQuery: Option[ElasticBoolQuery]): ElasticFilter = this
  override val functions: List[Function] = List(conditionalFunction)
  override def sql: String = conditionalFunction.sql
}

object ConditionalFunctionAsCriteria {
  def unapply(f: ConditionalFunction[_]): Option[Criteria] = f match {
    case IsNull(id)    => Some(IsNullCriteria(id))
    case IsNotNull(id) => Some(IsNotNullCriteria(id))
    case _             => None
  }
}

case class IsNullCriteria(identifier: Identifier) extends CriteriaWithConditionalFunction[SQLAny] {
  override val conditionalFunction: ConditionalFunction[SQLAny] = IsNull(identifier)
  override val operator: Operator = IS_NULL
  override def update(request: SingleSearch): Criteria = {
    val updated = this.copy(identifier = identifier.update(request))
    if (updated.nested) {
      ElasticNested(updated, limit)
    } else
      updated
  }
  override def painless(context: Option[PainlessContext]): String = {
    context match {
      case Some(ctx) =>
        ctx.addParam(identifier) match {
          case Some(p) => return s"$p == null" // TODO check with not
          case _       =>
        }
      case _ =>
    }
    if (identifier.nullable) {
      return s"def left = ${left(context)}; left == null"
    }
    s"${left(context)} == null"
  }

}

case class IsNotNullCriteria(identifier: Identifier)
    extends CriteriaWithConditionalFunction[SQLAny] {
  override lazy val conditionalFunction: ConditionalFunction[SQLAny] = IsNotNull(
    identifier
  )
  override val operator: Operator = IS_NOT_NULL
  override def update(request: SingleSearch): Criteria = {
    val updated = this.copy(identifier = identifier.update(request))
    if (updated.nested) {
      ElasticNested(updated, limit)
    } else
      updated
  }

  override def painless(context: Option[PainlessContext]): String = {
    context match {
      case Some(ctx) =>
        ctx.addParam(identifier) match {
          case Some(p) => return s"$p != null" // TODO check with not
          case _       =>
        }
      case _ =>
    }
    if (identifier.nullable) {
      return s"def left = ${left(context)}; left != null"
    }
    s"${left(context)} != null"
  }

}

case class InExpr[R, +T <: Value[R]](
  identifier: Identifier,
  values: Values[R, T],
  maybeNot: Option[NOT.type] = None
) extends Expression { this: InExpr[R, T] =>
  private[this] lazy val id = functions.headOption match {
    case Some(f) => s"$f($identifier)"
    case _       => s"$identifier"
  }
  override def sql =
    s"$id $notAsString$operator $values"
  override def operator: Operator = IN
  override def update(request: SingleSearch): Criteria = {
    val updated = this.copy(identifier = identifier.update(request))
    if (updated.nested) {
      ElasticNested(updated, limit)
    } else
      updated
  }

  override def maybeValue: Option[Token] = Some(values)

  override def includes(
    bucket: Bucket,
    not: Boolean,
    bucketIncludesExcludes: BucketIncludesExcludes
  ): BucketIncludesExcludes = {
    identifier.bucket.find(_.name == bucket.name) match {
      case Some(_) =>
        if ((!not && maybeNot.isEmpty) || (not && maybeNot.isDefined))
          bucketIncludesExcludes.copy(values =
            bucketIncludesExcludes.values ++ values.values.map(_.sql.replaceAll("'", "")).toSet
          )
        else bucketIncludesExcludes
      case _ => bucketIncludesExcludes
    }
  }

  override def asFilter(currentQuery: Option[ElasticBoolQuery]): ElasticFilter = this

  // `<expr> IN (v1, v2)` compares the element with the list's ELEMENT type. The shared
  // Expression.validate compared `identifier.out` with the list's ARRAY type, which rejected
  // `COUNT(x) IN (1, 2)` (`BIGINT` vs `ARRAY<BIGINT>`) while the untyped `MAX(x) IN (…)` passed
  // through `Any` -- loud, but misleading.
  override def validate(): Either[String, Unit] =
    for {
      _ <- identifier.validate()
      _ <- values.validate()
      _ <- {
        val elementType = values.out match {
          case a: SQLArray => a.elementType
          case other       => other
        }
        Validator
          .validateTypesMatching(identifier.out, elementType)
          .left
          .map(_ =>
            s"Type mismatch: '${identifier.out.typeId}' is not compatible with '${elementType.typeId}' in expression: $this"
          )
      }
    } yield ()

  override def painless(context: Option[PainlessContext]): String = {
    if (context.isEmpty && referencesBucketMetric) return bucketPipelinePainless
    s"$painlessNot${identifier.painless(context)}$painlessOp(${painlessValue(context)})"
  }

  // `params.<metric> == v1 || params.<metric> == v2` -- the guarded bucket form of
  // `<aggregate> IN (v1, v2)`. NOT `[v1,v2].contains(p)`: a buckets_path value arrives as a boxed
  // Double and the literals are Integers, so `List.contains` (Java `equals`) never matched --
  // measured live, `MAX(age) IN (40, 50)` returned no bucket. Painless `==` promotes numerics and
  // uses `equals` for strings. IN is a ComparisonOperator, so `painlessNot` is "" (a comparison
  // folds its NOT into the operator, which this form never uses): the negation is rendered here.
  override protected def bucketPipelineCheck(param: String): String = {
    val membership = values.values.map(v => s"$param == ${v.painless(None)}").mkString(" || ")
    if (maybeNot.isDefined) s"!($membership)" else membership
  }

}

case class BetweenExpr(
  identifier: Identifier,
  fromTo: FromTo,
  maybeNot: Option[NOT.type]
) extends Expression {
  override def sql = s"$identifier $notAsString$operator $fromTo"
  override def operator: Operator = BETWEEN
  override def update(request: SingleSearch): Criteria = {
    val updated = this.copy(identifier = identifier.update(request))
    if (updated.nested) {
      ElasticNested(updated, limit)
    } else
      updated
  }

  override def maybeValue: Option[Token] = Some(fromTo)

  override def asFilter(currentQuery: Option[ElasticBoolQuery]): ElasticFilter = this

  override def validate(): Either[String, Unit] = {
    for {
      _ <- identifier.validate()
      _ <- fromTo.validate()
      _ <- Validator.validateTypesMatching(identifier.out, fromTo.out)
    } yield ()
  }

  override def painless(context: Option[PainlessContext]): String = {
    if (context.isEmpty && referencesBucketMetric) return bucketPipelinePainless
    context match {
      case Some(ctx) =>
        ctx.addParam(identifier) match {
          case Some(p) =>
            if (identifier.nullable)
              return s"$p == null ? false : $painlessNot($p >= ${fromTo.from} && $p <= ${fromTo.to})"
            else
              return s"$painlessNot($p >= ${fromTo.from} && $p <= ${fromTo.to})"
          case _ =>
        }
      case _ =>
    }
    if (identifier.nullable) {
      return s"def left = ${left(context)}; left == null ? false : $painlessNot(${fromTo.from} <= left <= ${fromTo.to})"
    }
    s"$painlessNot(${fromTo.from} <= ${left(context)} <= ${fromTo.to})"
  }

  // The guarded bucket form of `<aggregate> BETWEEN a AND b` -- two comparisons, not the chained
  // `a <= p <= b` the document form used (Painless rejects `boolean <= int`). BETWEEN is a
  // ComparisonOperator, so `painlessNot` is "": the negation is rendered here.
  override protected def bucketPipelineCheck(param: String): String = {
    val range = s"$param >= ${fromTo.from} && $param <= ${fromTo.to}"
    if (maybeNot.isDefined) s"!($range)" else range
  }

}

case class DistanceCriteria(
  distance: Distance,
  operator: ComparisonOperator,
  geoDistance: GeoDistance
) extends Expression {

  override def identifier: Identifier = Identifier(distance)

  override def sql = s"$distance $operator $geoDistance"

  override def update(request: SingleSearch): DistanceCriteria =
    this.copy(distance = distance.update(request))

  override def maybeValue: Option[Token] = Some(geoDistance)

  override def maybeNot: Option[NOT.type] = None

  override def asFilter(currentQuery: Option[ElasticBoolQuery]): ElasticFilter = this
}

case class MultiMatchCriteria(
  identifiers: Seq[Identifier],
  value: StringValue,
  nestedElement: Option[NestedElement] = None
) extends Criteria { // FIXME map to multi_match
  override def sql: String =
    s"$operator (${identifiers.mkString(",")}) $AGAINST ($value)"
  override def operator: Operator = MATCH
  override def update(request: SingleSearch): Criteria =
    this.copy(identifiers = identifiers.map(_.update(request)))

  override def dependencies: Seq[Identifier] = identifiers.flatMap(_.dependencies)

  override lazy val nested: Boolean = identifiers.forall(_.nested)

  @tailrec
  private[this] def toCriteria(matches: List[MatchCriteria], curr: Criteria): Criteria =
    matches match {
      case Nil           => curr
      case single :: Nil => Predicate(curr, OR, single)
      case first :: rest => toCriteria(rest, Predicate(curr, OR, first))
    }

  lazy val criteria: Criteria =
    (identifiers.map(id => MatchCriteria(id, value, None)) match {
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

case class MatchCriteria(
  identifier: Identifier,
  value: StringValue,
  options: Option[String]
) extends Expression {
  override def sql: String =
    s"$operator($identifier,$value${options.map(o => s""","$o"""").getOrElse("")})"
  override def operator: Operator = MATCH
  override def update(request: SingleSearch): Criteria =
    this.copy(identifier = identifier.update(request))

  override def maybeValue: Option[Token] = Some(value)

  override def maybeNot: Option[NOT.type] = None

  override def asFilter(currentQuery: Option[ElasticBoolQuery]): ElasticFilter = this

  override def includes(
    bucket: Bucket,
    not: Boolean,
    bucketIncludesExcludes: BucketIncludesExcludes
  ): BucketIncludesExcludes = {
    identifier.bucket.find(_.name == bucket.name) match {
      case Some(_) =>
        if ((!not && maybeNot.isEmpty) || (not && maybeNot.isDefined))
          // The value, not its rendering — see the include/exclude note above.
          bucketIncludesExcludes.copy(regex =
            bucketIncludesExcludes.regex.orElse(Option(value.value.toString))
          )
        else bucketIncludesExcludes
      case _ => bucketIncludesExcludes
    }
  }

  override def matchCriteria: Boolean = true

  override def painless(context: Option[PainlessContext]): String =
    s"$painlessNot${identifier.painless(context)}$painlessOp(${painlessValue(context)})"

}

sealed abstract class ElasticRelation(val criteria: Criteria, val operator: ElasticOperator)
    extends Criteria
    with ElasticFilter {
  override def sql = s"$operator($criteria)"

  private[this] def rtype(criteria: Criteria): Option[String] = criteria match {
    case Predicate(left, _, right, _, _) => rtype(left).orElse(rtype(right))
    case c: Expression =>
      c.identifier.nestedElement
        .map(_.innerHitsName)
        .orElse(c.identifier.name.split('.').headOption)
    case relation: ElasticRelation => relation.relationType
    case _                         => None
  }

  lazy val relationType: Option[String] = rtype(criteria)

  override def asFilter(currentQuery: Option[ElasticBoolQuery]): ElasticFilter = this

  override def group: Boolean = criteria.group

}

case class ElasticNested(
  override val criteria: Criteria,
  override val limit: Option[Limit],
  fromCriteria: Boolean = true
) extends ElasticRelation(criteria, Nested) {
  override def sql: String =
    if (!fromCriteria) s"$operator($criteria)"
    else s"$criteria"

  def nestedElement: Option[NestedElement] = None

  override def update(request: SingleSearch): ElasticNested =
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

case class ElasticChild(
  override val criteria: Criteria
) extends ElasticRelation(criteria, Child) {
  def nestedElement: Option[NestedElement] = None
  override def update(request: SingleSearch): ElasticChild =
    this.copy(criteria = criteria.update(request))
}

case class ElasticParent(
  override val criteria: Criteria
) extends ElasticRelation(criteria, Parent) {
  def nestedElement: Option[NestedElement] = None
  override def update(request: SingleSearch): ElasticParent =
    this.copy(criteria = criteria.update(request))
}

case class Where(criteria: Option[Criteria]) extends Updateable {
  override def sql: String = criteria match {
    case Some(c) => s" $Where $c"
    case _       => ""
  }
  def update(request: SingleSearch): Where =
    this.copy(criteria = criteria.map(_.update(request)))

  override def validate(): Either[String, Unit] = criteria match {
    case Some(c) =>
      // An aggregate has no document-level query form: the bridge rendered it as `match_all` and
      // the predicate was silently DROPPED -- a wrong group set for a SELECT, EVERY document for a
      // DELETE or UPDATE (#280 family). Checked here, not in SingleSearch.validate(), so every
      // statement kind that carries a WHERE is covered: SELECT, DELETE, UPDATE and CREATE ENRICH
      // POLICY's source WHERE. Lead-confirmed 2026-09-06 (reject, rather than rewriting it as a
      // HAVING under a GROUP BY).
      c.extractAggregationFields.headOption match {
        case Some(f) =>
          Left(
            s"Aggregate functions are not allowed in WHERE (found ${f.identifier.sql}); use HAVING"
          )
        case None => c.validate()
      }
    case _ => Right(())
  }

  def nestedElements: Seq[NestedElement] = criteria match {
    case Some(c) => c.nestedElements.groupBy(_.path).map(_._2.head).toList
    case _       => Nil
  }
}
