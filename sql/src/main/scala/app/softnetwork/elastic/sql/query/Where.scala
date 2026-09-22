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
  SQLNumeric,
  SQLTemporal,
  SQLType,
  SQLTypeUtils,
  SQLTypes
}
import app.softnetwork.elastic.sql.function._
import app.softnetwork.elastic.sql.function.cond.{ConditionalFunction, IsNotNull, IsNull}
import app.softnetwork.elastic.sql.function.convert.Conversion
import app.softnetwork.elastic.sql.function.geo.Distance
import app.softnetwork.elastic.sql.parser.Validator
import app.softnetwork.elastic.sql.operator._
import app.softnetwork.elastic.sql._

import scala.annotation.tailrec

case object Where extends Expr("WHERE") with TokenRegex

sealed trait Criteria extends Updateable with PainlessScript {
  def operator: Operator

  /** This criterion with its own `NOT` flipped, when it can carry one.
    *
    * 🔴 Story BIDC-8 (review B-2/M-5). `WhereParser.predicate` is `criteria ~ (and|or) ~ not.? ~
    * criteria`, so a `NOT` written AFTER a predicate operator lands on the PREDICATE, and the
    * predicate used to emit it two different ways: `asFilter` wrapped the UN-negated right
    * criterion in an Elasticsearch `must_not` (which MATCHES a document that lacks the field,
    * inverting the per-predicate `false` collapse), while `painless` rendered `!(left && right)`
    * (which negates the WHOLE composite). MEASURED on live ES 8.18.3 over docs A / B /
    * field-absent: `WHERE NOT UPPER(x) = 'A' AND a = 1` returned `[2]` while the mirror order
    * `WHERE a = 1 AND NOT UPPER(x) = 'A'` returned `[2, 3]` — the same predicate, different rows by
    * writing order. Folding the NOT into the criterion it actually qualifies makes both paths agree
    * and puts the negation where `check` can fold it into the operator.
    */
  def negated: Option[Criteria] = None

  /** Comparisons whose two operands render `java.time` types that ANSI does not compare — today
    * exactly TIME against a date-carrying temporal (issue #384).
    *
    * 🔴 Asked of a SCHEMA-RESOLVED statement and of nothing else. Without a schema a bare column's
    * type is `Any`, so `CAST(ts AS TIME) > ts` is a TIME against an UNKNOWN and there is nothing to
    * reject — MEASURED: `Parser` validates the unresolved AST, where every such comparison is
    * indistinguishable from a legal one. That is why this is a separate method rather than an arm
    * of `validate()`: `validate()` runs at parse time, where the answer is not knowable yet.
    *
    * Recursive in ONE place, the way `dependencies` above already is, so a new `Criteria` subtype
    * cannot silently opt out of it.
    */
  def temporalComparisonErrors: Seq[String] = this match {
    case Predicate(left, _, right, _, _) =>
      left.temporalComparisonErrors ++ right.temporalComparisonErrors
    case e: Expression             => e.temporalComparisonError.toSeq
    case relation: ElasticRelation => relation.criteria.temporalComparisonErrors
    case _                         => Nil
  }

  def dependencies: Seq[Identifier] = this match {
    case Predicate(left, _, right, _, _) => left.dependencies ++ right.dependencies
    case c: Expression                   => c.dependencies
    case relation: ElasticRelation       => relation.criteria.dependencies
    case m: MultiMatchCriteria           => m.dependencies
    // Story 22.2 — a subquery node's LEFT operand is an ordinary outer identifier and must not be
    // hidden by the `case _` default below.
    case s: SubqueryCriteria => s.outerIdentifiers.flatMap(_.dependencies)
    case _                   => Nil
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
    // Story 22.2 — the OUTER operand only. A subquery's own body is a separate scope; walking into
    // it here would make `derivedScopeCheck` and `SubqueryScope` read inner columns as outer ones.
    case s: SubqueryCriteria => s.outerIdentifiers
    case _                   => Nil
  }

  /** Every WHERE-subquery node of this criteria tree, in statement order (story 22.2).
    *
    * Relation wrappers and predicates recurse; a subquery's OWN body is NOT walked here — an inner
    * subquery is resolved when the inner statement crosses the seam itself (`api.search(inner)` ->
    * `resolveWithSchema(inner)`), which is what makes the recursion depth-unbounded without this
    * walker having to know about it.
    */
  def subqueries: Seq[SubqueryCriteria] = this match {
    case Predicate(left, _, right, _, _) => left.subqueries ++ right.subqueries
    case relation: ElasticRelation       => relation.criteria.subqueries
    case s: SubqueryCriteria             => Seq(s)
    case _                               => Nil
  }

  /** Story 22.5 -- every statement this criteria EMBEDS, in statement order.
    *
    * DERIVED from [[subqueries]] rather than written as a second `this match`: the two would be arm
    * lists over the SAME sealed hierarchy and could silently disagree about where a statement can
    * hide -- the one-key-two-derivations drift story 21.3 paid for four times. `subqueries` already
    * recurses through `Predicate` and the three `ElasticRelation`s and stops AT a subquery node
    * (its own body is a separate scope, walked by whoever owns that scope), which is exactly the
    * boundary [[CteSubstitution.referencedNames]] wants.
    */
  final def embeddedStatements: Seq[DqlStatement] = subqueries.map(_.query)

  /** The WRITE half of [[embeddedStatements]]: rebuild this criteria with `f` applied to every
    * statement it embeds. `eq`-preserving -- a subtree in which nothing changed is returned AS IS,
    * which is what makes [[CteSubstitution]] 's "never touch a body that references no CTE" test
    * meaningful.
    *
    * The structural arms are the same three shapes `subqueries` recurses through; the leaf arm
    * delegates to [[SubqueryCriteria.withQuery]], which is ABSTRACT.
    *
    * 🔴 What that buys, stated exactly: a new `SubqueryCriteria` cannot forget to take part -- the
    * compiler refuses it. It does NOT extend to a new `Criteria` subtype that is not a
    * `SubqueryCriteria`; such a type falls to `case _ => this` SILENTLY, and if it ever carries a
    * statement the failure mode is a CTE name inside `IN (SELECT ... FROM cte)` read as an INDEX
    * (`index_not_found` when absent, a wrong answer with HTTP 200 when an index of that name
    * exists). `Criteria` is sealed, so that would be an edit to THIS file -- which is the whole of
    * the protection, and it is a convention rather than a compiler guarantee.
    */
  def mapEmbeddedStatements(f: DqlStatement => DqlStatement): Criteria = this match {
    case p: Predicate =>
      val l = p.leftCriteria.mapEmbeddedStatements(f)
      val r = p.rightCriteria.mapEmbeddedStatements(f)
      if ((l eq p.leftCriteria) && (r eq p.rightCriteria)) p
      else p.copy(leftCriteria = l, rightCriteria = r)
    case n: ElasticNested =>
      val c = n.criteria.mapEmbeddedStatements(f)
      if (c eq n.criteria) n else n.copy(criteria = c)
    case c: ElasticChild =>
      val x = c.criteria.mapEmbeddedStatements(f)
      if (x eq c.criteria) c else c.copy(criteria = x)
    case p: ElasticParent =>
      val x = p.criteria.mapEmbeddedStatements(f)
      if (x eq p.criteria) p else p.copy(criteria = x)
    case s: SubqueryCriteria =>
      val q = f(s.query)
      if (q eq s.query) s else s.withQuery(q)
    case _ => this
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
      // Story 22.2 — so `COUNT(x) IN (SELECT …)` is rejected by `Where.validate` exactly like any
      // other aggregate written in a WHERE clause.
      case s: SubqueryCriteria =>
        s.outerIdentifiers
          .filter(_.aggregations.nonEmpty)
          .flatMap(id => id.metricName.map(name => Field(id, Some(Alias(name)))))
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
    case p @ Predicate(left, op, _, maybeNot, group) =>
      // The same fold as `asFilter` (B-2): `A AND NOT B` renders as `A && <negated B>`, never as
      // `!(A && B)` — which negates the whole composite and, for an absent field, flipped a CASE
      // condition into its THEN branch where ANSI takes ELSE (review B-2 C2, measured).
      val right = p.emittedRight
      val notHandled = maybeNot.isEmpty || p.notConsumed
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
      val not = maybeNot.nonEmpty && !notHandled
      if (group || not)
        s"${if (not) maybeNot.map(_.painless(context)).getOrElse("") else ""}($leftStr $opStr $rightStr)"
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

  /** The right criterion as it must be EMITTED, with this predicate's own `NOT` folded into it when
    * the criterion can carry one (story BIDC-8, review B-2). `sql` keeps rendering the `NOT` where
    * the user wrote it; only emission changes. When the criterion cannot carry a `NOT` (a nested
    * group, a relation) the old routing stands and `notConsumed` stays false, so nothing silently
    * changes shape.
    */
  /** 🔴 EVERY CONSUMER OF `Predicate.not`, ENUMERATED (round 10, LOW-1). Nothing listed them, which
    * is why BLOCKING-1 (a missing `negated` override) and LOW-1 (a site still reading
    * `rightCriteria` + `not`) both slipped through a round that was specifically about this fold:
    *
    *   1. `Criteria.painless`'s `Predicate` arm (`Where.scala`, ~:227) — the script form;
    *   1. `Predicate.asFilter` (just below) — the query-DSL form;
    *   1. `MetricSelectorScript.metricSelector` (`GroupBy.scala`, ~:378) — HAVING, via
    *      `right.negated`; it kept its OWN copy of the negation table until round 10 collapsed it
    *      into `Criteria.negated`, and that copy already had the `BetweenExpr` arm the shared one
    *      was missing — two tables, one right, one wrong;
    *   1. `ElasticBridge`'s nested/relation arm (`ElasticBridge.scala`, ~:135, and the es6 copy) —
    *      which deliberately does NOT fold, and that is the point of enumerating them.
    *
    * 🔴 THE FOLD IS NOT UNIFORM, and round 10 MEASURED why. It is valid exactly where the negated
    * criterion is evaluated over the SAME document. Under a NESTED relation the criterion runs over
    * a CHILD document inside an EXISTENTIAL, so `NOT` outside and `NOT` inside ask different
    * questions: `AND NOT replies.lastUpdated < d` means "no reply is before d" (a blog with no
    * replies qualifies), while the folded form asks "some reply is not before d" (it does not).
    * Aligning that fourth site by folding was tried and reverted; `SQLQuerySpec`'s "predicate with
    * distinct nested" fixture is what caught the flip, turning `must_not[nested] + filter` into
    * `must[nested, nested(… == false)]`.
    *
    * So the first three fold and the fourth does not, each for a stated reason.
    * `PainlessNullSurvivalSpec`'s source scan fails if a consumer appears that states NEITHER —
    * silence is the failure mode both round-10 defects had in common.
    */
  private[query] lazy val (emittedRight: Criteria, notConsumed: Boolean) =
    not match {
      case Some(_) => rightCriteria.negated.map(_ -> true).getOrElse(rightCriteria -> false)
      case None    => rightCriteria -> false
    }

  override def asFilter(currentQuery: Option[ElasticBoolQuery]): ElasticFilter = {
    val query = asBoolQuery(currentQuery)
    // A folded NOT is part of the right criterion now, so it must NOT be wrapped in `must_not`:
    // `must_not` over a script MATCHES documents the script rejects, including those that lack the
    // field, which is the opposite of the three-valued reading the criterion itself emits (B-2).
    val negate = not.isDefined && !notConsumed
    operator match {
      case AND =>
        (if (negate) query.not(emittedRight.asFilter(Option(query)))
         else query.filter(emittedRight.asFilter(Option(query))))
          .filter(leftCriteria.asFilter(Option(query)))
      case OR =>
        (if (negate) query.not(emittedRight.asFilter(Option(query)))
         else query.should(emittedRight.asFilter(Option(query))))
          .should(leftCriteria.asFilter(Option(query)))
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

  /** This predicate's operator with its own `NOT` folded in, where the operator set HAS a negated
    * spelling for it. `NOT x = 1` becomes `x != 1`; `NOT x LIKE 'a%'` stays `LIKE` and the negation
    * is rendered by [[painlessNot]] instead (review M-6). One derivation, so `check`, `painlessOp`
    * and the bridge cannot disagree about which operator is actually being emitted.
    */
  protected def effectiveOperator: Operator = operator match {
    case o: ComparisonOperator if maybeNot.isDefined => o.maybeNegated.getOrElse(o)
    case o                                           => o
  }

  /** The `!` to render in front of the check, if any.
    *
    * A comparison normally folds its NOT into the operator, so there is nothing left to render --
    * UNLESS the operator has no negated spelling (`LIKE`, `RLIKE`, `IN`, `BETWEEN`, `MATCH`), in
    * which case the whole check is negated here. The `!` lands INSIDE the null guard `painless`
    * emits, so an absent field still collapses to `false` instead of being inverted into a match:
    * ANSI three-valued logic, not `must_not` semantics.
    */
  def painlessNot: String = operator match {
    case o: ComparisonOperator if o.maybeNegated.isDefined => ""
    case _ => maybeNot.map(_.painless(None)).getOrElse("")
  }

  def painlessOp: String = effectiveOperator.painless(None)

  def painlessValue(context: Option[PainlessContext]): String =
    maybeValue.map(painlessOperand(_, context)).getOrElse("")

  /** One operand's Painless rendering. A `Token` that is not a [[PainlessScript]] has only its SQL
    * spelling to offer -- which is why `BetweenExpr` must go through this and not `toString`: a
    * `StringValue` renders `'A'` in SQL and `"A"` in Painless, and the SQL form is a syntax error
    * in a script.
    */
  protected def painlessOperand(token: Token, context: Option[PainlessContext]): String =
    token match {
      case v: PainlessScript => v.painless(context)
      case v                 => v.sql
    }

  /** `<param> LIKE '<pattern>'` (or RLIKE) as Painless.
    *
    * 🔴 Story BIDC-8 (review L-10). The generic `$param $painlessOp $value` fallback emitted `left1
    * .matches "A%"`, which is not Painless at all -- MEASURED on live ES 8.18.3: `invalid sequence
    * of tokens near ['"A%"']`. Two further spellings were MEASURED and REFUTED before this one:
    * `left1.matches("A.*")` gives `dynamic method [java.lang.String, matches/1] not found`, and
    * `java.util.regex.Pattern.compile("A.*").matcher(left1).matches()` gives `static method
    * [java.util.regex.Pattern, compile/1] not found` -- Painless deliberately whitelists NO
    * `Pattern.compile`, so a regex may only enter through a `/.../` literal. (⚠️ That refutation
    * also condemns `REGEXP_LIKE`'s emission at `function/string/package.scala:469`, which uses
    * `Pattern.compile` -- PRE-EXISTING, untouched here, recorded for the lead.)
    *
    * So the common patterns -- which is what a BI tool emits -- decompose into whitelisted `String`
    * methods, and only a pattern that genuinely needs a regex falls back to the literal. The regex
    * itself comes from the SHARED `toRegex`, the same translation the query-DSL path feeds to
    * `regexQuery` (`bridge/package.scala:825-827`), so the scripted and non-scripted spellings of
    * one SQL pattern read the same PATTERN -- a claim that was false until round 10 escaped the
    * regex metacharacters there. ⚠️ It is a claim about the pattern, not about the whole predicate:
    * on a MULTI-VALUED field the two routes still differ, because the native query matches if ANY
    * value matches while the script reads `doc['x'].value`. That is orthogonal to this method and
    * is recorded, not fixed.
    *
    * 🔴 THE EXACT RULE, because "only `%` patterns are safe" was MEASURED WRONG (round 10, HIGH-1):
    * the string-method fast path is taken when the pattern has NO `_` and `%` appears ONLY at the
    * ends. Everything else compiles to a Painless regex literal -- including patterns made only of
    * `%`, such as `'A%B'`. A regex literal needs `script.painless.regex.enabled`, which stock **ES
    * 6.8** leaves at `false`: there, `UPPER(status) LIKE 'A%B'` answers `illegal_state_exception:
    * Regexes are disabled` while `LIKE 'A%'` returns 200 on the same cluster.
    *
    * 📌 FUTURE IMPROVEMENT (recorded, not a defect; no issue filed). This decomposition is reached
    * ONLY for a function-wrapped identifier. A plain `LIKE` / `RLIKE` / `NOT LIKE` never enters
    * Painless at all -- it becomes a native `regexp` query (`bridge/package.scala:825-827`) -- and
    * the script path is chosen at `bridge/package.scala:657-697` (the identifier carries functions,
    * the single geo-`Distance` case excepted). For the case-folding functions, `UPPER(x)` /
    * `LOWER(x)` over a PLAIN column, the predicate is really a case-insensitive match, and
    * Elasticsearch has supported `case_insensitive: true` on `regexp` and `wildcard` queries since
    * **7.10**: that would be native, index-accelerated and free of the Painless whitelist entirely.
    * It is not available on ES 6.8, which would still need this decomposition or a loud rejection.
    * And it replaces nothing: a genuinely computed operand (`SUBSTRING(x, 1, 3) LIKE 'AB%'`,
    * `CONCAT`, `TRIM`, arithmetic) cannot be expressed as a native query at all, so the scripted
    * path and this decomposition are required for the general case. The improvement is an
    * optimisation for a recognisable subset.
    */
  private def likePainless(param: String, rawPattern: String, sqlWildcards: Boolean): String = {
    def lit(value: String): String = s""""${escapePainlessString(value)}""""
    // 🔴 Round 11 (M-2). `%%` means exactly what `%` means, but the fast-path test strips only ONE
    // leading and ONE trailing `%`, so `'%%A'` fell through to a regex — and the rule this method
    // documents ("no `_`, `%` only at the ends") was then false as written. MEASURED on live ES
    // 8.18.3, `UPPER(status) LIKE '%%A'` answered
    // `circuit_breaking_exception: Regular expression considered too many characters`, and on 6.8 it
    // is `Regexes are disabled`. Collapsing the runs first makes the documented rule TRUE and keeps
    // more patterns off the regex path on every version. The collapse is identity for LIKE.
    val pattern = if (sqlWildcards) rawPattern.replaceAll("%+", "%") else rawPattern
    if (sqlWildcards && !pattern.contains("_")) {
      val core = pattern.stripPrefix("%").stripSuffix("%")
      // 🔴 Round 10 (MEDIUM-1): NO `core.nonEmpty` precondition. `LIKE ''` fell through to the
      // regex branch and emitted `left1 ==~ //`, where `//` opens a Painless COMMENT —
      // `unexpected character [//))))]` on live ES 8.18.3, while the native `status LIKE ''`
      // answers `[]`. An empty core is meaningful in all four shapes: `''` is `equals("")`,
      // `'%'` is `endsWith("")` and `'%%'` is `contains("")`, both true for any non-null value.
      if (!core.contains("%")) {
        val leading = pattern.startsWith("%")
        val trailing = pattern.endsWith("%") && pattern.length > 1
        return (leading, trailing) match {
          case (false, false) => s"$param.equals(${lit(core)})"
          case (false, true)  => s"$param.startsWith(${lit(core)})"
          case (true, false)  => s"$param.endsWith(${lit(core)})"
          case (true, true)   => s"$param.contains(${lit(core)})"
        }
      }
    }
    val regex = if (sqlWildcards) toRegex(pattern) else pattern
    s"($param ==~ /${regex.replace("/", "\\/")}/)"
  } /*{
      operator match {
        case IsNull | IsNotNull => "null"
        case _                  => ""
      }
    }*/

  /** True when the right-hand side READS A DOCUMENT FIELD, i.e. renders a doc-value rather than a
    * literal.
    *
    * 🔴 It decides whether the narrowing below may fire. `.toLocalDate()` exists so that a
    * comparison against a DATE LITERAL types: the literal renders as `LocalDate.parse(…)`, so the
    * doc-value has to be narrowed to meet it. A date COLUMN on the other side renders a
    * `ZonedDateTime` and gets no such step, so narrowing only the left operand made the two sides
    * different Java types — MEASURED on ES 8.18, `WHERE LAST_DAY(d) = LAST_DAY(ts)` answered
    * `class_cast_exception: Cannot cast java.time.ZonedDateTime to
    * java.time.chrono.ChronoLocalDate`. Both sides or neither; a document field on the right means
    * neither.
    */
  protected def valueReadsDocumentField: Boolean =
    maybeValue.exists {
      case id: Identifier => id.name.trim.nonEmpty
      case _              => false
    }

  /** The type of the value the RIGHT-hand side RENDERS — the same question `Identifier.chainType`
    * answers for the left operand, asked on the right.
    *
    * 🔴 Issue #367, second half. `out` on a function-wrapped identifier is its COLUMN's type, so a
    * predicate with a function on BOTH sides chose its comparison from the wrong one and coerced
    * the left operand towards the wrong target. MEASURED on ES 8.18:
    *
    *   - `WHERE YEAR(d) = YEAR(ts)` emitted `left1.isEqual(param2)` over two `int`s and answered
    *     `dynamic method [java.lang.Integer, isEqual/1] not found`;
    *   - `WHERE DATE_FORMAT(d,'yyyy') = DATE_FORMAT(ts,'yyyy')` did the same over two `String`s;
    *   - `WHERE CAST(num AS INT) = CAST(other AS INT)` wrapped the LEFT in `String.valueOf(…)` —
    *     because the common supertype of INT and the right-hand COLUMN's KEYWORD is a string — and
    *     compared `"7"` with `7`: zero rows, HTTP 200, for documents that match.
    *
    * The fix that made the left operand honest is the fix here, at the two places that read the
    * right-hand type: this predicate's target type, and `check`'s per-type dispatch.
    */
  protected def valueType: SQLType =
    maybeValue match {
      case Some(id: Identifier)    => id.chainType
      case Some(v: PainlessScript) => v.out
      case Some(other)             => other.out
      case None                    => SQLTypes.Any
    }

  /** The JAVA type the RIGHT-hand side renders — what `Identifier.renderedType` answers for the
    * left operand, asked on the right (issue #384).
    *
    * [[valueType]] stays the SQL type because that is what `check`'s per-type dispatch needs: it
    * picks the comparison SPELLING (`isBefore` / `compareTo` / `==`), which follows the SQL type.
    * This one decides what the two sides must be CONVERTED to, which follows the Java type, and the
    * two questions had one answer for as long as only the left operand was ever converted.
    *
    * A non-identifier value is its own producer — a DATE literal renders `LocalDate.parse(…)` — so
    * `out` is already the Java type for it.
    */
  protected def valueRenderedType: SQLType =
    maybeValue match {
      case Some(id: Identifier) => id.renderedType
      case _                    => valueType
    }

  /** Can the doc-value of this identifier be a TEMPORAL at run time? `Any` means the column's type
    * is UNKNOWN — no schema was attached — and the narrowing below has always been applied there,
    * which the no-schema bridge fixtures pin; only a column KNOWN to be something else may skip it.
    * Issue #367: "not known to be temporal" and "known not to be temporal" are different answers,
    * and confusing them dropped `.toLocalDate()` from three fixtures that need it.
    */
  private def mayBeTemporal(identifier: Identifier): Boolean =
    identifier.baseType == SQLTypes.Any || identifier.baseType.isInstanceOf[SQLTemporal]

  /** The JAVA type the LEFT operand renders — `Identifier.renderedType`, plus the one thing that
    * derivation cannot see (issue #384).
    *
    * 🔴 [[leftOperand]] INJECTS a narrowing onto the operand when the right-hand side is a DATE (or
    * TIME) LITERAL: the literal renders `LocalDate.parse(…)`, so the doc-value is given
    * `.toLocalDate()` to meet it. That injection happens AFTER the type is computed and changes
    * what the operand renders, so asking the chain alone answers TIMESTAMP for something that
    * renders a `LocalDate`.
    *
    * Getting this wrong is not theoretical: the first version of this fix promoted the LITERAL of
    * `DATE_ADD(d, INTERVAL 1 DAY) = CAST('2025-01-31' AS DATE)` to a `ZonedDateTime` while the
    * narrowed operand stayed a `LocalDate` — caught by `PredicateTransformSurvivalSpec`'s interval
    * pin, which exists for exactly this class of mistake.
    *
    * The condition is the injection's own, stated once so the two cannot drift.
    */
  protected def operandRenderedType: SQLType =
    if (narrowsToValueType) valueType else identifier.renderedType

  /** Will [[leftOperand]] narrow the operand to the right-hand side's DATE/TIME type? The same
    * predicate the injection uses — `originalType == Any` says this identifier NAMES a column, the
    * column may be temporal, and the right side is a LITERAL (a document field on the other side
    * renders a `ZonedDateTime` of its own and gets no narrowing: both sides or neither).
    */
  private def narrowsToValueType: Boolean =
    identifier.originalType == SQLTypes.Any && mayBeTemporal(identifier) &&
    !valueReadsDocumentField &&
    (valueType == SQLTypes.Date || valueType == SQLTypes.Time)

  /** The type BOTH operands are reconciled to before they are compared.
    *
    * 🔴 Extracted so there is ONE answer (issue #384). It used to be computed inside
    * [[leftOperand]], where only the left operand could see it, and the right one was rendered raw
    * by [[painlessValue]] — which is the whole defect: `ts > CAST(d AS DATE)` promoted the side
    * that was already a `ZonedDateTime` and left the `LocalDate` alone.
    */
  protected def comparisonTargetType: SQLType = {
    val operandType = operandRenderedType
    maybeValue match {
      // 🔴 Two numbers are compared as numbers, with NO widening. Painless compares boxed numerics
      // numerically (MEASURED on ES 8.18 for `==`, `>`, `contains` and a BETWEEN pair, across
      // Integer/long/double mixes), and the parameter shortcut in [[painless]] has always emitted
      // the comparison with no conversion at all — so skipping it here is what keeps the two paths
      // agreeing rather than a shortcut of its own.
      //
      // It is also the difference between working and a 400: a widening renders as the PRIMITIVE
      // cast `((long) <operand>)`, and the operand of a function chain is a null-guarded ternary,
      // which Painless types as `Object` — `CAST(num AS INT) = 9` compiled to
      // `(def)(((long) (param1 != null ? … : null)))` and Elasticsearch answered `script_exception:
      // compile error` (MEASURED). That is the same unification rule as story 21.8's
      // method-inside-the-guard: a conversion may not wrap a guarded ternary.
      case Some(_) if operandType.isInstanceOf[SQLNumeric] && valueType.isInstanceOf[SQLNumeric] =>
        operandType
      // 🔴 Issue #384 — the common super type is computed from what BOTH sides RENDER. Reading the
      // right side's SQL type here made `CAST(d AS DATE) > DATE_TRUNC(ts, MONTH)` ask for
      // `leastCommonSuperType(DATE, TEMPORAL)`, which is DATE, and a DATE target then told the
      // left operand it was already there — while the right was rendering a `ZonedDateTime`.
      case Some(_) => SQLTypeUtils.leastCommonSuperType(List(operandType, valueRenderedType))
      case None    => operandType
    }
  }

  /** ANSI SQL defines no comparison between a TIME and a date-carrying temporal: a `LocalTime` has
    * no date, so promoting it to a `ZonedDateTime` would have to invent one (issue #384, lead
    * decision D-2 — reject, do not guess).
    *
    * Without this the two sides reach Painless as different types and Elasticsearch fails the shard
    * with `Cannot cast java.time.ZonedDateTime to java.time.LocalTime` — loud, but naming neither
    * the column nor the SQL that produced it. `SQLTypeUtils.coerce` has no `(Time, _)` SOURCE arm
    * and is not given one: there is no correct value to emit.
    *
    * ⚠️ TIME against TIME is legal and untouched — `CAST(ts AS TIME) = CAST('07:00:00' AS TIME)`
    * compares two `LocalTime`s and works.
    *
    * ⚠️ Comparison operators only. `BETWEEN` and `IN` carry a pair / a list rather than one
    * comparable operand, and `BETWEEN` belongs to issue #381.
    */
  def temporalComparisonError: Option[String] = {
    def dateCarrying(t: SQLType): Boolean =
      t == SQLTypes.Date || t == SQLTypes.DateTime || t == SQLTypes.Timestamp
    val l = operandRenderedType
    val r = valueRenderedType
    if (!operator.isInstanceOf[ComparisonOperator]) None
    else if ((l == SQLTypes.Time && dateCarrying(r)) || (r == SQLTypes.Time && dateCarrying(l)))
      Some(
        s"Type mismatch: a TIME value cannot be compared with a ${if (l == SQLTypes.Time) r.typeId
        else l.typeId} value in expression: $this"
      )
    else None
  }

  /** Do the two operands render DIFFERENT `java.time` types, so that the right one has to be moved
    * to meet the left (issue #384)?
    *
    * 🔴 NEVER in a PROCESSOR context, and this is not a precaution — it is a measured regression
    * the first version shipped. In an ingest script the operand is not a doc-value: a DATE column
    * renders `(ctx.d instanceof String ? LocalDate.parse(…).atStartOfDay(Z) : Instant.ofEpochMilli(
    * ctx.d).atZone(Z))`, already a `ZonedDateTime`, so `.atStartOfDay(…)` is `dynamic method
    * [java.time.ZonedDateTime, atStartOfDay/1] not found` — #368's rule again. And the failure is
    * INVISIBLE: `ScriptProcessor` sets `ignore_failure = true`, so `CREATE TABLE t (… c INT SCRIPT
    * AS (CASE WHEN ts > CAST(d AS DATE) THEN 1 ELSE 0 END))` indexed documents with no `c` at all.
    * MEASURED on real ES 8.18.3 through the DDL path, for both the string and the epoch-millis
    * document shapes. The LEFT operand has always had this guard (it routes through
    * `processorTemporal` rather than `coerce`); the right one needs the same.
    *
    * ⚠️ The TRANSFORM (materialized-view) context is NOT excluded and must not be: it reads
    * `doc['x'].value`, so its operands are the same `java.time` objects a query's are.
    *
    * 🔴 TEMPORAL mismatches only (lead decision D-3). Full symmetry would also convert string and
    * numeric right-hand sides, which are compared today with no conversion on either side; keeping
    * the condition here is what makes "every non-temporal predicate is byte-identical" a property
    * of the code rather than a claim about a test run.
    *
    * ⚠️ It says the two RENDERINGS disagree, not that `coerce` has an arm for the pair. Where it
    * has none the promotion is the identity and only the binding below remains — visible today only
    * for a TIME source, which [[temporalComparisonError]] refuses before it can execute.
    */
  protected def rightNeedsTemporalPromotion(context: Option[PainlessContext]): Boolean = {
    val from = valueRenderedType
    val to = comparisonTargetType
    from != to && from.isInstanceOf[SQLTemporal] && to.isInstanceOf[SQLTemporal] &&
    !context.exists(_.isProcessor)
  }

  /** The RIGHT operand, promoted to [[comparisonTargetType]] (issue #384).
    *
    * ⚠️ NOT applied inside `check`: `bucketPipelineCheck` calls that with `params.<metric>`
    * operands, which are epoch-millis doubles and have no `java.time` type to reconcile. The five
    * call sites are all in [[painless]].
    */
  protected def promotedRight(ref: String, context: Option[PainlessContext]): String =
    if (rightNeedsTemporalPromotion(context))
      SQLTypeUtils.coerce(ref, valueRenderedType, comparisonTargetType, nullable = false, context)
    else ref

  /** The operand of this criterion, rendered ONCE, as `(chain, coerced)` — see the note at the
    * bottom of the method.
    */
  protected def leftOperand(context: Option[PainlessContext]): (String, String) = {
    // 🔴 Issue #367 — BOTH the target and the source of the coercion are read from the type the
    // operand's RENDERING really has, not from the column's. With `identifier.out` here,
    // `WEEKDAY(d) = 0` asked for the common supertype of TIMESTAMP and BIGINT and then converted
    // the extracted `int` as though it were a date. Issue #384 moved those two derivations up to
    // `operandRenderedType` / `comparisonTargetType`, where the RIGHT operand can read them too.

    /** 🔴 The coercion arms are keyed on the JAVA type of the string that was rendered, and a
      * TEMPORAL chain over a TEMPORAL column does not change it: `doc['d'].value` is a
      * `ZonedDateTime` and every adjuster in the family returns the RECEIVER's own type
      * (`with(TemporalAdjusters…)`, `truncatedTo`, `plus`) — #368's rule, restated. So the chain's
      * SQL type can say DATE while the value is still a `ZonedDateTime`, and coercing DATE ->
      * TIMESTAMP then emits `.atStartOfDay(…)`, which is declared on `LocalDate` alone. MEASURED on
      * ES 8.18: `WHERE LAST_DAY(d) = ts` answered `dynamic method [java.time.ZonedDateTime,
      * atStartOfDay/1] not found`, where the same query WORKED before that fix — found by review,
      * and pinned by `PredicateTransformSurvivalSpec` plus an executed row in
      * `PredicateFunctionResultSpec`.
      *
      * The exception is the narrowing the block below injects: when the target really is DATE (or
      * TIME) the operand is given `.toLocalDate()` / `.toLocalTime()` first, so the two types agree
      * again.
      *
      * A chain whose BASE is not temporal is untouched by any of this: `DATE_PARSE(name, …)`
      * renders `LocalDate.parse(…)`, a genuine `LocalDate`, so DATE -> TIMESTAMP is correct there.
      */
    // 🔴 Issue #384 — ONE derivation, `Identifier.renderedType`, replaces the four-clause override
    // and the `convertsToLocal` predicate that used to sit here. That predicate enumerated the
    // producers BY CLASS (`case c: Conversion if targetType == Date`) and so could not see
    // `DATE_PARSE`, could not see a `CAST` with a preserving function above it, and excluded TIME.
    // `renderedType` asks the FUNCTION instead (`producesOutputJavaType`), which is the only place
    // the answer is knowable — see there for why it is not `foldsOntoOperand`.
    val operandType = operandRenderedType
    val targetedType = comparisonTargetType
    context match {
      case Some(ctx) =>
        ctx.addParam(identifier) match {
          case Some(_) =>
            identifier.originalType match {
              // 🔴 `originalType == Any` means "this identifier NAMES a column", NOT "that column is
              // temporal" — so this narrowing fired for any column at all, and `DATE_PARSE(iso,
              // 'yyyy-MM-dd') = CAST('2025-01-06' AS DATE)` over a KEYWORD column emitted
              // `doc['iso'].value.toLocalDate()`: `.toLocalDate()` on a `String`, which fails the
              // shard. MEASURED on ES 8.18, before and after this fix — the narrowing predates it —
              // and it is the last reason a `DATE_PARSE` predicate could not work. The doc-value is a
              // temporal only when the COLUMN is, which `baseType` is the thing that knows.
              case SQLTypes.Any if mayBeTemporal(identifier) && !valueReadsDocumentField =>
                valueType match {
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
    // 🔴 ONE render, and the pair is what the caller needs (issue #367): `chain` is the operand as its
    // function chain renders it, `coerced` is that with the target-type conversion on top. The
    // shortcut in `painless` is sound exactly when the parameter IS the chain, so it must compare
    // against `chain` and not against `coerced` — a numeric widening is not a chain that failed to
    // fold.
    //
    // 🔴 It is ONE render because a render is not idempotent, MEASURED twice: a repeat render after
    // the right-hand side registered its parameter RE-RESOLVES onto it (`DATE_ADD(d, INTERVAL 1 DAY)
    // = CAST(…)` appended `.plus(1, ChronoUnit.DAYS)` to the LITERAL, adding a day to both sides),
    // and a repeat render of a `TRY_CAST` hoists a SECOND `safe` local, leaving the first one dead
    // and the predicate reading the wrong name. Both are pinned in
    // `PredicateTransformSurvivalSpec`.
    //
    // Calling the STRING form of `coerce` is what makes one render possible, so the two things its
    // identifier form does first are done here instead: the `Any` method injection (for the TARGET
    // type as well as the value's — `coerce` keyed on the first, this method on the second, and both
    // ran before) and, in a PROCESSOR context, the temporal parse it returns in place of a coercion.
    val chain: String = {
      context match {
        // The same temporal guard as the injection above, and for the same measured reason: a
        // `.toLocalDate()` on a KEYWORD column's doc-value is a `String` receiver and fails the shard.
        case Some(ctx)
            if identifier.originalType == SQLTypes.Any && !ctx.isProcessor &&
              mayBeTemporal(identifier) && !valueReadsDocumentField =>
          targetedType match {
            case SQLTypes.Date => identifier.addPainlessMethod(".toLocalDate()")
            case SQLTypes.Time => identifier.addPainlessMethod(".toLocalTime()")
            case _             =>
          }
        case _ =>
      }
      identifier.painless(context)
    }
    val coerced =
      context match {
        // 🔴 `originalType == Any` says this identifier NAMES a column -- it does NOT say the
        // operand still RENDERS that column (issue #373, item 7). Where the chain has already
        // produced something else, the processor's own parse was applied to it a SECOND time:
        // `SCRIPT AS (CASE WHEN YEAR(d) > MONTH(d) …)` emitted
        // `(param2 instanceof String ? LocalDate.parse(param2 …) : Instant.ofEpochMilli(param2)…)`
        // with `param2` an extracted `int`. That is what kept item 6's split from RUNNING.
        // `chainType` (#367) is what says whether the rendering is still a temporal.
        case Some(ctx)
            if identifier.originalType == SQLTypes.Any && ctx.isProcessor &&
              (identifier.chainType == SQLTypes.Any ||
              identifier.chainType.isInstanceOf[SQLTemporal]) =>
          SQLTypeUtils
            .processorTemporal(chain, identifier.declaredType)
            .getOrElse(
              SQLTypeUtils
                .coerce(chain, operandType, targetedType, identifier.nullable, context)
            )
        case _ =>
          SQLTypeUtils.coerce(chain, operandType, targetedType, identifier.nullable, context)
      }
    (chain, coerced)
  }

  /** The operand with its target-type coercion — [[leftOperand]] 's second element, for the callers
    * that do not need the chain rendering apart from it.
    */
  protected def left(context: Option[PainlessContext]): String = leftOperand(context)._2

  protected def check(context: Option[PainlessContext], param: String): String =
    check(context, param, painlessValue(context))

  /** `param <operator> value` in the operator's Painless spelling. `value` is the already-rendered
    * right-hand side, so a caller may adapt it first (the bucket-pipeline rendering converts a
    * temporal literal to epoch millis, the unit a date metric arrives in).
    */
  protected def check(context: Option[PainlessContext], param: String, value: String): String =
    check(context, param, value, effectiveOperator)

  /** Same, for an EXPLICIT operator. `BetweenExpr` renders as two comparisons and needs the `GE` /
    * `LE` per-type dispatch (`compareTo` for strings, `isBefore` / `isAfter` for temporals) that
    * this method owns -- duplicating it there is how the two spellings drift apart.
    */
  protected def check(
    context: Option[PainlessContext],
    param: String,
    value: String,
    op: Operator
  ): String = {
    op match {
      case _: ComparisonOperator =>
        // 🔴 Story BIDC-8 AD-9 — dispatch on the EFFECTIVE operator, i.e. with a `NOT` folded in,
        // exactly as `painlessOp` does for the generic fallback below. Each arm here returns a
        // HARD-CODED spelling (`compareTo(...) == 0`, `isEqual(...)`, …) chosen from the RAW
        // operator, so `WHERE NOT status = 'A'` and `WHERE NOT UPPER(status) = 'A'` both emitted
        // the same Painless as their un-negated twins and the NOT was silently lost — a wrong
        // answer, not an error (MEASURED at the baseline: `maybeNot = Some(NOT)` on the AST,
        // `compareTo("A") == 0` in the script).
        val comparison = op
        comparison match {
          case LIKE | RLIKE =>
            maybeValue match {
              case Some(sv: StringValue) =>
                return likePainless(param, sv.value, sqlWildcards = comparison == LIKE)
              case _ =>
            }
          case LT =>
            valueType match {
              case SQLTypes.Varchar =>
                return s"$param.compareTo($value) < 0"
              case _: SQLTemporal if !isAggregation && !hasBucket =>
                return s"$param.isBefore($value)"
              case _ =>
            }
          case GT =>
            valueType match {
              case SQLTypes.Varchar =>
                return s"$param.compareTo($value) > 0"
              case _: SQLTemporal if !isAggregation && !hasBucket =>
                return s"$param.isAfter($value)"
              case _ =>
            }
          case EQ =>
            valueType match {
              // 🔴 `java.time.LocalTime` is the ONE temporal with no `isEqual` (issue #373): the
              // other three have it -- `LocalDate` and `LocalDateTime` declare it, `ZonedDateTime`
              // inherits it from `ChronoZonedDateTime`. MEASURED on ES 8.18: `CAST(d AS TIME) =
              // CAST('00:00:00' AS TIME)` answered `dynamic method [java.time.LocalTime,
              // isEqual/1] not found`, a shard failure.
              //
              // 🔴 `compareTo(…) == 0` and NOT `equals`, and the difference is the whole point.
              // This dispatch reads `valueType`, the type of the RIGHT operand, while the receiver
              // is the LEFT one -- so a mismatched pair reaches here too (`WHERE name =
              // CAST('00:00:00' AS TIME)` inside a CASE, a `String` receiver). `equals` takes an
              // `Object` and answers FALSE for a mismatch: a loud shard failure would have become
              // a silent wrong answer, and `<>` would have matched every document. MEASURED on ES
              // 8.18, all three: `equals` -> `[false,false,false]`; `compareTo` ->
              // `class_cast_exception`; `isEqual` (main) -> `dynamic method … not found`. The two
              // spellings are otherwise identical for a `LocalTime`, nanoseconds included.
              case SQLTypes.Varchar | SQLTypes.Time =>
                return s"$param.compareTo($value) == 0"
              case _: SQLTemporal if !isAggregation && !hasBucket =>
                return s"$param.isEqual($value)"
              case _ =>
            }
          case NE | DIFF =>
            valueType match {
              case SQLTypes.Varchar | SQLTypes.Time =>
                return s"$param.compareTo($value) != 0"
              case _: SQLTemporal if !isAggregation && !hasBucket =>
                return s"$param.isEqual($value) == false"
              case _ =>
            }
          case GE =>
            valueType match {
              case SQLTypes.Varchar =>
                return s"$param.compareTo($value) >= 0"
              case _: SQLTemporal if !isAggregation && !hasBucket =>
                return s"$param.isBefore($value) == false"
              case _ =>
            }
          case LE =>
            valueType match {
              case SQLTypes.Varchar =>
                return s"$param.compareTo($value) <= 0"
              case _: SQLTemporal if !isAggregation && !hasBucket =>
                return s"$param.isAfter($value) == false"
              case _ =>
            }
          case _ =>
        }
        s"$param ${op.painless(None)} $value"
      case _ => s"$param${op.painless(None)}($value)"
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
    val (chainRendering, innerLeft) = leftOperand(context)

    // The right-hand side is rendered ONCE: `painlessValue` can register a parameter on the context,
    // so calling it twice would declare it twice (review H-4).
    val innerRight = painlessValue(context)

    /** The same rendering, promoted to the comparison's target type when the two sides render
      * different `java.time` types (issue #384). `innerRight` is atomic on every route that uses
      * this one — the parameter shortcut requires `!rightNullable`, and the two tail returns are
      * reached only when the right side needs no local — so the promotion lands where a method may
      * legally be appended. The guarded route below promotes `rightRef` instead, which is the bound
      * local.
      */
    lazy val rightPromoted = promotedRight(innerRight, context)

    /** Does this rendering carry a conditional, i.e. is it a guarded ternary rather than a name, a
      * literal or a call chain? A `?` can only be the ternary operator here — a Painless string
      * literal is the one place it could appear otherwise, and DOUBLE-quoted ones are skipped.
      * (Single-quoted forms such as `ZoneId.of('Z')` are not skipped and do not need to be: no
      * emission in this tree puts a `?` inside one.)
      */
    def carriesConditional(rendered: String): Boolean = {
      var inString = false
      var i = 0
      var found = false
      while (i < rendered.length && !found) {
        val c = rendered.charAt(i)
        if (inString) {
          if (c == '\\') i += 1
          else if (c == '"') inString = false
        } else if (c == '"') inString = true
        else if (c == '?') found = true
        i += 1
      }
      found
    }

    /** An operand of a comparison must be ATOMIC. A rendering that is not a bare name carries
      * operators whose precedence swallows what is appended to it — `param2 ? 1 : 0` with `== 1`
      * appended parses as `param2 ? 1 : (0 == 1)` (review L-9, measured: `Cannot cast from [int] to
      * [java.lang.Object]`), and a null-guarded function rendering has the same shape (AD-9).
      *
      * What makes a rendering dangerous is an operator at PAREN DEPTH 0, so that is exactly what
      * this asks: a name, a literal and a method CHAIN (`p.toLocalDate().get(X)`) are all atomic --
      * every operator inside them is already bracketed -- while a null-guarded ternary is not. Two
      * pins measure both sides of that line: `BooleanCastSpec`'s 21.8 restoration pin fails if the
      * literal of `CASE WHEN 1 = 1` is bound to a local, and `SQLQuerySpec`'s `LAST_DAY` fixture
      * fails if the method chain is.
      */
    def atomic(rendered: String): Boolean = {
      var depth = 0
      var inString = false
      var i = 0
      var result = true
      while (i < rendered.length && result) {
        val c = rendered.charAt(i)
        if (inString) {
          if (c == '\\') i += 1
          else if (c == '"') inString = false
        } else
          c match {
            case '"'                                                 => inString = true
            case '(' | '['                                           => depth += 1
            case ')' | ']'                                           => depth -= 1
            case _ if depth == 0 && "?:+-*/%<>=!&|,".indexOf(c) >= 0 => result = false
            case _                                                   =>
          }
        i += 1
      }
      result
    }

    /** Can this rendering evaluate to NULL at run time? A document field can be missing; a literal
      * cannot. MEASURED over the real parser: a bare column reports `nullable = true`; a
      * function-wrapped column reports `nullable = false` with `dependencies = [status]`; a literal
      * (`CASE WHEN 1 = 1`, an identifier with an empty name and a one-element function chain —
      * story 20.9) reports neither, which is why `functions.nonEmpty` is the wrong test and broke
      * `BooleanCastSpec`'s 21.8 restoration pin.
      */
    def readsDocumentField(token: Option[Token]): Boolean = token match {
      case Some(id: Identifier) =>
        // 🔴 Round 11 (M-1) — `id.functions.exists(_.nullable)` is the third disjunct, and it is
        // what makes a `CASE … END` with no `ELSE` guardable. SQL says the missing branch is NULL,
        // `Case.nullable` reports that faithfully, but the wrapping identifier reports neither
        // `nullable` nor dependencies for it, so the operand reached the comparison unguarded.
        // MEASURED on live ES 8.18.3: `= 1` survived only because Painless tolerates `null == 1`,
        // while `> 0` gave `Cannot invoke "Object.getClass()" because "leftObject" is null` and a
        // string `=` gave `cannot access method/field [compareTo] from a null def reference`.
        // This does NOT re-open the `functions.nonEmpty` trap round 9 measured: a `CASE` WITH an
        // `ELSE` reports `nullable = false`, so the literal shapes `BooleanCastSpec` pins stay
        // unguarded and byte-identical.
        id.nullable || id.dependencies.nonEmpty || id.functions.exists(_.nullable)
      case _ => false
    }

    val leftNullable = readsDocumentField(Some(identifier))
    val rightNullable = readsDocumentField(maybeValue)

    context match {
      case Some(ctx) =>
        ctx.get(identifier) match {
          // 🔴 Issue #367 — the shortcut compares the PARAMETER NAME, so it is correct ONLY where
          // the parameter IS the whole operand, i.e. where the function chain folded entirely into
          // the parameter's own declaration (`YEAR(d)` becomes `def param1 = doc['d']…
          // .get(ChronoField.YEAR)` and `innerLeft` is then literally `param1`). Whenever the chain
          // renders as an EXPRESSION instead -- `WEEKDAY(d)` is `(param1 == null) ? null :
          // (param1.get(ChronoField.DAY_OF_WEEK) + 6) % 7`, because arithmetic cannot be appended to
          // a parameter as a method -- the parameter holds the UNTRANSFORMED doc value, and
          // comparing it here SILENTLY DROPPED the function: `WHERE WEEKDAY(d) = 0` emitted
          // `param1 == null ? false : (param1 == 0)`, a `ZonedDateTime` against an `int`.
          //
          // MEASURED on real Elasticsearch 8.18: that comparison is not an error, it is FALSE, so
          // `=` and `IN` answered ZERO rows with HTTP 200 and `NOT IN` answered EVERY row (the
          // #205 / #253 silent-wrong-answer family), while `>` / `BETWEEN` / a string `compareTo`
          // failed the shard loudly. The SELECT list was correct all along -- a `script_field`
          // renders the operand itself and never enters this method -- which is why it survived.
          //
          // The population is every function whose Painless is not a suffix method chain, and it is
          // wider than the six the issue names: `WEEKDAY`/`DAYOFWEEK`, `LAST_DAY`, `DATE_FORMAT`,
          // `DATETIME_FORMAT`, `DATE_PARSE`, `DATETIME_PARSE` (with their aliases), plus every
          // `CAST`/`CONVERT`/`TRY_CAST`/`SAFE_CAST` and `ISNULL`/`ISNOTNULL`. Guarded by
          // `PredicateTransformSurvivalSpec`, which derives that population from
          // `SQLKeywords.functionWords` rather than listing it.
          //
          // ⚠️ The test is deliberately `p == chainRendering` and not `identifier.functions.isEmpty`
          // or a per-function property: what makes the shortcut sound is that the parameter and the
          // chain are the SAME STRING, and asking exactly that keeps every already-correct emission
          // byte-identical while sending everything else to the general path below, which binds the
          // operand to a local (`bindLocal`) and guards it -- the machinery `UPPER(status)` and
          // `ABS(amount)` already used, and the reason those were never affected.
          case Some(p) if !rightNullable && p == chainRendering =>
            if (identifier.nullable)
              return s"$p == null ? false : $painlessNot(${check(context, p, rightPromoted)})"
            else
              return s"$painlessNot(${check(context, p, rightPromoted)})"
          case _ =>
        }
      case _ =>
    }

    // 🔴 Story BIDC-8 AD-9 — an operand that can be NULL is bound to a local so the comparison lands
    // INSIDE the guard, and the guard collapses to `false` exactly once, at the top of THIS
    // predicate.
    //
    // MEASURED at the baseline: `WHERE UPPER(status) <> 'ZZZ'` emitted
    // `(param1 == null) ? null : param1.toUpperCase().compareTo("ZZZ") != 0` — the function's own
    // null guard with the comparison appended OUTSIDE it. `?:` binds loosest, so the ternary's
    // branches became `null` (Object) and a primitive `boolean`, and Elasticsearch refused the whole
    // query at COMPILE time: `class_cast_exception: Cannot cast from [boolean] to
    // [java.lang.Object]` (live ES 8.18.3, data-independent — it fails on an empty index). The
    // numeric family failed identically; the date family escaped only because `YEAR(...)` folds into
    // the parameter assignment and reports `nullable`.
    //
    // 🔴 BOTH sides are guarded (review H-4): `WHERE UPPER(status) = UPPER(other)` over a document
    // carrying `status` but NOT `other` used to reach `left.compareTo(null)` and fail the shard with
    // `null_pointer_exception`.
    //
    // 🔴 WHY THE `false` COLLAPSE IS THE RIGHT ANSWER HERE, IN EVERY CONTEXT: an `Expression` IS a
    // boolean — it is consumed as a condition, never projected as a value. Its three consumers are
    // the filter script (`bridge/package.scala`, `scriptQuery`), a CASE condition
    // (`function/cond/package.scala`, which captures this rendering into `paramN` and uses it as
    // `paramN ? … : …`) and HAVING (`bucketPipelinePainless`, which returns above). A `null` in any
    // of them is not a value, it is a condition that does not hold — and in a CASE it does not even
    // compile (`def paramN = <Object>; paramN ? 1 : 0`). MEASURED: `SELECT CASE WHEN UPPER(status) =
    // 'A' THEN 1 ELSE 0 END` now returns 1/0/0/1 over docs A/B/absent/A, which is ANSI. What keeps
    // its `null` is the rendering of an IDENTIFIER or a FUNCTION (`SELECT UPPER(status) AS u`, a
    // sort key, a `terms` script) — a different code path that this method never enters, and which
    // still emits `(param1 == null) ? null : …` (measured: `u` is `null` for the absent row).
    // That is the 21.8 Part C rule, preserved.
    val needsLeftLocal = leftNullable || !atomic(innerLeft)
    val leftRef =
      if (needsLeftLocal) context.map(_.bindLocal(innerLeft)).getOrElse(innerLeft) else innerLeft
    // Issue #384 — a rendering carrying a CONDITIONAL is bound before a promotion is appended to
    // it, even though `atomic` accepts it: `atomic` asks whether an OPERATOR may be appended, which
    // a parenthesised ternary allows because every operator inside it is already bracketed.
    //
    // ⚠️ This is about ONE EVALUATION, not about correctness on ES 8. The unbound form
    // `(param2 != null ? … : null).atStartOfDay(…)` was MEASURED on real ES 8.18.3 and it WORKS and
    // returns the right rows — `param2` is a `def`, so the ternary is `def`-typed and the call
    // dispatches dynamically. Story 21.8 rule 2 (`member method [java.lang.Object, …] not found`)
    // describes a differently-typed operand, and assuming it applied here was wrong until it was
    // executed. What the binding buys is real but smaller: the null guard and the comparison read
    // the SAME local instead of evaluating the guarded conversion twice, the emission matches what
    // the LEFT operand already does, and no claim is being made about Painless on ES 6.8 / 7.17,
    // where this shape has not been measured.
    val rightRef =
      if (
        (rightNullable && !atomic(innerRight)) ||
        (rightNeedsTemporalPromotion(context) && carriesConditional(innerRight))
      )
        context.map(_.bindLocal(innerRight, "right")).getOrElse(innerRight)
      else innerRight
    val guards =
      Seq(
        if (leftNullable) Some(s"$leftRef == null") else None,
        if (rightNullable) Some(s"$rightRef == null") else None
      ).flatten
    if (guards.nonEmpty && context.nonEmpty)
      return s"(${guards.mkString(" || ")} ? false : ($painlessNot(${check(context, leftRef, promotedRight(rightRef, context))})))"
    // 🔴 INVARIANT (story BIDC-8, review L-11): a statement sequence is legal ONLY with no
    // `PainlessContext`. `Criteria.painless` is spliced into larger expressions -- a `Predicate`
    // joins two renderings with `&&`/`||`, and a `CASE` condition captures one as
    // `def paramN = <rendering>;` -- so a `def x = …;` in an operand is a compile-time 400. With a
    // context the binding belongs in the PROLOGUE (`PainlessContext.bindLocal`). The context-free
    // route is the HAVING / bucket-pipeline one, which is never spliced. Gated by
    // `PainlessOperandFormSpec`, which FAILS (14 shapes) if `bindLocal` stops hoisting.
    if (identifier.nullable)
      return s"def left = $leftRef; left == null ? false : $painlessNot(${check(context, "left", rightPromoted)})"
    s"$painlessNot${check(context, leftRef, rightPromoted)}"
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

  override def negated: Option[Criteria] =
    Some(this.copy(maybeNot = if (maybeNot.isDefined) None else Some(NOT)))

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

  override def negated: Option[Criteria] =
    Some(this.copy(maybeNot = if (maybeNot.isDefined) None else Some(NOT)))

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

  override def negated: Option[Criteria] = Some(IsNotNullExpr(identifier))

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

  override def negated: Option[Criteria] = Some(IsNullExpr(identifier))

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
      // See the L-11 invariant on `Expression.painless`: the statement form is legal only with no
      // context. `IS NULL` takes a BARE name in the grammar today, so `addParam` above always
      // answers and this line is unreachable under a context -- but a grammar widening must not be
      // able to ship the statement form, so the binding is hoisted when there IS somewhere to
      // hoist it to.
      val rendered = left(context)
      return context match {
        case Some(ctx) => s"${ctx.bindLocal(rendered)} == null"
        case None      => s"def left = $rendered; left == null"
      }
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
      // See the L-11 invariant on `Expression.painless`: the statement form is legal only with no
      // context. `IS NULL` takes a BARE name in the grammar today, so `addParam` above always
      // answers and this line is unreachable under a context -- but a grammar widening must not be
      // able to ship the statement form, so the binding is hoisted when there IS somewhere to
      // hoist it to.
      val rendered = left(context)
      return context match {
        case Some(ctx) => s"${ctx.bindLocal(rendered)} != null"
        case None      => s"def left = $rendered; left != null"
      }
    }
    s"${left(context)} != null"
  }

}

case class InExpr[R, +T <: Value[R]](
  identifier: Identifier,
  values: Values[R, T],
  maybeNot: Option[NOT.type] = None
) extends Expression { this: InExpr[R, T] =>

  override def negated: Option[Criteria] =
    Some(this.copy(maybeNot = if (maybeNot.isDefined) None else Some(NOT)))

  /** 🔴 `$identifier`, NOT a re-application of its head function (issue #365).
    *
    * `Identifier.sql` ALREADY renders the function chain — `Identifier` is sealed with a single
    * implementation that never overrides `sql`, and `Expression.functions` IS
    * `identifier.functions` — so wrapping it again emitted the operand twice. How that looked
    * depended on the function's own `toString`: one that carries its operand gave the malformed
    * `ABS(a)(ABS(a))`, a bare token gave `YEAR(YEAR(a))`.
    *
    * ⚠️ MEASURED, because the obvious reading of that second case is wrong. `YEAR(YEAR(a))` is
    * legal SQL, but it does NOT execute differently: the temporal emission collapses a repeated
    * extractor, so its Painless is byte-identical to `YEAR(a)`, across every statement whose render
    * moved. The extractors rendered wrongly and still ANSWERED correctly; the functions that broke
    * LOUDLY are the ones that cost something.
    *
    * And the loud half does not self-heal. It is not cosmetic: `MaterializedViewExtension` PERSISTS
    * this render and re-runs `client.run(alter.sql)`, `SHOW CREATE MATERIALIZED VIEW` echoes it and
    * `SCRIPT AS` stores it beside the Painless — so a view authored before this fix still holds
    * `ABS(a)(ABS(a)) IN (…)` and keeps failing until it is re-created.
    *
    * Every sibling render here already did the right thing — the comparison arm and `BETWEEN` both
    * interpolate `$identifier` — so this is a return to the house form, not a new convention.
    */
  override def sql =
    s"$identifier $notAsString$operator $values"
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

  /** `[v1, v2].contains(<param>)` -- the LIST owns `contains`, not the element.
    *
    * 🔴 Story BIDC-8 (review L-8). The old override rendered
    * `${identifier.painless}$painlessOp(${values})` = `left.contains(["A","B"])`, which asks a
    * String whether it contains a List. It also bypassed [[Expression.painless]] entirely, so a
    * function-wrapped operand got NO null guard, and `painlessNot` was "" for `IN` so `NOT IN` lost
    * its negation in the document form. Overriding `check` instead puts IN on the ONE guarded
    * emission path: `painless` binds the operand, collapses an absent field to `false`, and
    * `painlessNot` renders the `!` (`IN` has no negated operator -- see
    * `ComparisonOperator.maybeNegated`).
    */
  override protected def check(
    context: Option[PainlessContext],
    param: String,
    value: String,
    op: Operator
  ): String =
    // 🔴 A NUMERIC list is compared with `==`, exactly as [[bucketPipelineCheck]] already does, and
    // for the same measured reason: `List.contains` is Java `equals`, which is FALSE across boxed
    // numeric types. MEASURED on ES 8.18 over a keyword column holding "6"/"9"/"12":
    // `[9,12].contains(<Integer>)` matched, `[9,12].contains(<Long>)` and
    // `[9,12].contains(<Double>)` both returned ZERO rows, while `== 9` matched in all three. So
    // `CAST(num AS BIGINT) IN (9, 12)` and `EPOCHDAY(d) IN (…)` (which renders `getLong`) answered
    // nothing, silently — the #205 family again. `==` promotes numerics, and issue #367 is what
    // makes these operands reach a document at all: before it, the conversion was dropped.
    //
    // A non-numeric list keeps `contains`: for strings `equals` is what IN means, and the rendering
    // is one term rather than N.
    if (numericList)
      values.values.map(v => s"$param == ${v.painless(context)}").mkString("(", " || ", ")")
    else s"$value.contains($param)"

  /** True when every element of the list is a number — the case `List.contains` gets wrong. */
  private def numericList: Boolean =
    values.values.nonEmpty && values.values.forall(_.out.isInstanceOf[SQLNumeric])

  // `params.<metric> == v1 || params.<metric> == v2` -- the guarded bucket form of
  // `<aggregate> IN (v1, v2)`. NOT `[v1,v2].contains(p)`: a buckets_path value arrives as a boxed
  // Double and the literals are Integers, so `List.contains` (Java `equals`) never matched --
  // measured live, `MAX(age) IN (40, 50)` returned no bucket. Painless `==` promotes numerics and
  // uses `equals` for strings. The NOT is NOT rendered here: `IN` has no negated operator, so
  // `painlessNot` renders the `!` for it (review M-6) and `bucketPipelinePainless` already wraps
  // this call in it -- negating twice was a double negation.
  override protected def bucketPipelineCheck(param: String): String =
    values.values.map(v => s"$param == ${v.painless(None)}").mkString(" || ")

}

/** A WHERE predicate whose right-hand side is a SUBQUERY (story 22.2 — ANSI-92 `<in predicate>` /
  * `<exists predicate>` / `<comparison predicate>` with a `<scalar subquery>` / `<quantified
  * comparison predicate>`).
  *
  * It has NO Elasticsearch form of its own. The inner statement is executed FIRST, at the ONE
  * `SingleSearch -> ElasticQuery` seam (`SearchApi.resolveWithSchema`, epic 22 AD-2), and the node
  * is REPLACED by a literal criteria the bridges already translate — an `InExpr` over the inner
  * column's distinct values, a `GenericExpression` over a literal, or [[MatchAllCriteria]] /
  * [[MatchNoneCriteria]]. A node that reaches a bridge unresolved is a programming error and fails
  * LOUDLY there (`ElasticBridge.query`), never silently.
  *
  * Extends `Criteria` directly rather than `Expression`: `Expression` is keyed on an `identifier`
  * and a `maybeValue: Option[Token]` and its `validate()` compares `identifier.out` with the
  * value's type — a subquery has no value type until it has run, and `EXISTS` has no identifier at
  * all.
  *
  * `correlatedRefs` is RECORDED in `update(outer)` (the house pattern: record in update, format in
  * validate — `FieldSort.bareTableAlias`), because `update` runs inside `Parser.single`'s action
  * and cannot report, and because the OUTER scope is only known one level up. See
  * [[SubqueryScope]].
  */
sealed trait SubqueryCriteria extends Criteria with ElasticFilter {
  def query: DqlStatement
  def maybeNot: Option[NOT.type]
  def correlatedRefs: Seq[Identifier]

  /** This node carrying a REWRITTEN body (story 22.5) — the write half of
    * [[Criteria.embeddedStatements]], which `Criteria.mapEmbeddedStatements` dispatches to.
    *
    * ABSTRACT on purpose. A defaulted `this` would let a new subquery kind silently opt out of
    * every rewrite that descends through criteria (today: the CTE substitution), and the failure
    * mode is a CTE name inside `IN (SELECT ... FROM cte)` resolved as an INDEX — `index_not_found`
    * when absent, a WRONG ANSWER with HTTP 200 when an index of that name exists. Declared here,
    * the compiler refuses the omission.
    */
  def withQuery(q: DqlStatement): SubqueryCriteria

  /** The identifiers of the OUTER statement this node names directly (its left operand) — what
    * `referencedIdentifiers` / `derivedScopeCheck` / the aggregate-in-WHERE rejection must see.
    * `EXISTS` names none.
    */
  def outerIdentifiers: Seq[Identifier]

  /** The body as a `SingleSearch` when it is one — the only body kind this story EXECUTES (PD-7).
    */
  final def inner: Option[SingleSearch] = query match {
    case s: SingleSearch => Some(s)
    case _               => None
  }

  override def group: Boolean = false

  override def nestedElement: Option[NestedElement] = None

  override def asFilter(currentQuery: Option[ElasticBoolQuery]): ElasticFilter = this

  /** A subquery has no Painless: it is never a script predicate (WHERE only — AD-6). Reachable only
    * through a CASE-WHEN condition that `Case.validate()` did not catch, so it is the LAST line of
    * defence rather than the rejection — loud and named either way.
    */
  override def painless(context: Option[PainlessContext]): String =
    throw new IllegalArgumentException(
      s"A WHERE subquery has no Painless form and cannot appear in a CASE / HAVING / script " +
      s"context: $sql"
    )

  protected def notAsString: String = maybeNot.map(_ => "NOT ").getOrElse("")

  /** Shared validation, in order: a body kind this story executes, then the body's OWN rules
    * (`Parser.apply` validates the TOP level only — story 22.1's trap, one clause over).
    *
    * 🔴 Story 22.3b DELETED the third arm — the `correlatedRefs.headOption => Left` rejection. That
    * single deletion, plus ONE widened disjunct in `SingleSearch.relationalClosureRequired`, IS the
    * rejection-to-routing flip: a correlated subquery now PARSES and routes to the relational
    * engine, and every venue WITHOUT the engine refuses it loudly at `SearchApi.resolveWithSchema`
    * / `CoreDqlExtension` through `RelationalClosureGuard`. Nothing else in `sql` moved.
    * `correlatedRefs` stays RECORDED on the node — it is what the planner reads to build the
    * correlated leg.
    */
  protected def commonChecks: Either[String, Unit] =
    for {
      _ <- query match {
        case s: SingleSearch => s.validate()
        // 🔴 Story 22.6 — name the operator the analyst WROTE. This message hard-coded `UNION
        // ALL`, and until 22.6 that was the only spelling the grammar accepted, so it was always
        // right. It is now reachable for five more, and MEASURED naming `UNION ALL` for a
        // statement whose author wrote `UNION` — the one spelling they did not use.
        case m: MultiSearch =>
          Left(
            s"${m.resolvedOperators.map(_.sql).distinct.mkString(" / ")} inside a WHERE subquery " +
            s"is not supported yet: $sql. Write one subquery per branch."
          )
        case _: FromlessSelect =>
          Left(
            s"A WHERE subquery must read a table: $sql. " +
            "For constant values write the literal list (IN ('a', 'b')) or the literal itself."
          )
        case other =>
          Left(s"A WHERE subquery body must be a SELECT, got ${other.getClass.getSimpleName}")
      }
    } yield ()

  /** The projected column of the inner statement — exactly ONE, never `*` — for IN / quantified /
    * scalar. `identifierName` is the SAME test `SearchApi.extractOutputFieldNames` makes: ONE
    * spelling of "is this a SELECT *" (the story 21.3 two-derivations lesson).
    */
  protected def singleColumnCheck(position: String): Either[String, Unit] =
    inner.map(_.select.fields) match {
      case Some(Seq(f)) if f.identifier.identifierName == "*" =>
        Left(s"A subquery in $position position must project exactly one column, not *: $sql")
      case Some(Seq(_)) => Right(())
      case Some(fs) =>
        Left(
          s"A subquery in $position position must project exactly one column, got ${fs.size}: $sql"
        )
      case None => Right(()) // the body KIND was already rejected by commonChecks
    }
}

/** `<identifier> [NOT] IN (<subquery>)` — also what `= ANY|SOME (…)` and `<> ALL (…)` / `!= ALL
  * (…)` reduce to AT PARSE TIME (PD-3): the SAME node, so the rewrite, the correlation rule and the
  * render have ONE implementation. The render is the canonical `IN` spelling — `x = ANY (SELECT …)`
  * re-parses as `x IN (SELECT …)`, an equal AST by construction (the fixed point holds on the
  * CANONICAL text, as `LEFT OUTER JOIN` -> `LEFT JOIN` already does).
  */
case class InSubquery(
  identifier: Identifier,
  query: DqlStatement,
  maybeNot: Option[NOT.type] = None,
  correlatedRefs: Seq[Identifier] = Nil
) extends SubqueryCriteria {
  override def withQuery(q: DqlStatement): InSubquery = this.copy(query = q)

  override def operator: Operator = IN
  override def sql: String = s"$identifier $notAsString$operator (${query.sql})"

  /** 🔴 Required by `PainlessOperandFormSpec`, and it is not bookkeeping: without it a `NOT`
    * written AFTER a predicate operator (`a = 1 AND NOT <this>`) falls back to wrapping the
    * UN-negated criterion in an Elasticsearch `must_not`, which MATCHES a document lacking the
    * field - the story BIDC-8 defect. Folding the NOT into `maybeNot` hands the negation to the
    * RESOLVER, which is the only place that knows the ANSI three-valued answer (an `ALL` form over
    * a NULL-bearing set stays `MatchNone` under a NOT, an empty `ANY` flips to `MatchAll`).
    */
  override def negated: Option[Criteria] =
    Some(this.copy(maybeNot = if (maybeNot.isDefined) None else Some(NOT)))
  override def outerIdentifiers: Seq[Identifier] = Seq(identifier)
  override def nested: Boolean = identifier.nested
  override def nestedElement: Option[NestedElement] = identifier.nestedElement
  override lazy val limit: Option[Limit] = identifier.limit

  override def update(request: SingleSearch): Criteria = {
    val updated = this.copy(
      identifier = identifier.update(request),
      correlatedRefs = SubqueryScope.correlatedReferences(query, request)
    )
    // the same shape as InExpr.update: a nested (UNNEST) left operand is wrapped like any other
    if (updated.nested) ElasticNested(updated, limit) else updated
  }

  override def validate(): Either[String, Unit] =
    for {
      _ <- identifier.validate()
      _ <- commonChecks
      _ <- singleColumnCheck("IN")
    } yield ()
}

/** `[NOT] EXISTS (<subquery>)` — true iff the inner statement returns at least one row. */
case class ExistsSubquery(
  query: DqlStatement,
  maybeNot: Option[NOT.type] = None,
  correlatedRefs: Seq[Identifier] = Nil
) extends SubqueryCriteria {
  override def withQuery(q: DqlStatement): ExistsSubquery = this.copy(query = q)

  override def operator: Operator = EXISTS
  override def sql: String = s"$notAsString$operator (${query.sql})"

  /** 🔴 Required by `PainlessOperandFormSpec`, and it is not bookkeeping: without it a `NOT`
    * written AFTER a predicate operator (`a = 1 AND NOT <this>`) falls back to wrapping the
    * UN-negated criterion in an Elasticsearch `must_not`, which MATCHES a document lacking the
    * field - the story BIDC-8 defect. Folding the NOT into `maybeNot` hands the negation to the
    * RESOLVER, which is the only place that knows the ANSI three-valued answer (an `ALL` form over
    * a NULL-bearing set stays `MatchNone` under a NOT, an empty `ANY` flips to `MatchAll`).
    */
  override def negated: Option[Criteria] =
    Some(this.copy(maybeNot = if (maybeNot.isDefined) None else Some(NOT)))
  override def outerIdentifiers: Seq[Identifier] = Nil
  override def update(request: SingleSearch): Criteria =
    this.copy(correlatedRefs = SubqueryScope.correlatedReferences(query, request))
  override def validate(): Either[String, Unit] = commonChecks
}

/** `<identifier> <op> (<subquery>)` where the subquery yields ONE row, ONE column (ANSI `<scalar
  * subquery>`). Statically enforced as "an aggregate with no GROUP BY, or LIMIT 1"; the RUN-TIME
  * row count is re-checked by the resolver (0 rows = NULL = no match; > 1 = 400, ANSI cardinality
  * violation).
  */
case class ScalarSubquery(
  identifier: Identifier,
  operator: ComparisonOperator,
  query: DqlStatement,
  maybeNot: Option[NOT.type] = None,
  correlatedRefs: Seq[Identifier] = Nil
) extends SubqueryCriteria {
  override def withQuery(q: DqlStatement): ScalarSubquery = this.copy(query = q)

  override def sql: String = s"$notAsString$identifier $operator (${query.sql})"

  /** 🔴 Required by `PainlessOperandFormSpec`, and it is not bookkeeping: without it a `NOT`
    * written AFTER a predicate operator (`a = 1 AND NOT <this>`) falls back to wrapping the
    * UN-negated criterion in an Elasticsearch `must_not`, which MATCHES a document lacking the
    * field - the story BIDC-8 defect. Folding the NOT into `maybeNot` hands the negation to the
    * RESOLVER, which is the only place that knows the ANSI three-valued answer (an `ALL` form over
    * a NULL-bearing set stays `MatchNone` under a NOT, an empty `ANY` flips to `MatchAll`).
    */
  override def negated: Option[Criteria] =
    Some(this.copy(maybeNot = if (maybeNot.isDefined) None else Some(NOT)))
  override def outerIdentifiers: Seq[Identifier] = Seq(identifier)
  override def nested: Boolean = identifier.nested
  override def nestedElement: Option[NestedElement] = identifier.nestedElement
  override lazy val limit: Option[Limit] = identifier.limit

  override def update(request: SingleSearch): Criteria = {
    val updated = this.copy(
      identifier = identifier.update(request),
      correlatedRefs = SubqueryScope.correlatedReferences(query, request)
    )
    if (updated.nested) ElasticNested(updated, limit) else updated
  }

  override def validate(): Either[String, Unit] =
    for {
      _ <- identifier.validate()
      _ <- commonChecks
      _ <- singleColumnCheck("scalar")
      _ <- inner match {
        case Some(s) if (!s.returnsRows && s.groupBy.isEmpty) || s.limit.exists(_.limit == 1) =>
          Right(())
        case Some(_) =>
          Left(
            s"A scalar subquery must return a single row: $sql. " +
            "Use an aggregate with no GROUP BY (MAX, MIN, AVG, SUM, COUNT ...) or add LIMIT 1."
          )
        case None => Right(())
      }
    } yield ()
}

/** `<identifier> <op> ANY|SOME|ALL (<subquery>)` for the TEN combinations that are not already the
  * `IN` machinery (lead ruling OQ-4, 2026-09-14 — this REVERSES the spec's PD-1, which rejected
  * them with a MIN/MAX rewrite message).
  *
  * 🔴 The body resolves EXACTLY as [[InSubquery]] 's does — a bounded value LIST (mode P / mode W,
  * the same typing, the same 65,536 bound) — and the quantifier is reduced IN THE RESOLVER from
  * that list (`SubqueryResolver.reduceQuantified`). The rejected alternative, rewriting the body to
  * `MIN(…)` / `MAX(…)` at parse time, cannot wrap a body that already carries `GROUP BY` / `HAVING`
  * / `LIMIT` (that needs 22.4's derived tables), needs TWO aggregates for `= ALL` and `<> ANY`, and
  * would have to recover the ANSI empty-set rule from a NULL aggregate result — the `rowNormalizer`
  * null-vs-absent trap this story's own review flagged.
  *
  * It is a [[SubqueryCriteria]] sibling and NOT a [[ScalarSubquery]] variant precisely because its
  * body yields a LIST where a scalar subquery's yields one cell.
  *
  * `SOME` is CANONICALISED to `ANY` (they are synonyms in ANSI SQL), the same canonical-render rule
  * PD-3 applies to `= ANY` -> `IN`: `x > SOME (S)` renders `x > ANY (S)` and re-parses to an EQUAL
  * AST.
  */
case class QuantifiedSubquery(
  identifier: Identifier,
  operator: ComparisonOperator,
  quantifier: Quantifier,
  query: DqlStatement,
  maybeNot: Option[NOT.type] = None,
  correlatedRefs: Seq[Identifier] = Nil
) extends SubqueryCriteria {
  override def withQuery(q: DqlStatement): QuantifiedSubquery = this.copy(query = q)

  override def sql: String =
    s"$notAsString$identifier $operator $quantifier (${query.sql})"

  /** 🔴 Required by `PainlessOperandFormSpec`, and it is not bookkeeping: without it a `NOT`
    * written AFTER a predicate operator (`a = 1 AND NOT <this>`) falls back to wrapping the
    * UN-negated criterion in an Elasticsearch `must_not`, which MATCHES a document lacking the
    * field - the story BIDC-8 defect. Folding the NOT into `maybeNot` hands the negation to the
    * RESOLVER, which is the only place that knows the ANSI three-valued answer (an `ALL` form over
    * a NULL-bearing set stays `MatchNone` under a NOT, an empty `ANY` flips to `MatchAll`).
    */
  override def negated: Option[Criteria] =
    Some(this.copy(maybeNot = if (maybeNot.isDefined) None else Some(NOT)))
  override def outerIdentifiers: Seq[Identifier] = Seq(identifier)
  override def nested: Boolean = identifier.nested
  override def nestedElement: Option[NestedElement] = identifier.nestedElement
  override lazy val limit: Option[Limit] = identifier.limit

  /** `true` for `ALL`, `false` for `ANY` / `SOME` — the ONE place the two families are told apart,
    * so the resolver's opposite empty-set rules cannot be keyed off two different tests.
    */
  def universal: Boolean = quantifier == ALL

  override def update(request: SingleSearch): Criteria = {
    val updated = this.copy(
      identifier = identifier.update(request),
      correlatedRefs = SubqueryScope.correlatedReferences(query, request)
    )
    if (updated.nested) ElasticNested(updated, limit) else updated
  }

  override def validate(): Either[String, Unit] =
    for {
      _ <- identifier.validate()
      _ <- commonChecks
      _ <- singleColumnCheck(s"$operator $quantifier")
    } yield ()
}

/** The two RESOLVED sentinels (story 22.2). Produced ONLY by `SubqueryResolver`, never by the
  * parser; their `sql` renders (`1 = 1` / `1 = 0`) exist so a resolved statement still logs and
  * re-parses as a `SingleSearch` — they are not a fixed point of THESE classes (the re-parse yields
  * a `GenericExpression`), which is fine: nothing persists a RESOLVED statement
  * (`MaterializedViewExtension` persists the PARSED one, and a MV carrying a WHERE subquery is
  * refused at `validate()`).
  */
case class MatchAllCriteria() extends Criteria with ElasticFilter {
  override def operator: Operator = EQ
  override def sql: String = "1 = 1"
  override def group: Boolean = false
  override def nestedElement: Option[NestedElement] = None
  override def update(request: SingleSearch): Criteria = this
  override def asFilter(currentQuery: Option[ElasticBoolQuery]): ElasticFilter = this
  override def painless(context: Option[PainlessContext]): String = "true"
}

case class MatchNoneCriteria() extends Criteria with ElasticFilter {
  override def operator: Operator = EQ
  override def sql: String = "1 = 0"
  override def group: Boolean = false
  override def nestedElement: Option[NestedElement] = None
  override def update(request: SingleSearch): Criteria = this
  override def asFilter(currentQuery: Option[ElasticBoolQuery]): ElasticFilter = this
  override def painless(context: Option[PainlessContext]): String = "false"
}

case class BetweenExpr(
  identifier: Identifier,
  fromTo: FromTo,
  maybeNot: Option[NOT.type]
) extends Expression {

  /** 🔴 Story BIDC-8 (round 10, BLOCKING-1). Without this override `Predicate.emittedRight` fell
    * back to wrapping the UN-negated criterion in an Elasticsearch `must_not`, whose semantics
    * INCLUDE a document lacking the field — so the guarded script's `false` was inverted into a
    * match. MEASURED on live ES 8.18.3 over a document carrying `status` but NO `amount`: `WHERE
    * status = 'A' AND NOT ABS(amount) BETWEEN 1 AND 10` returned `[8]` while the mirror `WHERE
    * ABS(amount) NOT BETWEEN 1 AND 10 AND status = 'A'` returned `[]` — the same predicate,
    * different rows by writing order, and the row it added is the one ANSI excludes. It is the
    * defect `Criteria.negated` exists to prevent, and it survived because the round-9 pins only
    * exercised `GenericExpression`, the one class that already had the override.
    */
  override def negated: Option[Criteria] =
    Some(this.copy(maybeNot = if (maybeNot.isDefined) None else Some(NOT)))

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

  /** Two comparisons, `param >= from && param <= to`, each rendered by the SHARED per-type
    * dispatch.
    *
    * 🔴 Story BIDC-8 (review L-8). The old override emitted three things Painless rejects.
    * `${fromTo.from}` is `Token.toString` = the SQL spelling, so a string bound rendered `'A'` -- a
    * syntax error in a script; the un-parameterised branch emitted the CHAINED `a <= left <= b`,
    * which Painless refuses (`boolean <= int`) exactly as this class's own `bucketPipelineCheck`
    * comment already recorded; and a temporal bound needs `isBefore` / `isAfter`, which only
    * [[Expression.check]] knows. Overriding `check` puts BETWEEN on the ONE guarded emission path
    * and reuses that dispatch via the explicit `GE` / `LE` operators. The NOT is rendered by
    * `painlessNot` (`BETWEEN` has no negated operator).
    */
  override protected def check(
    context: Option[PainlessContext],
    param: String,
    value: String,
    op: Operator
  ): String = {
    val from = painlessOperand(fromTo.from, context)
    val to = painlessOperand(fromTo.to, context)
    s"(${super.check(context, param, from, GE)} && ${super.check(context, param, to, LE)})"
  }

  // The guarded bucket form of `<aggregate> BETWEEN a AND b` -- two comparisons, not a chained
  // `a <= p <= b` (Painless rejects `boolean <= int`). The NOT is NOT rendered here: `BETWEEN` has
  // no negated operator, so `painlessNot` renders the `!` (review M-6) and
  // `bucketPipelinePainless` already wraps this call in it.
  override protected def bucketPipelineCheck(param: String): String =
    s"$param >= ${fromTo.from} && $param <= ${fromTo.to}"

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

  /** 🔴 Story 22.2 (independent review, H2). Without this override a relation inherits
    * `Validator`'s no-op `validate()` and NEVER recurses into the criteria it wraps — so EVERY rule
    * that lives in a leaf's `validate()` is skipped inside `NESTED(…)` / `CHILD(…)` / `PARENT(…)`,
    * and inside the `ElasticNested` wrapper the subquery nodes put THEMSELVES in when their left
    * operand is nested (`InSubquery.update` and friends).
    *
    * Measured consequence for this story: `WHERE items.sku IN (SELECT a, b FROM u)` skipped the
    * single-column check and silently resolved against the FIRST of two projected columns, and a
    * CORRELATED body inside a relation ran as if it were uncorrelated. `Criteria.subqueries` DOES
    * walk relations, so the resolver executed the node either way — validation was the only thing
    * missing. The fix is not subquery-specific: `InExpr`'s and `Expression`'s own type checks were
    * being skipped in the same position.
    */
  override def validate(): Either[String, Unit] = criteria.validate()

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
