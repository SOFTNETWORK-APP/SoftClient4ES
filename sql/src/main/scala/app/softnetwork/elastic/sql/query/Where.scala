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
    val innerLeft = left(context)
    // The right-hand side is rendered ONCE: `painlessValue` can register a parameter on the context,
    // so calling it twice would declare it twice (review H-4).
    val innerRight = painlessValue(context)

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
          case Some(p) if !rightNullable =>
            if (identifier.nullable)
              return s"$p == null ? false : $painlessNot(${check(context, p, innerRight)})"
            else
              return s"$painlessNot(${check(context, p, innerRight)})"
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
    val rightRef =
      if (rightNullable && !atomic(innerRight))
        context.map(_.bindLocal(innerRight, "right")).getOrElse(innerRight)
      else innerRight
    val guards =
      Seq(
        if (leftNullable) Some(s"$leftRef == null") else None,
        if (rightNullable) Some(s"$rightRef == null") else None
      ).flatten
    if (guards.nonEmpty && context.nonEmpty)
      return s"(${guards.mkString(" || ")} ? false : ($painlessNot(${check(context, leftRef, rightRef)})))"
    // 🔴 INVARIANT (story BIDC-8, review L-11): a statement sequence is legal ONLY with no
    // `PainlessContext`. `Criteria.painless` is spliced into larger expressions -- a `Predicate`
    // joins two renderings with `&&`/`||`, and a `CASE` condition captures one as
    // `def paramN = <rendering>;` -- so a `def x = …;` in an operand is a compile-time 400. With a
    // context the binding belongs in the PROLOGUE (`PainlessContext.bindLocal`). The context-free
    // route is the HAVING / bucket-pipeline one, which is never spliced. Gated by
    // `PainlessOperandFormSpec`, which FAILS (14 shapes) if `bindLocal` stops hoisting.
    if (identifier.nullable)
      return s"def left = $leftRef; left == null ? false : $painlessNot(${check(context, "left", innerRight)})"
    s"$painlessNot${check(context, leftRef, innerRight)}"
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
  ): String = s"$value.contains($param)"

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
