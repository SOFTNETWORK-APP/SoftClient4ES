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

import app.softnetwork.elastic.sql.{Expr, Identifier, TokenRegex, Updateable}
import app.softnetwork.elastic.sql.function.{FunctionN, FunctionUtils}
import app.softnetwork.elastic.sql.function.cond.Case
import app.softnetwork.elastic.sql.function.convert.Conversion

import scala.util.DynamicVariable

case object Having extends Expr("HAVING") with TokenRegex {

  /** The SELECT items a bare name in a `HAVING` may resolve to: aggregates AND arithmetic over
    * aggregates (`MAX(x) - MIN(x) AS d`), the latter being a `bucket_script` that a
    * `bucket_selector` may read as a sibling pipeline aggregation by name.
    *
    * ONE derivation, read by [[resolveAggregateAliases]] (which SUBSTITUTES a reference, at every
    * depth of a value expression) and by `SingleSearch.validate()` (which REFUSES, issue #389, a
    * function of an alias the substitution leaves in place: inside a relation predicate or a MATCH,
    * which it does not touch).
    */
  private[query] def aggregateAliases(request: SingleSearch): Map[String, Identifier] =
    request.select.fields.collect {
      case f if (f.isAggregation || f.isBucketScript) && f.fieldAlias.isDefined =>
        f.fieldAlias.get.alias -> f.identifier
    }.toMap

  /** The SELECT aggregates a bare name denotes while [[resolveAggregateAliases]] runs, and nothing
    * outside it: read by `GenericIdentifier.update` through [[aliasedAggregate]].
    *
    * 🔴 Why a thread-scoped carrier, not a parameter -- the reason `SubqueryScope` gives for its
    * own. An alias hides at ANY depth of a value expression: an operand, a function argument
    * (`COALESCE(max_a, min_b)`), an arithmetic operand, a `CASE` branch. The only walk that reaches
    * every one of those AND rebuilds the node it found is `update(request)`, implemented by every
    * function of the dialect; a second walk with its own rebuild for each of them would be a second
    * derivation that drifts. `update` takes the statement and nothing else, so the alias map rides
    * beside it, set around the substitution below and restored on exit. Resolution is synchronous,
    * so the value is read on the thread that set it.
    */
  private[this] val aliasScope: DynamicVariable[Map[String, Identifier]] =
    new DynamicVariable[Map[String, Identifier]](Map.empty)

  /** The SELECT item a bare HAVING name stands for, while [[resolveAggregateAliases]] runs.
    *
    * Never for an identifier that IS an aggregate: its name is the column the aggregate reads (`a`
    * in `MAX(a)`), not a reference to a SELECT item -- and `MAX(a) AS a` would otherwise substitute
    * the aggregate into its own operand forever. Never for a nested column either, as before.
    */
  private[sql] def aliasedAggregate(id: Identifier): Option[Identifier] = {
    val aliases = aliasScope.value
    if (aliases.isEmpty || id.nested || id.isAggregation) None else aliases.get(id.name)
  }

  /** `body` with no alias substitution: what an aggregate's own operands are resolved under. */
  private[sql] def outsideAliasScope[T](body: => T): T =
    if (aliasScope.value.isEmpty) body else aliasScope.withValue(Map.empty)(body)

  /** `HAVING cnt > 1` where `cnt` aliases a SELECT aggregate (`COUNT(name) AS cnt`). The bare
    * identifier carries no aggregate function of its own, so the selector rendering saw no metric
    * in the condition and the whole HAVING degenerated to `1 == 1`: every group came back — the
    * "workaround" issue #54 itself prescribed, never tested (BIDC-2). Substituting the aliased
    * SELECT item's identifier BEFORE the criteria are updated makes the reference resolve like any
    * other aggregate: the alias comes back through `fieldAliases`, the selector reads `params.cnt`,
    * and no extra aggregation is created since the item is already a SELECT aggregate. Scoped to
    * HAVING on purpose: ORDER BY resolves an alias by name already, and WHERE must keep reading a
    * bare name as a field. Relation predicates (`NESTED(...)`) and `MATCH` are left untouched.
    *
    * 🔴 At EVERY depth of the leaf's operands, not only the operand itself. These become the very
    * trees their bare spellings parse to: `SIGN(min_a)`, `max_a - min_b`, `CAST(min_a AS DOUBLE)`,
    * `COALESCE(max_a, min_b)`. So the bare, qualified and alias spellings of one shape get ONE
    * verdict and ONE message. Substituting the operand alone left an alias inside a function a bare
    * column, which the statement rules then had to refuse by name ("compare the aggregate itself")
    * -- for shapes whose bare spelling is accepted.
    */
  private[query] def resolveAggregateAliases(
    criteria: Criteria,
    request: SingleSearch
  ): Criteria = {
    val aliased: Map[String, Identifier] = aggregateAliases(request)
    if (aliased.isEmpty) return criteria

    // Does this operand name an alias where the rules that read the substituted tree can see it --
    // itself, a function argument at any depth (`FunctionUtils.funIdentifiers`, the walk behind
    // `Identifier.bucketMetrics`), a `CASE` condition?
    //
    // ⚠️ Deliberately the derivation's walk, not `update`'s. `ST_DISTANCE` keeps its operands
    // outside `args`: `update` reaches them, `funIdentifiers` does not. Substituted there, an
    // aggregate would sit where no rule can see it, and `ST_DISTANCE(max_a, min_b) > 1` would be
    // refused as reading "neither a GROUP BY key nor an aggregate" with a remedy -- move it to
    // WHERE -- that now names aggregates WHERE refuses. Left unsubstituted, it keeps its verdict.
    def namesAlias(id: Identifier): Boolean =
      FunctionUtils.funIdentifiers(id).exists(i => aliased.contains(i.name)) ||
      Case.conditionsOf(id).exists(_.referencedIdentifiers.exists(namesAlias))

    // The aliases of SELECT `bucket_script` items: expressions over aggregates (`MAX(a) - MIN(b) AS
    // d`), not aggregates.
    val bucketScriptAliases: Set[String] =
      aliased.collect {
        case (alias, item) if !item.isAggregation && item.hasAggregation => alias
      }.toSet

    // Does this operand read such an alias where no group filter answers it right, at any depth?
    //
    // ⚠️ Left unsubstituted, so it keeps the refusal it has always had (`SingleSearch.validate`:
    // "cannot apply a function to the aggregate alias"). Two places, both MEASURED on
    // Elasticsearch 8.18.3 over groups lacking an operand:
    //   - among the arguments of a function that decides what a NULL argument means -- COALESCE,
    //     GREATEST, LEAST ([[NullDecider]]). There the item is read through its OPERANDS
    //     (`params.max_a - params.min_b`), and an operand's NULL throws instead of making the item
    //     NULL, the one thing those functions exist to decide: `COALESCE(d, 0)`, `GREATEST(d, 1)`
    //     and `LEAST(d, 1)` failed the search with a null-pointer error;
    //   - under a conversion of the alias itself (`CAST(d AS DOUBLE)`, `d::INT`, `TRY_CAST`,
    //     `CONVERT`). The conversion is a function OF the item, so the filter declares the item --
    //     but a comparison reads `params.d` and DROPS the conversion (`CAST(d AS BIGINT) > 1`
    //     compared `d` unconverted), and the value side and `ISNULL` / `ISNOTNULL` read the
    //     undeclared operands and failed with a null-pointer error.
    // `SIGN(d)` -- NULL whenever an operand is, its filter declaring the operands it reads --
    // answered right in every position.
    def unansweredOverBucketScript(id: Identifier): Boolean =
      bucketScriptAliases.nonEmpty && (decidesNullOfBucketScript(id) || FunctionUtils
        .funIdentifiers(id)
        .exists(i =>
          bucketScriptAliases.contains(i.name) && i.functions.exists(_.isInstanceOf[Conversion])
        ))

    def decidesNullOfBucketScript(id: Identifier): Boolean =
      id.functions.exists {
        case NullDecider(arguments) =>
          arguments.exists {
            case argument: Identifier =>
              FunctionUtils
                .funIdentifiers(argument)
                .exists(i => bucketScriptAliases.contains(i.name))
            case _ => false
          }
        case f: FunctionN[_, _] =>
          f.args.exists {
            case argument: Identifier => decidesNullOfBucketScript(argument)
            case _                    => false
          }
        case _ => false
      }

    // `update` is the walk that rebuilds: `GenericIdentifier.update` swaps every alias it meets for
    // its SELECT item (see [[aliasScope]]). The result is updated again with the clause, as the
    // operand alone always was.
    def substitute(id: Identifier): Identifier =
      if (namesAlias(id) && !unansweredOverBucketScript(id))
        aliasScope.withValue(aliased)(id.update(request))
      else id

    def rewrite(c: Criteria): Criteria = c match {
      case p: Predicate =>
        p.copy(leftCriteria = rewrite(p.leftCriteria), rightCriteria = rewrite(p.rightCriteria))
      case e: GenericExpression =>
        e.copy(
          identifier = substitute(e.identifier),
          value = e.value match {
            case id: Identifier => substitute(id)
            case v              => v
          }
        )
      case e: Comparison =>
        e.copy(
          identifier = substitute(e.identifier),
          value = e.value match {
            case id: Identifier => substitute(id)
            case v              => v
          }
        )
      case e: BetweenExpr       => e.copy(identifier = substitute(e.identifier))
      case e: InExpr[_, _]      => e.copy(identifier = substitute(e.identifier))
      case e: IsNullCriteria    => e.copy(identifier = substitute(e.identifier))
      case e: IsNotNullCriteria => e.copy(identifier = substitute(e.identifier))
      case other                => other
    }

    rewrite(criteria)
  }
}

case class Having(criteria: Option[Criteria]) extends Updateable {
  override def sql: String = criteria match {
    case Some(c) => s" $Having $c"
    case _       => ""
  }
  def update(request: SingleSearch): Having =
    this.copy(criteria =
      criteria.map(c => Having.resolveAggregateAliases(c, request).update(request))
    )

  /** The name of every metric this clause reads, in statement order: the `metricPathKey` of each of
    * `Criteria.bucketMetrics` -- the keys of the `buckets_path` a `bucket_selector` over this
    * clause must declare, and exactly the `params.<name>` [[script]] reads.
    *
    * 🔴 PUBLIC for `softclient4es-extensions`: a materialized view's pivot names each aggregation
    * after its SELECT alias, and that alias IS the metric key here, so the view's filter declares
    * `metricNames.map(n => n -> n)` and nothing else. The view rules in `SingleSearch` refuse a
    * clause one of whose names the pivot does not create (`transformAggregationNames`).
    */
  def metricNames: Seq[String] = criteria.toSeq.flatMap(_.bucketMetrics).map(_.metricPathKey)

  /** Story 22.2 (AD-6) — HAVING reaches `criteria` through the SHARED `whereCriteria` production,
    * so a subquery node is grammatically reachable here. It is DECLINED in `validate()` rather than
    * by duplicating the alternation: one grammar, one reduction
    * (`project_parser_rejection_semantics`). The subquery phase runs on the WHERE only, so a
    * subquery left in a HAVING would reach the bucket-selector script and die in the node's
    * `painless` throw — this arm is the named rejection that comes first.
    */
  override def validate(): Either[String, Unit] =
    criteria.map(_.subqueries).getOrElse(Nil).headOption match {
      case Some(s) =>
        Left(
          s"A subquery is not supported in HAVING: ${s.sql}. " +
          "Compute the value in a separate query, or move the condition to WHERE."
        )
      case None => criteria.map(_.validate()).getOrElse(Right(()))
    }

  def nestedElements: Seq[NestedElement] =
    criteria.map(_.nestedElements).getOrElse(Seq.empty).groupBy(_.path).map(_._2.head).toList

  /** Every condition of this clause the engine cannot express as a `bucket_selector` (issue #389),
    * in statement order. Refused by name in `SingleSearch.validate()`; EMPTY is the only shape that
    * reaches emission.
    *
    * 🔴 PUBLIC on purpose. `softclient4es-extensions` builds the materialized-view transform's
    * `TransformBucketSelectorConfig` from [[script]] and has no other way to tell "this clause has
    * nothing to filter" from "this clause cannot be expressed" -- which is #389's conflation, live
    * inside MV enrichment. This is the reason it needs, and the only thing core can give it until
    * it is rebuilt against a published `0.24.0`.
    */
  def unrepresentable: Seq[MetricSelector.Unrepresentable] =
    criteria.toSeq.flatMap(MetricSelectorScript.unrepresentable)

  /** The `bucket_selector` source for this clause, or `None` when there is nothing to filter.
    *
    * 🔴 NOT dead code -- round 1 recorded it as having no production caller and that was WRONG:
    * `softclient4es-extensions`'s `graph/Stage.scala:291` reads it to build a materialized view's
    * `TransformBucketSelectorConfig`. That is a FIFTH HAVING mechanism, outside this repo.
    *
    * ⚠️ It still answers `None` for a clause the engine cannot express, which is exactly the
    * conflation #389 closed everywhere else -- so a materialized view over such a HAVING is
    * enriched with NO filter. Core cannot fix that from here: switching this to
    * `MetricSelectorScript.metricSelector` would THROW inside the extension (a 500), and the
    * extension must decide for itself. [[unrepresentable]] is public so it can. See `§9` of the
    * story artifact (§10.B) for the exact change extensions needs once `0.24.0` is published.
    *
    * It reads every aggregate as SELECT returns it, through the ONE function the bridge's
    * `bucket_selector` reads too (`MetricSelectorScript.nullAwareSelectorScript`, #292): an empty
    * group's `MIN` / `MAX` / `AVG` / percentile is NULL here exactly as it is there. That read
    * declares no `buckets_path` variable of its own, so a view's `buckets_path` -- derived from the
    * clause, not from this script -- stays valid.
    */
  @deprecated(
    "read `unrepresentable` first, then `MetricSelectorScript.nullAwareSelectorScript`",
    "0.24.0"
  )
  def script: Option[String] = criteria.flatMap { criteria =>
    if (unrepresentable.nonEmpty) None
    else {
      // `None` when there is nothing to filter -- never a placeholder stripped out of the script,
      // which also ate `params.max_c1 == 1` (see `MetricSelectorScript.selectorScript`).
      MetricSelectorScript.nullAwareSelectorScript(criteria).map(_.trim).filter(_.nonEmpty)
    }
  }
}
