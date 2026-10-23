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

package app.softnetwork.elastic.sql.bridge

import app.softnetwork.elastic.sql.{PainlessContextType, PainlessTarget}
import app.softnetwork.elastic.sql.operator.AND
import app.softnetwork.elastic.sql.query.{
  BetweenExpr,
  Criteria,
  ElasticBoolQuery,
  ElasticChild,
  ElasticFilter,
  ElasticNested,
  ElasticParent,
  ElasticRelation,
  Expression,
  GenericExpression,
  InExpr,
  IsNotNullCriteria,
  IsNotNullExpr,
  IsNullCriteria,
  IsNullExpr,
  MatchAllCriteria,
  MatchCriteria,
  MatchNoneCriteria,
  MultiMatchCriteria,
  NestedElement,
  NestedElements,
  Predicate,
  SubqueryCriteria
}
import com.sksamuel.elastic4s.ElasticApi._
import com.sksamuel.elastic4s.FetchSourceContext
import com.sksamuel.elastic4s.searches.queries.{InnerHit, Query}

import scala.annotation.tailrec

object ElasticBridge {

  /** The nested path of each column a condition reads, the first segment of its dotted name, for a
    * `NESTED(…)` no JOIN UNNEST declares (`ElasticBridge.query`). A column with no dot has none.
    */
  private[bridge] def nestedPaths(criteria: Criteria): List[String] = criteria match {
    case Predicate(left, _, right, _, _) => nestedPaths(left) ++ nestedPaths(right)
    case relation: ElasticRelation       => nestedPaths(relation.criteria)
    case m: MultiMatchCriteria           => nestedPaths(m.criteria)
    case e: Expression =>
      e.identifier.name.split('.').toList match {
        case path :: _ :: _ => List(path)
        case _              => Nil
      }
    case _ => Nil
  }

  /** The columns a MATCH reads, when `filter` is one: a `MATCH (c) AGAINST (…)`, or the group of
    * one per column that `MATCH (c1, c2) AGAINST (…)` is. `None` for any other filter.
    */
  private[bridge] def matchColumns(filter: ElasticFilter): Option[Seq[String]] = filter match {
    case m: MatchCriteria => Some(Seq(m.identifier.name))
    case b: ElasticBoolQuery
        if b.shouldFilters.nonEmpty && b.innerFilters.isEmpty && b.mustFilters.isEmpty &&
          b.notFilters.isEmpty && b.shouldFilters.forall(_.isInstanceOf[MatchCriteria]) =>
      Some(b.shouldFilters.collect { case m: MatchCriteria => m.identifier.name })
    case _ => None
  }
}

case class ElasticBridge(filter: ElasticFilter) {
  def query(
    innerHitsNames: Set[String] = Set.empty,
    currentQuery: Option[ElasticBoolQuery]
  )(implicit
    timestamp: Long,
    contextType: PainlessContextType = PainlessContextType.Query,
    target: PainlessTarget = PainlessTarget.Default
  ): Query = {
    filter match {
      case boolQuery: ElasticBoolQuery =>
        import boolQuery._
        // 🔴 A negated MATCH -- the one leaf that reaches `must_not` as it is, having no negated
        // spelling of its own -- keeps only the documents where its columns have a value, as SQL
        // reads `NOT` over a NULL: the negation stands beside each column's `exists`
        // (`negationOnAll`), in this bool's `filter`. Not where this bool's `should` clauses are
        // required (no `filter` or `must` beside them), which a `filter` would make optional. A
        // relation keeps its `must_not`: `NOT NESTED(…)` asks whether NO child matches.
        val shouldsRequired = shouldFilters.nonEmpty && innerFilters.isEmpty && mustFilters.isEmpty
        val (negatedMatches, negated) =
          if (shouldsRequired) (Nil, notFilters)
          else notFilters.partition(f => ElasticBridge.matchColumns(f).isDefined)
        bool(
          mustFilters.map(implicitly[ElasticBridge](_).query(innerHitsNames, currentQuery)),
          shouldFilters.map(implicitly[ElasticBridge](_).query(innerHitsNames, currentQuery)),
          negated.map(implicitly[ElasticBridge](_).query(innerHitsNames, currentQuery))
        )
          .filter(
            innerFilters.map(_.query(innerHitsNames, currentQuery)) ++ negatedMatches.map(f =>
              negationOnAll(
                ElasticBridge.matchColumns(f).getOrElse(Nil),
                implicitly[ElasticBridge](f).query(innerHitsNames, currentQuery)
              )
            )
          )
      case nested: ElasticNested =>
        import nested._
        if (innerHitsNames.contains(innerHitsName.getOrElse(""))) {
          criteria.asFilter(currentQuery).query(innerHitsNames, currentQuery)
        } else {
          NestedElements.buildNestedTrees(criteria.nestedElements) match {
            case Nil =>
              // 🔴 A `NESTED(…)` no JOIN UNNEST declares has no nested element: its path is the
              // first segment of the columns it names (`items.sku` -> `items`), the segment its
              // inner-hits name is read from. It rendered `match_all` here, so `WHERE
              // NESTED(items.sku = 's1')` returned EVERY document, HTTP 200. It is now the nested
              // query over that path -- the documents one of whose elements satisfies the
              // condition, as DuckDB answers `EXISTS (SELECT 1 FROM unnest(items) …)` -- with no
              // inner hits (no element is projected), tolerating an index that does not map the
              // path, where no element can match. Columns of several paths in one `NESTED` are
              // refused by name; a condition naming no dotted column is applied as it is.
              val boolQuery = Option(ElasticBoolQuery(group = true))
              val q = criteria
                .asFilter(boolQuery)
                .query(innerHitsNames + innerHitsName.getOrElse(""), boolQuery)
              ElasticBridge.nestedPaths(criteria).distinct match {
                case Nil => q
                case path :: Nil =>
                  nestedQuery(path, q).copy(ignoreUnmapped = Some(true))
                case paths =>
                  throw new IllegalArgumentException(
                    s"NESTED(${criteria.sql}) reads the nested paths ${paths.mkString(", ")}: " +
                    "without a JOIN UNNEST a NESTED condition reads one path. Write one NESTED " +
                    "per path."
                  )
              }
            case nestedTrees =>
              def nestedInner(n: NestedElement): InnerHit = {
                var inner = innerHits(n.innerHitsName)
                n.size match {
                  case Some(s) =>
                    inner = inner.from(0).size(s)
                  case _ =>
                }
                if (n.sources.nonEmpty) {
                  inner = inner.docValueFields(
                    n.sources.map { source =>
                      (n.path.split('.').toSeq ++ Seq(source)).mkString(".")
                    }
                  )
                }
                inner
              }

              def buildNestedQuery(n: NestedElement, q: Query): Query = {
                val children = n.children
                if (children.nonEmpty) {
                  val innerQueries = children.map(child => buildNestedQuery(child, q))
                  val combinedQuery = if (innerQueries.size == 1) {
                    innerQueries.head
                  } else {
                    must(innerQueries)
                  }
                  nestedQuery(
                    n.path,
                    combinedQuery
                  ) /*.scoreMode(ScoreMode.None)*/
                    .inner(
                      nestedInner(n)
                    )
                    .copy(ignoreUnmapped = n.ignoreUnmapped)
                } else {
                  nestedQuery(
                    n.path,
                    q
                  ) /*.scoreMode(ScoreMode.None)*/
                    .inner(
                      nestedInner(n)
                    )
                    .copy(ignoreUnmapped = n.ignoreUnmapped)
                }
              }

              criteria match {
                case p: Predicate if nestedTrees.size > 1 =>
                  val leftNested = ElasticNested(p.leftCriteria, p.leftCriteria.limit)
                  val leftBoolQuery = Option(ElasticBoolQuery(group = true))
                  val leftQuery = ElasticBridge(leftNested)
                    .query(innerHitsNames /*++ leftNested.innerHitsName.toSet*/, leftBoolQuery)

                  // 🔴 NOT-FOLD: this consumer of `Predicate.not` deliberately does NOT fold.
                  //
                  // Round 10 (LOW-1) proposed aligning this site with the other three by reading
                  // `Predicate.emittedRight`. That was tried and MEASURED WRONG: under a nested
                  // relation the right criterion is evaluated over a CHILD document and the query
                  // wraps it in an EXISTENTIAL, so `NOT` outside and `NOT` inside are different
                  // questions. `WHERE MATCH(comments.content) AGAINST ('Nice') AND NOT
                  // replies.lastUpdated < LAST_DAY(…)` means "no reply is before that date", which
                  // includes a blog with NO replies; folded, it became "SOME reply is not before
                  // it", which excludes that blog and admits one with replies on both sides. The
                  // `SQLQuerySpec` "predicate with distinct nested" fixture caught the flip:
                  // `must_not[nested(…)] + filter[…]` became `must[nested(…), nested(… == false)]`.
                  //
                  // So the fold applies where the negated criterion is evaluated over the SAME
                  // document, and not here. Enumerated on `Predicate.emittedRight`.
                  val rightNested = ElasticNested(p.rightCriteria, p.rightCriteria.limit)
                  val rightBoolQuery = Option(ElasticBoolQuery(group = true))
                  val rightQuery = ElasticBridge(rightNested)
                    .query(innerHitsNames /*++ rightNested.innerHitsName.toSet*/, rightBoolQuery)

                  val negate = p.not.isDefined
                  p.operator match {
                    case AND =>
                      if (negate) not(rightQuery).filter(leftQuery)
                      else must(leftQuery, rightQuery)
                    case _ =>
                      if (negate) not(rightQuery).should(leftQuery)
                      else should(leftQuery, rightQuery)
                  }
                case _ =>
                  val boolQuery = Option(ElasticBoolQuery(group = true))
                  val q = criteria
                    .asFilter(boolQuery)
                    .query(innerHitsNames + innerHitsName.getOrElse(""), boolQuery)
                  if (nestedTrees.size == 1) {
                    buildNestedQuery(nestedTrees.head, q)
                  } else {
                    val innerQueries = nestedTrees.map(nested => buildNestedQuery(nested, q))
                    must(innerQueries)
                  }
              }
          }
        }
      case child: ElasticChild =>
        import child._
        hasChildQuery(
          relationType.getOrElse(""),
          criteria.asQuery(group = group, innerHitsNames = innerHitsNames)
        ).copy(ignoreUnmapped = child.ignoreUnmapped)
      case parent: ElasticParent =>
        import parent._
        hasParentQuery(
          relationType.getOrElse(""),
          criteria.asQuery(group = group, innerHitsNames = innerHitsNames),
          score = false
        ).copy(ignoreUnmapped = parent.ignoreUnmapped)
      case expression: GenericExpression => expression
      case isNull: IsNullExpr            => isNull
      case isNotNull: IsNotNullExpr      => isNotNull
      case in: InExpr[_, _]              => in
      case between: BetweenExpr          => between
      // case geoDistance: DistanceCriteria => geoDistance
      case matchExpression: MatchCriteria => matchExpression
      case isNull: IsNullCriteria         => isNull
      case isNotNull: IsNotNullCriteria   => isNotNull
      // Story 22.2 — the two RESOLVED sentinels a WHERE subquery collapses to.
      case _: MatchAllCriteria  => matchAllQuery()
      case _: MatchNoneCriteria => matchNoneQuery()
      // 🔴 Story 22.2 — an UNRESOLVED subquery node must never reach a bridge. It is replaced by a
      // literal criteria at `SearchApi.resolveWithSchema` (the ONE seam), so arriving here means a
      // statement was handed straight to `singleSearch` / `singleSearchToJsonQuery`, which bypass
      // it. Named rather than swallowed by the `Unsupported filter type` default below, because
      // the alternative — emitting nothing for the predicate — is a silent wrong answer.
      case s: SubqueryCriteria =>
        throw new IllegalArgumentException(
          s"Unresolved WHERE subquery reached the query builder: ${s.sql}. Subqueries are " +
          "executed at SearchApi.resolveWithSchema before translation; a statement handed " +
          "straight to singleSearch / singleSearchToJsonQuery bypasses it."
        )
      case other =>
        throw new IllegalArgumentException(s"Unsupported filter type: ${other.getClass.getName}")
    }
  }
}
