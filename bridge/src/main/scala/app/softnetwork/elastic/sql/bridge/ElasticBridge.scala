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

import app.softnetwork.elastic.sql.PainlessContextType
import app.softnetwork.elastic.sql.operator.AND
import app.softnetwork.elastic.sql.query.{
  BetweenExpr,
  ElasticBoolQuery,
  ElasticChild,
  ElasticFilter,
  ElasticNested,
  ElasticParent,
  GenericExpression,
  InExpr,
  IsNotNullCriteria,
  IsNotNullExpr,
  IsNullCriteria,
  IsNullExpr,
  MatchCriteria,
  NestedElement,
  NestedElements,
  Predicate
}
import com.sksamuel.elastic4s.ElasticApi._
import com.sksamuel.elastic4s.requests.searches.queries.{InnerHit, Query}

case class ElasticBridge(filter: ElasticFilter) {
  def query(
    innerHitsNames: Set[String] = Set.empty,
    currentQuery: Option[ElasticBoolQuery]
  )(implicit
    timestamp: Long,
    contextType: PainlessContextType = PainlessContextType.Query
  ): Query = {
    filter match {
      case boolQuery: ElasticBoolQuery =>
        import boolQuery._
        bool(
          mustFilters.map(implicitly[ElasticBridge](_).query(innerHitsNames, currentQuery)),
          shouldFilters.map(implicitly[ElasticBridge](_).query(innerHitsNames, currentQuery)),
          notFilters.map(implicitly[ElasticBridge](_).query(innerHitsNames, currentQuery))
        )
          .filter(innerFilters.map(_.query(innerHitsNames, currentQuery)))
      case nested: ElasticNested =>
        import nested._
        if (innerHitsNames.contains(innerHitsName.getOrElse(""))) {
          criteria.asFilter(currentQuery).query(innerHitsNames, currentQuery)
        } else {
          val boolQuery = Option(ElasticBoolQuery(group = true))
          val q = criteria
            .asFilter(boolQuery)
            .query(innerHitsNames + innerHitsName.getOrElse(""), boolQuery)

          NestedElements.buildNestedTrees(criteria.nestedElements) match {
            case Nil =>
              matchAllQuery()
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
                } else {
                  nestedQuery(
                    n.path,
                    q
                  ) /*.scoreMode(ScoreMode.None)*/
                    .inner(
                      nestedInner(n)
                    )
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
        )
      case parent: ElasticParent =>
        import parent._
        hasParentQuery(
          relationType.getOrElse(""),
          criteria.asQuery(group = group, innerHitsNames = innerHitsNames),
          score = false
        )
      case expression: GenericExpression => expression
      case isNull: IsNullExpr            => isNull
      case isNotNull: IsNotNullExpr      => isNotNull
      case in: InExpr[_, _]              => in
      case between: BetweenExpr          => between
      // case geoDistance: DistanceCriteria => geoDistance
      case matchExpression: MatchCriteria => matchExpression
      case isNull: IsNullCriteria         => isNull
      case isNotNull: IsNotNullCriteria   => isNotNull
      case other =>
        throw new IllegalArgumentException(s"Unsupported filter type: ${other.getClass.getName}")
    }
  }
}
