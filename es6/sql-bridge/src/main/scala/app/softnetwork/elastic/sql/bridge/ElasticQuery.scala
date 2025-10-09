package app.softnetwork.elastic.sql.bridge

import app.softnetwork.elastic.sql.query.{
  BetweenExpr,
  ElasticBoolQuery,
  ElasticChild,
  ElasticFilter,
  ElasticMatch,
  ElasticNested,
  ElasticParent,
  GenericExpression,
  InExpr,
  IsNotNullCriteria,
  IsNotNullExpr,
  IsNullCriteria,
  IsNullExpr,
  NestedElement,
  NestedElements
}
import com.sksamuel.elastic4s.ElasticApi._
import com.sksamuel.elastic4s.FetchSourceContext
import com.sksamuel.elastic4s.searches.queries.{InnerHit, Query}

import scala.annotation.tailrec

case class ElasticQuery(filter: ElasticFilter) {
  def query(
    innerHitsNames: Set[String] = Set.empty,
    currentQuery: Option[ElasticBoolQuery]
  ): Query = {
    filter match {
      case boolQuery: ElasticBoolQuery =>
        import boolQuery._
        bool(
          mustFilters.map(implicitly[ElasticQuery](_).query(innerHitsNames, currentQuery)),
          shouldFilters.map(implicitly[ElasticQuery](_).query(innerHitsNames, currentQuery)),
          notFilters.map(implicitly[ElasticQuery](_).query(innerHitsNames, currentQuery))
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
            case nestedElements =>
              def nestedInner(n: NestedElement): InnerHit = {
                var inner = innerHits(n.innerHitsName)
                n.size match {
                  case Some(s) =>
                    inner = inner.from(0).size(s)
                  case _ =>
                }
                if (n.sources.nonEmpty) {
                  inner = inner.fetchSource(
                    FetchSourceContext(
                      fetchSource = true,
                      includes = n.sources.toArray
                    )
                  )
                }
                inner
              }

              def buildNestedQuery(n: NestedElement): Query = {
                val children = n.children
                if (children.nonEmpty) {
                  val innerQueries = children.map(buildNestedQuery)
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

              if (nestedElements.size == 1) {
                buildNestedQuery(nestedElements.head)
              } else {
                val innerQueries = nestedElements.map(buildNestedQuery)
                must(innerQueries)
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
      case matchExpression: ElasticMatch => matchExpression
      case isNull: IsNullCriteria        => isNull
      case isNotNull: IsNotNullCriteria  => isNotNull
      case other =>
        throw new IllegalArgumentException(s"Unsupported filter type: ${other.getClass.getName}")
    }
  }
}
