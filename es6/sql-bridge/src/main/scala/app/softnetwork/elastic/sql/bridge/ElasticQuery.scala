package app.softnetwork.elastic.sql.bridge

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
                  inner = inner.fetchSource(
                    FetchSourceContext(
                      fetchSource = true,
                      includes = n.sources.map { source =>
                        (n.path.split('.').toSeq ++ Seq(source)).mkString(".")
                      }.toArray
                    )
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
                  val leftQuery = ElasticQuery(leftNested)
                    .query(innerHitsNames /*++ leftNested.innerHitsName.toSet*/, leftBoolQuery)

                  val rightNested = ElasticNested(p.rightCriteria, p.rightCriteria.limit)
                  val rightBoolQuery = Option(ElasticBoolQuery(group = true))
                  val rightQuery = ElasticQuery(rightNested)
                    .query(innerHitsNames /*++ rightNested.innerHitsName.toSet*/, rightBoolQuery)

                  p.operator match {
                    case AND =>
                      p.not match {
                        case Some(_) => not(rightQuery).filter(leftQuery)
                        case _       => must(leftQuery, rightQuery)
                      }
                    case _ =>
                      p.not match {
                        case Some(_) => not(rightQuery).should(leftQuery)
                        case _       => should(leftQuery, rightQuery)
                      }
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
