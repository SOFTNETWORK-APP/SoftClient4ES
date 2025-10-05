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
  NestedElement
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

          val nestedElements: Seq[NestedElement] = criteria.nestedElements.sortBy(_.level)

          val nestedParentsPath
            : collection.mutable.Map[String, (NestedElement, Seq[NestedElement])] =
            collection.mutable.Map.empty

          @tailrec
          def getNestedParents(
            n: NestedElement,
            parents: Seq[NestedElement]
          ): Seq[NestedElement] = {
            n.parent match {
              case Some(p) =>
                if (!nestedParentsPath.contains(p.path)) {
                  nestedParentsPath += p.path -> (p, Seq(n))
                  getNestedParents(p, p +: parents)
                } else {
                  nestedParentsPath += p.path -> (p, nestedParentsPath(p.path)._2 :+ n)
                  parents
                }
              case _ => parents
            }
          }

          val nestedParents = getNestedParents(nestedElements.last, Seq.empty)

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
            val children = nestedParentsPath.get(n.path).map(_._2).getOrElse(Seq.empty)
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
          if (nestedParents.nonEmpty)
            buildNestedQuery(nestedParents.head)
          else
            buildNestedQuery(nestedElements.last)
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
