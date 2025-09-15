package app.softnetwork.elastic.sql.bridge

import app.softnetwork.elastic.sql.{
  ElasticBoolQuery,
  ElasticChild,
  ElasticFilter,
  ElasticGeoDistance,
  ElasticMatch,
  ElasticNested,
  ElasticParent,
  SQLBetween,
  SQLExpression,
  SQLIn,
  SQLIsNotNull,
  SQLIsNotNullCriteria,
  SQLIsNull,
  SQLIsNullCriteria
}
import com.sksamuel.elastic4s.ElasticApi._
import com.sksamuel.elastic4s.requests.searches.queries.Query

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
          nestedQuery(
            relationType.getOrElse(""),
            criteria
              .asFilter(boolQuery)
              .query(innerHitsNames + innerHitsName.getOrElse(""), boolQuery)
          ) /*.scoreMode(ScoreMode.None)*/
            .inner(
              innerHits(innerHitsName.getOrElse("")).from(0).size(limit.map(_.limit).getOrElse(3))
            )
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
      case expression: SQLExpression       => expression
      case isNull: SQLIsNull               => isNull
      case isNotNull: SQLIsNotNull         => isNotNull
      case in: SQLIn[_, _]                 => in
      case between: SQLBetween[String]     => between
      case between: SQLBetween[Long]       => between
      case between: SQLBetween[Double]     => between
      case geoDistance: ElasticGeoDistance => geoDistance
      case matchExpression: ElasticMatch   => matchExpression
      case isNull: SQLIsNullCriteria       => isNull
      case isNotNull: SQLIsNotNullCriteria => isNotNull
      case other =>
        throw new IllegalArgumentException(s"Unsupported filter type: ${other.getClass.getName}")
    }
  }
}
