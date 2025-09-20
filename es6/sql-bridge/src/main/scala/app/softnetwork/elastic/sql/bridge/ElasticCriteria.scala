package app.softnetwork.elastic.sql.bridge

import app.softnetwork.elastic.sql.Criteria
import com.sksamuel.elastic4s.searches.queries.Query

case class ElasticCriteria(criteria: Criteria) {

  def asQuery(group: Boolean = true, innerHitsNames: Set[String] = Set.empty): Query = {
    val query = criteria.boolQuery.copy(group = group)
    query
      .filter(criteria.asFilter(Option(query)))
      .unfilteredMatchCriteria()
      .query(innerHitsNames, Option(query))
  }
}
