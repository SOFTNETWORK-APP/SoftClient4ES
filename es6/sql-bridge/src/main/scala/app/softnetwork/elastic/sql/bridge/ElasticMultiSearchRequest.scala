package app.softnetwork.elastic.sql.bridge

import com.sksamuel.elastic4s.http.search.MultiSearchBuilderFn
import com.sksamuel.elastic4s.searches.MultiSearchRequest

case class ElasticMultiSearchRequest(
  requests: Seq[ElasticSearchRequest],
  multiSearch: MultiSearchRequest
) {
  def query: String = MultiSearchBuilderFn(multiSearch).replace("\"version\":true,", "") /*FIXME*/
}
