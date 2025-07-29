package app.softnetwork.elastic.client.sql.bridge

import com.sksamuel.elastic4s.requests.searches.{MultiSearchBuilderFn, MultiSearchRequest}

case class ElasticMultiSearchRequest(
  requests: Seq[ElasticSearchRequest],
  multiSearch: MultiSearchRequest
) {
  def query: String = MultiSearchBuilderFn(multiSearch).replace("\"version\":true,", "") /*FIXME*/
}
