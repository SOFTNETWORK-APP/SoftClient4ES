package app.softnetwork.elastic.sql.bridge

import app.softnetwork.elastic.sql.{Field, SQLBucket, SQLCriteria, SQLExcept}
import com.sksamuel.elastic4s.searches.SearchRequest
import com.sksamuel.elastic4s.http.search.SearchBodyBuilderFn

case class ElasticSearchRequest(
  fields: Seq[Field],
  except: Option[SQLExcept],
  sources: Seq[String],
  criteria: Option[SQLCriteria],
  limit: Option[Int],
  search: SearchRequest,
  buckets: Seq[SQLBucket] = Seq.empty,
  aggregations: Seq[ElasticAggregation] = Seq.empty
) {
  def minScore(score: Option[Double]): ElasticSearchRequest = {
    score match {
      case Some(s) => this.copy(search = search minScore s)
      case _       => this
    }
  }

  def query: String =
    SearchBodyBuilderFn(search).string.replace("\"version\":true,", "") /*FIXME*/
}
