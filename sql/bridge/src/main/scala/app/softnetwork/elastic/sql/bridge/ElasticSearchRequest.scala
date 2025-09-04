package app.softnetwork.elastic.sql.bridge

import app.softnetwork.elastic.sql.{SQLBucket, SQLCriteria, SQLExcept, Field}
import com.sksamuel.elastic4s.requests.searches.{SearchBodyBuilderFn, SearchRequest}

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
