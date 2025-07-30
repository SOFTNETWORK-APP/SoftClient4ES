package app.softnetwork.elastic.sql.bridge

import app.softnetwork.elastic.sql.{SQLCriteria, SQLExcept, SQLField}
import com.sksamuel.elastic4s.requests.searches.{SearchBodyBuilderFn, SearchRequest}

case class ElasticSearchRequest(
  fields: Seq[SQLField],
  except: Option[SQLExcept],
  sources: Seq[String],
  criteria: Option[SQLCriteria],
  limit: Option[Int],
  search: SearchRequest,
  aggregations: Seq[ElasticAggregation] = Seq.empty
) {
  def minScore(score: Option[Double]): ElasticSearchRequest = {
    score match {
      case Some(s) => this.copy(search = search minScore s)
      case _       => this
    }
  }

  def query: String =
    SearchBodyBuilderFn(search).string().replace("\"version\":true,", "") /*FIXME*/
}
