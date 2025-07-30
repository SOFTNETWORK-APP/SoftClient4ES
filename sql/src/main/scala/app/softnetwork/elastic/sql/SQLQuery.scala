package app.softnetwork.elastic.sql

case class SQLQuery(query: String, score: Option[Double] = None) {
  import SQLImplicits._
  lazy val request: Option[Either[SQLSearchRequest, SQLMultiSearchRequest]] = {
    query
  }

  def minScore(score: Double): SQLQuery = this.copy(score = Some(score))
}
