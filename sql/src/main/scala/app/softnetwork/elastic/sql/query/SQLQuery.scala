package app.softnetwork.elastic.sql.query

case class SQLQuery(query: String, score: Option[Double] = None) {
  import app.softnetwork.elastic.sql.SQLImplicits._
  lazy val request: Option[Either[SQLSearchRequest, SQLMultiSearchRequest]] = {
    query
  }

  def minScore(score: Double): SQLQuery = this.copy(score = Some(score))
}
