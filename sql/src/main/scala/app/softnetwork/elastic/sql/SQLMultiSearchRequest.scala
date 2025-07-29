package app.softnetwork.elastic.sql

case class SQLMultiSearchRequest(requests: Seq[SQLSearchRequest]) extends SQLToken {
  override def sql: String = s"${requests.map(_.sql).mkString(" union ")}"

  def update(): SQLMultiSearchRequest = this.copy(requests = requests.map(_.update()))

}
