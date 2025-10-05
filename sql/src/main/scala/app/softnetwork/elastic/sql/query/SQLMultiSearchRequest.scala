package app.softnetwork.elastic.sql.query

import app.softnetwork.elastic.sql.Token

case class SQLMultiSearchRequest(requests: Seq[SQLSearchRequest]) extends Token {
  override def sql: String = s"${requests.map(_.sql).mkString(" union ")}"

  def update(): SQLMultiSearchRequest = this.copy(requests = requests.map(_.update()))

  override def validate(): Either[String, Unit] = {
    requests.map(_.validate()).filter(_.isLeft) match {
      case Nil    => Right(())
      case errors => Left(errors.map { case Left(err) => err }.mkString("\n"))
    }
  }
}
