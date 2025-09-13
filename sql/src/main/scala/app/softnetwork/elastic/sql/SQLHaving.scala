package app.softnetwork.elastic.sql

case object Having extends SQLExpr("having") with SQLRegex

case class SQLHaving(criteria: Option[SQLCriteria]) extends Updateable {
  override def sql: String = criteria match {
    case Some(c) => s" $Having $c"
    case _       => ""
  }
  def update(request: SQLSearchRequest): SQLHaving =
    this.copy(criteria = criteria.map(_.update(request)))

  override def validate(): Either[String, Unit] = criteria.map(_.validate()).getOrElse(Right(()))
}
