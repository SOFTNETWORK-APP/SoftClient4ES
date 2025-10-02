package app.softnetwork.elastic.sql.query

import app.softnetwork.elastic.sql.{Expr, TokenRegex, Updateable}

case object Having extends Expr("HAVING") with TokenRegex

case class Having(criteria: Option[Criteria]) extends Updateable {
  override def sql: String = criteria match {
    case Some(c) => s" $Having $c"
    case _       => ""
  }
  def update(request: SQLSearchRequest): Having =
    this.copy(criteria = criteria.map(_.update(request)))

  override def validate(): Either[String, Unit] = criteria.map(_.validate()).getOrElse(Right(()))
}
