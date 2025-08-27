package app.softnetwork.elastic.sql

case object GroupBy extends SQLExpr("group by") with SQLRegex

case object Having extends SQLExpr("having") with SQLRegex

case class SQLHaving(criteria: Option[SQLCriteria]) extends Updateable {
  override def sql: String = criteria match {
    case Some(c) => s" $Having $c"
    case _       => ""
  }
  def update(request: SQLSearchRequest): SQLHaving =
    this.copy(criteria = criteria.map(_.update(request)))
}

case class SQLGroupBy(buckets: Seq[SQLBucket], having: Option[SQLHaving] = None)
    extends Updateable {
  override def sql: String = s" $GroupBy ${buckets.mkString(",")}${asString(having)}"
  def update(request: SQLSearchRequest): SQLGroupBy =
    this.copy(buckets = buckets.map(_.update(request)), having = having.map(_.update(request)))
}

case class SQLBucket(
  identifier: SQLIdentifier
) extends Updateable {
  override def sql: String = s"$identifier"
  def update(request: SQLSearchRequest): SQLBucket =
    this.copy(identifier = identifier.update(request))
  lazy val sourceBucket: String =
    if (identifier.nested) {
      identifier.alias
        .map(a => s"$a.")
        .getOrElse("") + identifier.columnName.split("\\.").tail.mkString(".")
    } else {
      identifier.columnName
    }
  lazy val name: String = identifier.alias.getOrElse(sourceBucket.replace(".", "_"))
}
