package app.softnetwork.elastic.sql

case object GroupBy extends SQLExpr("group by") with SQLRegex

case class SQLGroupBy(buckets: Seq[SQLBucket]) extends Updateable {
  override def sql: String = s" $GroupBy ${buckets.mkString(",")}"
  def update(request: SQLSearchRequest): SQLGroupBy =
    this.copy(buckets = buckets.map(_.update(request)))
}

case class SQLBucket(
  identifier: SQLIdentifier
) extends Updateable {
  override def sql: String = s"$identifier"
  def update(request: SQLSearchRequest): SQLBucket =
    this.copy(identifier = identifier.update(request))
  lazy val sourceBucket: String =
    if (identifier.nested) {
      identifier.tableAlias
        .map(a => s"$a.")
        .getOrElse("") + identifier.name.split("\\.").tail.mkString(".")
    } else {
      identifier.name
    }
  lazy val name: String = identifier.fieldAlias.getOrElse(sourceBucket.replace(".", "_"))
}
