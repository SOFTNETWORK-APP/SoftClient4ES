package app.softnetwork.elastic.sql

case class SQLSearchRequest(
  select: SQLSelect = SQLSelect(),
  from: SQLFrom,
  where: Option[SQLWhere],
  groupBy: Option[SQLGroupBy] = None,
  having: Option[SQLHaving] = None,
  orderBy: Option[SQLOrderBy] = None,
  limit: Option[SQLLimit] = None,
  score: Option[Double] = None
) extends SQLToken {
  override def sql: String =
    s"$select$from${asString(where)}${asString(groupBy)}${asString(having)}${asString(orderBy)}${asString(limit)}"

  lazy val fieldAliases: Map[String, String] = select.fieldAliases
  lazy val tableAliases: Map[String, String] = from.tableAliases
  lazy val unnests: Seq[(String, String, Option[SQLLimit])] = from.unnests

  def update(): SQLSearchRequest = {
    val updated = this.copy(from = from.update(this))
    updated.copy(
      select = select.update(updated),
      where = where.map(_.update(updated)),
      groupBy = groupBy.map(_.update(updated)),
      having = having.map(_.update(updated))
    )
  }

  lazy val fields: Seq[String] = {
    if (aggregates.isEmpty && buckets.isEmpty)
      select.fields.map(_.sourceField).filterNot(f => excludes.contains(f))
    else
      Seq.empty
  }

  lazy val aggregates: Seq[SQLField] = select.fields.filter(_.aggregation)

  lazy val excludes: Seq[String] = select.except.map(_.fields.map(_.sourceField)).getOrElse(Nil)

  lazy val sources: Seq[String] = from.tables.collect { case SQLTable(source: SQLIdentifier, _) =>
    source.sql
  }

  lazy val buckets: Seq[SQLBucket] = groupBy.map(_.buckets).getOrElse(Seq.empty)
}
