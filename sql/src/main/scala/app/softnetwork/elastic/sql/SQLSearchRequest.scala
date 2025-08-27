package app.softnetwork.elastic.sql

case class SQLSearchRequest(
  select: SQLSelect = SQLSelect(),
  from: SQLFrom,
  where: Option[SQLWhere],
  groupBy: Option[SQLGroupBy] = None,
  orderBy: Option[SQLOrderBy] = None,
  limit: Option[SQLLimit] = None,
  score: Option[Double] = None
) extends SQLToken {
  override def sql: String =
    s"$select$from${asString(where)}${asString(groupBy)}${asString(orderBy)}${asString(limit)}"

  lazy val aliases: Seq[String] = from.aliases
  lazy val unnests: Seq[(String, String, Option[SQLLimit])] = from.unnests

  def update(): SQLSearchRequest = {
    val updated = this.copy(from = from.update(this))
    updated.copy(select = select.update(updated), where = where.map(_.update(updated)))
  }

  lazy val fields: Seq[String] =
    select.fields
      .filterNot {
        case _: SQLAggregate => true
        case _               => false
      }
      .map(_.sourceField)

  lazy val aggregates: Seq[SQLAggregate] = select.fields.collect { case a: SQLAggregate => a }

  lazy val excludes: Seq[String] = select.except.map(_.fields.map(_.sourceField)).getOrElse(Nil)

  lazy val sources: Seq[String] = from.tables.collect { case SQLTable(source: SQLIdentifier, _) =>
    source.sql
  }

  lazy val buckets: Option[Seq[SQLBucket]] = groupBy.map(_.buckets)
}
