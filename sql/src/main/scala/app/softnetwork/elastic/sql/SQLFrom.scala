package app.softnetwork.elastic.sql

case object From extends SQLExpr("from") with SQLRegex

case object Unnest extends SQLExpr("unnest") with SQLRegex

case class SQLUnnest(identifier: SQLIdentifier, limit: Option[SQLLimit]) extends SQLSource {
  override def sql: String = s"$Unnest($identifier${asString(limit)})"
  def update(request: SQLSearchRequest): SQLUnnest =
    this.copy(identifier = identifier.update(request))
  override val name: String = identifier.name
}

case class SQLTable(source: SQLSource, tableAlias: Option[SQLAlias] = None) extends Updateable {
  override def sql: String = s"$source${asString(tableAlias)}"
  def update(request: SQLSearchRequest): SQLTable = this.copy(source = source.update(request))
}

case class SQLFrom(tables: Seq[SQLTable]) extends Updateable {
  override def sql: String = s" $From ${tables.map(_.sql).mkString(",")}"
  lazy val tableAliases: Map[String, String] = tables
    .flatMap((table: SQLTable) => table.tableAlias.map(alias => table.source.name -> alias.alias))
    .toMap
  lazy val unnests: Seq[(String, String, Option[SQLLimit])] = tables.collect {
    case SQLTable(u: SQLUnnest, a) =>
      (a.map(_.alias).getOrElse(u.identifier.name), u.identifier.name, u.limit)
  }
  def update(request: SQLSearchRequest): SQLFrom =
    this.copy(tables = tables.map(_.update(request)))

  override def validate(): Either[String, Unit] = {
    if (tables.isEmpty) {
      Left("At least one table is required in FROM clause")
    } else {
      Right(())
    }
  }
}
