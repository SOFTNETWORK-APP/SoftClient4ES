package app.softnetwork.elastic.sql.query

import app.softnetwork.elastic.sql.{
  asString,
  Alias,
  Expr,
  Identifier,
  Source,
  TokenRegex,
  Updateable
}

case object From extends Expr("from") with TokenRegex

case object Unnest extends Expr("unnest") with TokenRegex

case class Unnest(identifier: Identifier, limit: Option[Limit]) extends Source {
  override def sql: String = s"$Unnest($identifier${asString(limit)})"
  def update(request: SQLSearchRequest): Unnest =
    this.copy(identifier = identifier.update(request))
  override val name: String = identifier.name
}

case class Table(source: Source, tableAlias: Option[Alias] = None) extends Updateable {
  override def sql: String = s"$source${asString(tableAlias)}"
  def update(request: SQLSearchRequest): Table = this.copy(source = source.update(request))
}

case class From(tables: Seq[Table]) extends Updateable {
  override def sql: String = s" $From ${tables.map(_.sql).mkString(",")}"
  lazy val tableAliases: Map[String, String] = tables
    .flatMap((table: Table) => table.tableAlias.map(alias => table.source.name -> alias.alias))
    .toMap
  lazy val unnests: Seq[(String, String, Option[Limit])] = tables.collect {
    case Table(u: Unnest, a) =>
      (a.map(_.alias).getOrElse(u.identifier.name), u.identifier.name, u.limit)
  }
  def update(request: SQLSearchRequest): From =
    this.copy(tables = tables.map(_.update(request)))

  override def validate(): Either[String, Unit] = {
    if (tables.isEmpty) {
      Left("At least one table is required in FROM clause")
    } else {
      Right(())
    }
  }
}
