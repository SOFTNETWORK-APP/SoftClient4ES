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

case object From extends Expr("FROM") with TokenRegex

sealed trait JoinType extends TokenRegex

case object InnerJoin extends Expr("INNER") with JoinType

case object LeftJoin extends Expr("LEFT") with JoinType

case object RightJoin extends Expr("RIGHT") with JoinType

case object FullJoin extends Expr("FULL") with JoinType

case object CrossJoin extends Expr("CROSS") with JoinType

case object On extends Expr("ON") with TokenRegex

case class On(criteria: Criteria) extends Updateable {
  override def sql: String = s" $On $criteria"
  def update(request: SQLSearchRequest): On = this.copy(criteria = criteria.update(request))
}

case object Join extends Expr("JOIN") with TokenRegex

sealed trait Join extends Updateable {
  def source: Source
  def joinType: Option[JoinType]
  def on: Option[On]
  def alias: Option[Alias]
  override def sql: String =
    s"${asString(joinType)} $Join $source${asString(on)}"

  override def update(request: SQLSearchRequest): Join
}

case object Unnest extends Expr("UNNEST") with TokenRegex

case class Unnest(identifier: Identifier, limit: Option[Limit], alias: Option[Alias] = None)
    extends Source
    with Join {
  override def sql: String = s"$Join $Unnest($identifier${asString(limit)})"
  def update(request: SQLSearchRequest): Unnest =
    this.copy(identifier = identifier.update(request))
  override val name: String = identifier.name

  override def source: Source = this

  override def joinType: Option[JoinType] = None

  override def on: Option[On] = None
}

case class Table(name: String, tableAlias: Option[Alias] = None, joins: Seq[Join] = Nil)
    extends Source {
  override def sql: String = s"$name${asString(tableAlias)}${joins.map(_.sql).mkString(" ")}"
  def update(request: SQLSearchRequest): Table = this.copy(joins = joins.map(_.update(request)))
}

case class From(tables: Seq[Table]) extends Updateable {
  override def sql: String = s" $From ${tables.map(_.sql).mkString(",")}"
  lazy val tableAliases: Map[String, String] = tables
    .flatMap((table: Table) => table.tableAlias.map(alias => table.name -> alias.alias))
    .toMap ++ unnests.map(unnest => unnest._2 -> unnest._1).toMap
  lazy val unnests: Seq[(String, String, Option[Limit])] = tables
    .map(_.joins)
    .collect { case j =>
      j.collect { case u: Unnest => // extract unnest info
        (u.alias.map(_.alias).getOrElse(u.identifier.name), u.identifier.name, u.limit)
      }
    }
    .flatten
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
