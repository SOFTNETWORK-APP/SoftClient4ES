package app.softnetwork.elastic.sql

case object From extends SQLExpr("from") with SQLRegex

sealed trait SQLSource extends Updateable {
  def update(request: SQLSearchRequest): SQLSource
}

case class SQLIdentifier(
  columnName: String,
  alias: Option[String] = None,
  distinct: Boolean = false,
  nested: Boolean = false,
  limit: Option[SQLLimit] = None,
  function: Option[SQLFunction] = None
) extends SQLExpr({
      var parts: Seq[String] = columnName.split("\\.").toSeq
      alias match {
        case Some(a) => parts = a +: parts
        case _       =>
      }
      val sql = {
        if (distinct) {
          s"$Distinct ${parts.mkString(".")}".trim
        } else {
          parts.mkString(".").trim
        }
      }
      function match {
        case Some(f) => s"$f($sql)"
        case _       => sql
      }
    })
    with SQLSource
    with SQLTokenWithFunction {

  lazy val aggregationName: Option[String] = if (aggregation) alias else None

  lazy val nestedType: Option[String] = if (nested) Some(columnName.split('.').head) else None

  lazy val innerHitsName: Option[String] = if (nested) alias else None

  def update(request: SQLSearchRequest): SQLIdentifier = {
    val parts: Seq[String] = columnName.split("\\.").toSeq
    if (request.aliases.contains(parts.head)) {
      request.unnests.find(_._1 == parts.head) match {
        case Some(tuple) =>
          this.copy(
            alias = Some(parts.head),
            columnName = s"${tuple._2}.${parts.tail.mkString(".")}",
            nested = true,
            limit = tuple._3
          )
        case _ =>
          this.copy(
            alias = Some(parts.head),
            columnName = parts.tail.mkString(".")
          )
      }
    } else {
      this
    }
  }
}

case object Unnest extends SQLExpr("unnest") with SQLRegex

case class SQLUnnest(identifier: SQLIdentifier, limit: Option[SQLLimit]) extends SQLSource {
  override def sql: String = s"$Unnest($identifier${asString(limit)})"
  def update(request: SQLSearchRequest): SQLUnnest =
    this.copy(identifier = identifier.update(request))
}

case class SQLTable(source: SQLSource, alias: Option[SQLAlias] = None) extends Updateable {
  override def sql: String = s"$source${asString(alias)}"
  def update(request: SQLSearchRequest): SQLTable = this.copy(source = source.update(request))
}

case class SQLFrom(tables: Seq[SQLTable]) extends Updateable {
  override def sql: String = s" $From ${tables.map(_.sql).mkString(",")}"
  lazy val aliases: Seq[String] = tables.flatMap((table: SQLTable) => table.alias).map(_.alias)
  lazy val unnests: Seq[(String, String, Option[SQLLimit])] = tables.collect {
    case SQLTable(u: SQLUnnest, a) =>
      (a.map(_.alias).getOrElse(u.identifier.columnName), u.identifier.columnName, u.limit)
  }
  def update(request: SQLSearchRequest): SQLFrom =
    this.copy(tables = tables.map(_.update(request)))
}
