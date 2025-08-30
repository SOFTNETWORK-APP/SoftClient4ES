package app.softnetwork.elastic.sql

case object From extends SQLExpr("from") with SQLRegex

sealed trait SQLSource extends Updateable {
  def name: String
  def update(request: SQLSearchRequest): SQLSource
}

case class SQLIdentifier(
  name: String,
  tableAlias: Option[String] = None,
  distinct: Boolean = false,
  nested: Boolean = false,
  limit: Option[SQLLimit] = None,
  function: Option[SQLFunction] = None,
  fieldAlias: Option[String] = None
) extends SQLExpr({
      var parts: Seq[String] = name.split("\\.").toSeq
      tableAlias match {
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

  lazy val aggregationName: Option[String] =
    if (aggregation) fieldAlias.orElse(Option(name)) else None

  lazy val identifierName: String =
    (function match {
      case Some(f) => s"${f.sql}($name)"
      case _       => name
    }).toLowerCase

  lazy val nestedType: Option[String] = if (nested) Some(name.split('.').head) else None

  lazy val innerHitsName: Option[String] = if (nested) tableAlias else None

  def update(request: SQLSearchRequest): SQLIdentifier = {
    val parts: Seq[String] = name.split("\\.").toSeq
    if (request.tableAliases.values.toSeq.contains(parts.head)) {
      request.unnests.find(_._1 == parts.head) match {
        case Some(tuple) =>
          this.copy(
            tableAlias = Some(parts.head),
            name = s"${tuple._2}.${parts.tail.mkString(".")}",
            nested = true,
            limit = tuple._3
          )
        case _ =>
          this.copy(
            tableAlias = Some(parts.head),
            name = parts.tail.mkString(".")
          )
      }
    } else if (request.fieldAliases.contains(identifierName)) {
      this.copy(fieldAlias = Some(request.fieldAliases(identifierName)))
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
}
