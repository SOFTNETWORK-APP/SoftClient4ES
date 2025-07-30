package app.softnetwork.elastic.sql

case object Select extends SQLExpr("select") with SQLRegex

case class SQLField(
  identifier: SQLIdentifier,
  alias: Option[SQLAlias] = None
) extends Updateable {
  override def sql: String = s"$identifier${asString(alias)}"
  def update(request: SQLSearchRequest): SQLField =
    this.copy(identifier = identifier.update(request))
  lazy val sourceField: String =
    if (identifier.nested) {
      identifier.alias
        .map(a => s"$a.")
        .getOrElse("") + identifier.columnName.split("\\.").tail.mkString(".")
    } else {
      identifier.columnName
    }
}

case object Except extends SQLExpr("except") with SQLRegex

case class SQLExcept(fields: Seq[SQLField]) extends Updateable {
  override def sql: String = s" $Except(${fields.mkString(",")})"
  def update(request: SQLSearchRequest): SQLExcept =
    this.copy(fields = fields.map(_.update(request)))
}

case object Filter extends SQLExpr("filter") with SQLRegex

case class SQLFilter(criteria: Option[SQLCriteria]) extends Updateable {
  override def sql: String = criteria match {
    case Some(c) => s" $Filter($c)"
    case _       => ""
  }
  def update(request: SQLSearchRequest): SQLFilter =
    this.copy(criteria = criteria.map(_.update(request)))
}

class SQLAggregate(
  val function: AggregateFunction,
  override val identifier: SQLIdentifier,
  override val alias: Option[SQLAlias] = None,
  val filter: Option[SQLFilter] = None
) extends SQLField(identifier, alias) {
  override def sql: String = s"$function($identifier)${asString(alias)}"
  override def update(request: SQLSearchRequest): SQLAggregate =
    new SQLAggregate(function, identifier.update(request), alias, filter.map(_.update(request)))
}

case class SQLSelect(
  fields: Seq[SQLField] = Seq(SQLField(identifier = SQLIdentifier("*"))),
  except: Option[SQLExcept] = None
) extends Updateable {
  override def sql: String =
    s"$Select ${fields.mkString(",")}${except.getOrElse("")}"
  def update(request: SQLSearchRequest): SQLSelect =
    this.copy(fields = fields.map(_.update(request)), except = except.map(_.update(request)))
}
