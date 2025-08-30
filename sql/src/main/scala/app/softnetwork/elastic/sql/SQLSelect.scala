package app.softnetwork.elastic.sql

case object Select extends SQLExpr("select") with SQLRegex

case class SQLField(
  identifier: SQLIdentifier,
  fieldAlias: Option[SQLAlias] = None
) extends Updateable
    with SQLTokenWithFunction {
  override def sql: String = s"$identifier${asString(fieldAlias)}"
  def update(request: SQLSearchRequest): SQLField =
    this.copy(identifier = identifier.update(request))
  lazy val sourceField: String =
    if (identifier.nested) {
      identifier.tableAlias
        .orElse(fieldAlias.map(_.alias))
        .map(a => s"$a.")
        .getOrElse("") + identifier.name.split("\\.").tail.mkString(".")
    } else {
      identifier.name
    }

  override def function: Option[SQLFunction] = identifier.function
}

case object Except extends SQLExpr("except") with SQLRegex

case class SQLExcept(fields: Seq[SQLField]) extends Updateable {
  override def sql: String = s" $Except(${fields.mkString(",")})"
  def update(request: SQLSearchRequest): SQLExcept =
    this.copy(fields = fields.map(_.update(request)))
}

case class SQLSelect(
  fields: Seq[SQLField] = Seq(SQLField(identifier = SQLIdentifier("*"))),
  except: Option[SQLExcept] = None
) extends Updateable {
  override def sql: String =
    s"$Select ${fields.mkString(",")}${except.getOrElse("")}"
  lazy val fieldAliases: Map[String, String] = fields.flatMap { field =>
    field.fieldAlias.map(a => field.identifier.identifierName -> a.alias)
  }.toMap
  def update(request: SQLSearchRequest): SQLSelect =
    this.copy(fields = fields.map(_.update(request)), except = except.map(_.update(request)))
}
