package app.softnetwork.elastic.sql

case object Select extends SQLExpr("select") with SQLRegex

sealed trait Field extends Updateable with SQLFunctionChain with PainlessScript {
  def identifier: Identifier
  def fieldAlias: Option[SQLAlias]
  def isScriptField: Boolean =
    identifier.name.isEmpty || (functions.nonEmpty && !aggregation && identifier.bucket.isEmpty)
  override def sql: String = s"$identifier${asString(fieldAlias)}"
  lazy val sourceField: String =
    if (identifier.nested) {
      identifier.tableAlias
        .orElse(fieldAlias.map(_.alias))
        .map(a => s"$a.")
        .getOrElse("") + identifier.name
        .replace("(", "")
        .replace(")", "")
        .split("\\.")
        .tail
        .mkString(".")
    } else {
      identifier.name.replace("(", "").replace(")", "")
    }

  override def functions: List[SQLFunction] = identifier.functions

  def update(request: SQLSearchRequest): Field

  def painless: String = SQLFunctionUtils.buildPainless(identifier)

  lazy val scriptName: String = fieldAlias.map(_.alias).getOrElse(sourceField)
}

case class SQLField(
  identifier: SQLIdentifier,
  fieldAlias: Option[SQLAlias] = None
) extends Field {
  def update(request: SQLSearchRequest): SQLField =
    this.copy(identifier = identifier.update(request))
}

case object Except extends SQLExpr("except") with SQLRegex

case class SQLExcept(fields: Seq[Field]) extends Updateable {
  override def sql: String = s" $Except(${fields.mkString(",")})"
  def update(request: SQLSearchRequest): SQLExcept =
    this.copy(fields = fields.map(_.update(request)))
}

case class SQLSelect(
  fields: Seq[Field] = Seq(SQLField(identifier = SQLIdentifier("*"))),
  except: Option[SQLExcept] = None
) extends Updateable {
  override def sql: String =
    s"$Select ${fields.mkString(", ")}${except.getOrElse("")}"
  lazy val fieldAliases: Map[String, String] = fields.flatMap { field =>
    field.fieldAlias.map(a => field.identifier.identifierName -> a.alias)
  }.toMap
  def update(request: SQLSearchRequest): SQLSelect =
    this.copy(fields = fields.map(_.update(request)), except = except.map(_.update(request)))
}
