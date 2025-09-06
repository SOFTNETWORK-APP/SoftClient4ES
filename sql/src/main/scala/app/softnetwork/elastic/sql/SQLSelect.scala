package app.softnetwork.elastic.sql

case object Select extends SQLExpr("select") with SQLRegex

sealed trait Field extends Updateable with SQLFunctionChain {
  def identifier: SQLIdentifier
  def fieldAlias: Option[SQLAlias]
  def isScriptField: Boolean = false
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
}

case class SQLField(
  identifier: SQLIdentifier,
  fieldAlias: Option[SQLAlias] = None
) extends Field {
  def update(request: SQLSearchRequest): SQLField =
    this.copy(identifier = identifier.update(request))
}

sealed trait ScriptField extends Field with PainlessScript {
  override def isScriptField: Boolean = true

  def update(request: SQLSearchRequest): ScriptField

  lazy val name: String = fieldAlias.map(_.alias).getOrElse(sourceField)
}

case class SQLFunctionField(
  override val functions: List[SQLFunction],
  fieldAlias: Option[SQLAlias] = None
) extends ScriptField
    with SQLFunctionChain {

  override def update(request: SQLSearchRequest): SQLFunctionField =
    this // TODO update SQLAlias if needed

  override def identifier: SQLIdentifier = SQLIdentifier("", functions = functions)

  override def painless: String = SQLFunctionUtils.buildPainless(functions)

  override lazy val sourceField: String = toSQL("").replace("(", "").replace(")", "")
}

case class SQLDateTimeField(
  identifier: SQLIdentifier,
  operator: Option[ArithmeticOperator] = None,
  interval: Option[TimeInterval],
  fieldAlias: Option[SQLAlias] = None
) extends ScriptField {
  override def sql: String =
    s"$identifier${asString(operator)}${asString(interval)}${asString(fieldAlias)}"
  def update(request: SQLSearchRequest): SQLDateTimeField =
    this.copy(identifier = identifier.update(request))
  override def painless: String = {
    val base = identifier.functions.headOption match { // FIXME
      case f @ Some(CurrentDate | CurrentTime | CurrentTimestamp | Now) =>
        f.asInstanceOf[PainlessScript].painless
      case _ => s"doc['$sourceField'].value"
    }
    (operator, interval) match {
      case (Some(Minus), Some(i)) => s"$base.minus(${i.painless})"
      case (Some(Plus), Some(i))  => s"$base.plus(${i.painless})"
      case _                      => base
    }
  }
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
