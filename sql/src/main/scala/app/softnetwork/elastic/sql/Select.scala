package app.softnetwork.elastic.sql

import app.softnetwork.elastic.sql.function.{Function, FunctionChain}

case object Select extends Expr("select") with TokenRegex

case class Field(
  identifier: GenericIdentifier,
  fieldAlias: Option[Alias] = None
) extends Updateable
    with FunctionChain
    with PainlessScript {
  def isScriptField: Boolean = functions.nonEmpty && !aggregation && identifier.bucket.isEmpty
  override def sql: String = s"$identifier${asString(fieldAlias)}"
  lazy val sourceField: String = {
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
    } else if (identifier.name.nonEmpty) {
      identifier.name
        .replace("(", "")
        .replace(")", "")
    } else {
      AliasUtils.normalize(identifier.identifierName)
    }
  }

  override def functions: List[Function] = identifier.functions

  def update(request: SQLSearchRequest): Field =
    this.copy(identifier = identifier.update(request))

  def painless: String = identifier.painless

  lazy val scriptName: String = fieldAlias.map(_.alias).getOrElse(sourceField)

  override def validate(): Either[String, Unit] = identifier.validate()
}

case object Except extends Expr("except") with TokenRegex

case class Except(fields: Seq[Field]) extends Updateable {
  override def sql: String = s" $Except(${fields.mkString(",")})"
  def update(request: SQLSearchRequest): Except =
    this.copy(fields = fields.map(_.update(request)))
}

case class Select(
  fields: Seq[Field] = Seq(Field(identifier = GenericIdentifier("*"))),
  except: Option[Except] = None
) extends Updateable {
  override def sql: String =
    s"$Select ${fields.mkString(", ")}${except.getOrElse("")}"
  lazy val fieldAliases: Map[String, String] = fields.flatMap { field =>
    field.fieldAlias.map(a => field.identifier.identifierName -> a.alias)
  }.toMap
  def update(request: SQLSearchRequest): Select =
    this.copy(fields = fields.map(_.update(request)), except = except.map(_.update(request)))

  override def validate(): Either[String, Unit] =
    if (fields.isEmpty) {
      Left("At least one field is required in SELECT clause")
    } else {
      fields.map(_.validate()).find(_.isLeft).getOrElse(Right(()))
    }
}
