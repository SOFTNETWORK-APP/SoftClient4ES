package app.softnetwork.elastic.sql.query

import app.softnetwork.elastic.sql.function.aggregate.TopHitsAggregation
import app.softnetwork.elastic.sql.function.{Function, FunctionChain}
import app.softnetwork.elastic.sql.{
  asString,
  Alias,
  AliasUtils,
  DateMathScript,
  Expr,
  Identifier,
  PainlessContext,
  PainlessScript,
  TokenRegex,
  Updateable
}

case object Select extends Expr("SELECT") with TokenRegex

case class Field(
  identifier: Identifier,
  fieldAlias: Option[Alias] = None
) extends Updateable
    with FunctionChain
    with PainlessScript
    with DateMathScript {
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

  lazy val topHits: Option[TopHitsAggregation] =
    functions.collectFirst { case th: TopHitsAggregation => th }

  def update(request: SQLSearchRequest): Field = {
    val updated =
      topHits match {
        case Some(th) =>
          val topHitsAggregation = th.update(request)
          identifier.functions match {
            case _ :: tail => identifier.withFunctions(functions = topHitsAggregation +: tail)
            case _         => identifier.withFunctions(functions = List(topHitsAggregation))
          }
        case None => identifier
      }
    this.copy(identifier = updated.update(request))
  }

  def painless(context: Option[PainlessContext]): String = identifier.painless(context)

  def script: Option[String] = identifier.script

  lazy val scriptName: String = fieldAlias.map(_.alias).getOrElse(sourceField)

  override def validate(): Either[String, Unit] = identifier.validate()

  lazy val nested: Boolean = identifier.nested

  lazy val path: String = identifier.path
}

case object Except extends Expr("except") with TokenRegex

case class Except(fields: Seq[Field]) extends Updateable {
  override def sql: String = s" $Except(${fields.mkString(",")})"
  def update(request: SQLSearchRequest): Except =
    this.copy(fields = fields.map(_.update(request)))
}

case class Select(
  fields: Seq[Field] = Seq(Field(identifier = Identifier("*"))),
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
      fields.map(_.validate()).filter(_.isLeft) match {
        case Nil    => Right(())
        case errors => Left(errors.map { case Left(err) => err }.mkString("\n"))
      }
    }
}
