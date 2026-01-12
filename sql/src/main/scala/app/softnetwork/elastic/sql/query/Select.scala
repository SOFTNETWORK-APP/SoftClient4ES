/*
 * Copyright 2025 SOFTNETWORK
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package app.softnetwork.elastic.sql.query

import app.softnetwork.elastic.sql.function.aggregate.{
  AggregateFunction,
  BucketScriptAggregation,
  COUNT,
  WindowFunction
}
import app.softnetwork.elastic.sql.function.{Function, FunctionChain, FunctionUtils}
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
  def tableAlias: Option[String] = identifier.tableAlias
  def table: Option[String] = identifier.table
  def isScriptField: Boolean =
    functions.nonEmpty && !hasAggregation && identifier.bucket.isEmpty
  override def sql: String = s"$identifier${asString(fieldAlias)}"
  lazy val sourceField: String = {
    if (identifier.nested) {
      tableAlias
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

  def update(request: SingleSearch): Field = {
    identifier.windows match {
      case Some(th) =>
        val windowFunction = th.update(request)
        val identifier = windowFunction.identifier
        identifier.functions match {
          case _ :: tail =>
            this.copy(identifier = identifier.withFunctions(functions = windowFunction +: tail))
          case _ =>
            this.copy(identifier = identifier.withFunctions(functions = List(windowFunction)))
        }
      case None => this.copy(identifier = identifier.update(request))
    }
  }

  def painless(context: Option[PainlessContext]): String = identifier.painless(context)

  def script: Option[String] = identifier.script

  lazy val scriptName: String = fieldAlias.map(_.alias).getOrElse(sourceField)

  override def validate(): Either[String, Unit] = identifier.validate()

  def nestedElement: Option[NestedElement] =
    identifier.nestedElement
      .orElse(
        identifier.functionNestedElement
      )
      .orElse(this.functionNestedElement)

  lazy val nested: Boolean = nestedElement.isDefined

  lazy val path: String = identifier.path

  def isBucketScript: Boolean = !isAggregation && hasAggregation
}

case object Except extends Expr("except") with TokenRegex

case class Except(fields: Seq[Field]) extends Updateable {
  override def sql: String = s" $Except(${fields.mkString(",")})"
  def update(request: SingleSearch): Except =
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
  def update(request: SingleSearch): Select =
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

case class SQLAggregation(
  aggName: String,
  field: String,
  sourceField: String,
  distinct: Boolean = false,
  aggType: AggregateFunction,
  direction: Option[SortOrder] = None,
  nestedElement: Option[NestedElement] = None,
  bucketPath: String = ""
) {
  val nested: Boolean = nestedElement.nonEmpty
  val multivalued: Boolean = aggType.multivalued
  val isGlobalMetric: Boolean =
    distinct && (aggType match {
      case COUNT => true
      case _     => false
    })
  val bucketRoot: String =
    bucketPath.split(">").toSeq match {
      case Nil => ""
      case seq => seq.head
    }
}

object SQLAggregation {
  def fromField(field: Field, request: SingleSearch): Option[SQLAggregation] = {
    import field._

    val aggType = aggregateFunction match {
      case Some(agg) => agg
      case None if field.isBucketScript =>
        BucketScriptAggregation(identifier).update(request)
      case _ => return None
    }

    val sourceField = identifier.path

    val direction = request.sorts.get(identifier.identifierName)

    val _field = fieldAlias match {
      case Some(alias) => alias.alias
      case _           => sourceField
    }

    val distinct = identifier.distinct

    val aggName = {
      if (fieldAlias.isDefined)
        _field
      else if (distinct)
        s"${aggType}_distinct_${sourceField.replace(".", "_")}"
      else {
        aggType match {
          case th: WindowFunction =>
            s"${th.window.sql.toLowerCase}_${sourceField.replace(".", "_")}"
          case _ =>
            s"${aggType}_${sourceField.replace(".", "_")}"

        }
      }
    }

    var aggPath = Seq[String]()

    val (aggFuncs, _) = FunctionUtils.aggregateAndTransformFunctions(identifier)

    if (!isBucketScript)
      require(aggFuncs.size == 1, s"Multiple aggregate functions not supported: $aggFuncs")

    val nestedElement = identifier.nestedElement

    val nestedElements: Seq[NestedElement] =
      nestedElement.map(n => NestedElements.buildNestedTrees(Seq(n))).getOrElse(Nil)

    nestedElements match {
      case Nil =>
        aggPath ++= Seq(aggName)
      case nestedElements =>
        def buildNested(n: NestedElement): Unit = {
          aggPath ++= Seq(n.innerHitsName)
          val children = n.children
          if (children.nonEmpty) {
            children.map(buildNested)
          }
        }
        val root = nestedElements.head
        buildNested(root)
        request.having match {
          case Some(_) => aggPath ++= Seq("filtered_agg")
          case _       =>
        }
        aggPath ++= Seq(aggName)
    }

    val bucketPath =
      aggType.bucketPath match {
        case paths if paths.isEmpty => identifier.bucketPath
        case other                  => other
      }

    Some(
      SQLAggregation(
        aggPath.mkString("."),
        _field,
        sourceField,
        distinct = distinct,
        aggType = aggType,
        direction = direction,
        nestedElement = identifier.nestedElement,
        bucketPath = bucketPath
      )
    )
  }
}
