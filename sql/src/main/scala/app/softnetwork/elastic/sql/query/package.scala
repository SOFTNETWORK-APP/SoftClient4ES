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

package app.softnetwork.elastic.sql

import app.softnetwork.elastic.sql.`type`.SQLType
import app.softnetwork.elastic.sql.schema.Column
import app.softnetwork.elastic.sql.function.aggregate.WindowFunction

package object query {
  sealed trait Statement extends Token

  trait DqlStatement extends Statement

  /** Select Statement wrapper
    * @param query
    *   - the SQL query
    * @param score
    *   - optional minimum score for the elasticsearch query
    */
  case class SelectStatement(query: SQL, score: Option[Double] = None) extends DqlStatement {
    import app.softnetwork.elastic.sql.SQLImplicits._

    lazy val statement: Option[DqlStatement] = {
      queryToStatement(query) match {
        case Some(s: DqlStatement) => Some(s)
        case _                     => None
      }
    }

    override def sql: SQL =
      statement match {
        case Some(value) => value.sql
        case None        => query
      }

    def minScore(score: Double): SelectStatement = this.copy(score = Some(score))
  }

  case class SingleSearch(
    select: Select = Select(),
    from: From,
    where: Option[Where],
    groupBy: Option[GroupBy] = None,
    having: Option[Having] = None,
    orderBy: Option[OrderBy] = None,
    limit: Option[Limit] = None,
    score: Option[Double] = None
  ) extends DqlStatement {
    override def sql: String =
      s"$select$from${asString(where)}${asString(groupBy)}${asString(having)}${asString(orderBy)}${asString(limit)}"

    lazy val fieldAliases: Map[String, String] = select.fieldAliases
    lazy val tableAliases: Map[String, String] = from.tableAliases
    lazy val unnestAliases: Map[String, (String, Option[Limit])] = from.unnestAliases
    lazy val bucketNames: Map[String, Bucket] = buckets.flatMap { b =>
      val name = b.identifier.identifierName
      "\\d+".r.findFirstIn(name) match {
        case Some(n) if name.trim.split(" ").length == 1 =>
          val identifier = select.fields(n.toInt - 1).identifier
          val updated = b.copy(identifier = select.fields(n.toInt - 1).identifier)
          Map(
            n                         -> updated, // also map numeric bucket to field name
            identifier.identifierName -> updated
          )
        case _ => Map(name -> b)
      }
    }.toMap

    var unnests: scala.collection.mutable.Map[String, Unnest] = {
      val map = from.unnests.map(u => u.alias.map(_.alias).getOrElse(u.name) -> u).toMap
      scala.collection.mutable.Map(map.toSeq: _*)
    }

    lazy val nestedFields: Map[String, Seq[Field]] =
      select.fields
        .filterNot(_.isAggregation)
        .filter(_.nested)
        .groupBy(_.identifier.innerHitsName.getOrElse(""))
    lazy val nested: Seq[NestedElement] =
      from.unnests.map(toNestedElement).groupBy(_.path).map(_._2.head).toList
    private[this] lazy val nestedFieldsWithoutCriteria: Map[String, Seq[Field]] = {
      val innerHitsWithCriteria = (where.map(_.nestedElements).getOrElse(Seq.empty) ++
        having.map(_.nestedElements).getOrElse(Seq.empty) ++
        groupBy.map(_.nestedElements).getOrElse(Seq.empty))
        .groupBy(_.path)
        .map(_._2.head)
        .toList
        .map(_.innerHitsName)
      val ret = nestedFields.filterNot { case (innerHitsName, _) =>
        innerHitsWithCriteria.contains(innerHitsName)
      }
      ret
    }
    // nested fields that are not part of where, having or group by clauses
    lazy val nestedElementsWithoutCriteria: Seq[NestedElement] =
      nested.filter(n => nestedFieldsWithoutCriteria.keys.toSeq.contains(n.innerHitsName))

    def toNestedElement(u: Unnest): NestedElement = {
      val updated = unnests.getOrElse(u.alias.map(_.alias).getOrElse(u.name), u)
      val parent = updated.parent.map(toNestedElement)
      NestedElement(
        path = updated.path,
        innerHitsName = updated.innerHitsName,
        size = limit.map(_.limit),
        children = Nil,
        sources = nestedFields
          .get(updated.innerHitsName)
          .map(_.map(_.identifier.name.split('.').tail.mkString(".")))
          .getOrElse(Nil),
        parent = parent
      )
    }

    lazy val sorts: Map[String, SortOrder] =
      orderBy.map { _.sorts.map(s => s.name -> s.direction) }.getOrElse(Map.empty).toMap

    def update(): SingleSearch = {
      (for {
        from <- Option(this.copy(from = from.update(this)))
        select <- Option(
          from.copy(
            select = select.update(from),
            groupBy = groupBy.map(_.update(from)),
            having = having.map(_.update(from))
          )
        )
        where   <- Option(select.copy(where = where.map(_.update(select))))
        updated <- Option(where.copy(orderBy = orderBy.map(_.update(where))))
      } yield updated).getOrElse(
        throw new IllegalStateException("Failed to update SQLSearchRequest")
      )
    }

    lazy val scriptFields: Seq[Field] = {
      if (aggregates.nonEmpty)
        Seq.empty
      else
        select.fields.filter(_.isScriptField)
    }

    lazy val fields: Seq[String] = {
      if (groupBy.isEmpty && !windowFunctions.exists(_.isWindowing))
        select.fields
          .filterNot(_.isScriptField)
          .filterNot(_.nested)
          .filterNot(_.isAggregation)
          .map(_.sourceField)
          .filterNot(f => excludes.contains(f))
          .distinct
      else
        Seq.empty
    }

    lazy val windowFields: Seq[Field] = select.fields.filter(_.identifier.hasWindow)

    lazy val windowFunctions: Seq[WindowFunction] = windowFields.flatMap(_.identifier.windows)

    lazy val aggregates: Seq[Field] =
      select.fields
        .filter(f => f.isAggregation || f.isBucketScript)
        .filterNot(_.identifier.hasWindow) ++ windowFields

    lazy val sqlAggregations: Map[String, SQLAggregation] =
      aggregates.flatMap(f => SQLAggregation.fromField(f, this)).map(a => a.aggName -> a).toMap

    lazy val excludes: Seq[String] = select.except.map(_.fields.map(_.sourceField)).getOrElse(Nil)

    lazy val sources: Seq[String] = from.tables.map(_.name)

    lazy val bucketTree: BucketTree = BucketTree.fromBuckets(
      Seq(groupBy.map(_.buckets).getOrElse(Seq.empty)) ++ windowFunctions.map(
        _.buckets
      )
    )

    lazy val buckets: Seq[Bucket] = bucketTree.allBuckets.flatten

    override def validate(): Either[String, Unit] = {
      for {
        _ <- from.validate()
        _ <- select.validate()
        _ <- where.map(_.validate()).getOrElse(Right(()))
        _ <- groupBy.map(_.validate()).getOrElse(Right(()))
        _ <- having.map(_.validate()).getOrElse(Right(()))
        _ <- orderBy.map(_.validate()).getOrElse(Right(()))
        _ <- limit.map(_.validate()).getOrElse(Right(()))
        /*_ <- {
          // validate that having clauses are only applied when group by is present
          if (having.isDefined && groupBy.isEmpty) {
            Left("HAVING clauses can only be applied when GROUP BY is present")
          } else {
            Right(())
          }
        }*/
        _ <- {
          // validate that non-aggregated fields are not present when group by is present
          if (groupBy.isDefined) {
            val nonAggregatedFields =
              select.fields.filterNot(f => f.hasAggregation)
            val invalidFields = nonAggregatedFields.filterNot(f =>
              buckets.exists(b =>
                b.name == f.fieldAlias.map(_.alias).getOrElse(f.sourceField.replace(".", "_"))
              )
            )
            if (invalidFields.nonEmpty) {
              Left(
                s"Non-aggregated fields ${invalidFields.map(_.sql).mkString(", ")} cannot be selected when GROUP BY is present"
              )
            } else {
              Right(())
            }
          } else {
            Right(())
          }
        }
      } yield ()
    }

  }

  case class MultiSearch(requests: Seq[SingleSearch]) extends DqlStatement {
    override def sql: String = s"${requests.map(_.sql).mkString(" UNION ALL ")}"

    def update(): MultiSearch = this.copy(requests = requests.map(_.update()))

    override def validate(): Either[String, Unit] = {
      requests.map(_.validate()).filter(_.isLeft) match {
        case Nil    => Right(()) // TODO validate that all requests have the same fields
        case errors => Left(errors.map { case Left(err) => err }.mkString("\n"))
      }
    }

    lazy val sqlAggregations: Map[String, SQLAggregation] =
      requests.flatMap(_.sqlAggregations).distinct.toMap

    lazy val fieldAliases: Map[String, String] =
      requests.flatMap(_.fieldAliases).distinct.toMap
  }

  sealed trait DmlStatement extends Statement

  case class Insert(
    table: String,
    cols: Seq[String],
    values: Either[DqlStatement, Seq[Value[_]]]
  ) extends DmlStatement {
    override def sql: String = {
      values match {
        case Left(query) if cols.isEmpty =>
          s"INSERT INTO $table ${query.sql}"
        case Left(query) =>
          s"INSERT INTO $table (${cols.mkString(",")}) ${query.sql}"
        case Right(vs) =>
          val valuesSql = vs
            .map {
              case v if v.isInstanceOf[StringValue] => s"'${v.value}'"
              case v                                => s"${v.value}"
            }
            .mkString(", ")
          s"INSERT INTO $table ${cols.mkString(",")} VALUES ($valuesSql)"
      }
    }

    override def validate(): Either[String, Unit] = {
      values match {
        case Right(vs) if cols.size != vs.size =>
          Left(s"Number of columns (${cols.size}) does not match number of values (${vs.size})")
        case _ =>
          Right(())
      }
    }
  }

  case class Update(table: String, values: Map[String, Value[_]], where: Option[Where])
      extends DmlStatement {
    override def sql: String = s"UPDATE $table SET ${values
      .map { case (k, v) => s"$k = ${v.value}" }
      .mkString(", ")}${where.map(w => s" ${w.sql}").getOrElse("")}"
  }

  case class Delete(table: Table, where: Option[Where]) extends DmlStatement {
    override def sql: String =
      s"DELETE FROM ${table.name}${where.map(w => s" ${w.sql}").getOrElse("")}"
  }

  sealed trait DdlStatement extends Statement

  case class CreateTable(
    table: String,
    ddl: Either[DqlStatement, List[Column]],
    ifNotExists: Boolean = false,
    orReplace: Boolean = false
  ) extends DdlStatement {

    override def sql: String = {
      val ineClause = if (ifNotExists) " IF NOT EXISTS" else ""
      val replaceClause = if (orReplace) " OR REPLACE" else ""
      ddl match {
        case Left(select) =>
          s"CREATE$replaceClause TABLE$ineClause $table AS ${select.sql}"
        case Right(columns) =>
          val colsSql = columns.map(_.sql).mkString(", ")
          s"CREATE$replaceClause TABLE$ineClause $table ($colsSql)"
      }
    }

    lazy val columns: Seq[Column] = {
      ddl match {
        case Left(select) =>
          select match {
            case s: SingleSearch =>
              s.select.fields.map(f => Column(f.identifier.aliasOrName, f.out))
            case m: MultiSearch =>
              m.requests.headOption
                .map { req =>
                  req.select.fields.map(f => Column(f.identifier.aliasOrName, f.out))
                }
                .getOrElse(Nil)
            case _ => Nil
          }
        case Right(cols) => cols
      }
    }
  }

  case class AlterTable(table: String, ifExists: Boolean, statements: List[AlterTableStatement])
      extends DdlStatement {
    override def sql: String = {
      val ifExistsClause = if (ifExists) " IF EXISTS " else ""
      val parenthesesNeeded = statements.size > 1
      val statementsSql = if (parenthesesNeeded) {
        statements.map(_.sql).mkString("(\n\t", ",\n\t", "\n)")
      } else {
        statements.map(_.sql).mkString("")
      }
      s"ALTER TABLE $table$ifExistsClause $statementsSql"
    }
  }

  sealed trait AlterTableStatement extends Token
  case class AddColumn(column: Column, ifNotExists: Boolean = false) extends AlterTableStatement {
    override def sql: String = {
      val ifNotExistsClause = if (ifNotExists) " IF NOT EXISTS" else ""
      s"ADD COLUMN$ifNotExistsClause ${column.sql}"
    }
  }
  case class DropColumn(columnName: String, ifExists: Boolean = false) extends AlterTableStatement {
    override def sql: String = {
      val ifExistsClause = if (ifExists) " IF EXISTS" else ""
      s"DROP COLUMN$ifExistsClause $columnName"
    }
  }
  case class RenameColumn(oldName: String, newName: String) extends AlterTableStatement {
    override def sql: String = s"RENAME COLUMN $oldName TO $newName"
  }
  case class AlterColumnOptions(
    columnName: String,
    options: Map[String, Value[_]],
    ifExists: Boolean = false
  ) extends AlterTableStatement {
    override def sql: String = {
      val ifExistsClause = if (ifExists) " IF EXISTS" else ""
      s"ALTER COLUMN$ifExistsClause $columnName SET OPTIONS (${options
        .map { case (k, v) => s"$k = $v" }
        .mkString(", ")})"
    }
  }
  case class AlterColumnType(columnName: String, newType: SQLType, ifExists: Boolean = false)
      extends AlterTableStatement {
    override def sql: String = {
      val ifExistsClause = if (ifExists) " IF EXISTS" else ""
      s"ALTER COLUMN$ifExistsClause $columnName SET TYPE $newType"
    }
  }
  case class AlterColumnDefault(
    columnName: String,
    defaultValue: Value[_],
    ifExists: Boolean = false
  ) extends AlterTableStatement {
    override def sql: String = {
      val ifExistsClause = if (ifExists) " IF EXISTS" else ""
      s"ALTER COLUMN$ifExistsClause $columnName SET DEFAULT $defaultValue"
    }
  }
  case class DropColumnDefault(columnName: String, ifExists: Boolean = false)
      extends AlterTableStatement {
    override def sql: String = {
      val ifExistsClause = if (ifExists) " IF EXISTS" else ""
      s"ALTER COLUMN$ifExistsClause $columnName DROP DEFAULT"
    }
  }
  case class AlterColumnNotNull(columnName: String, ifExists: Boolean = false)
      extends AlterTableStatement {
    override def sql: String = {
      val ifExistsClause = if (ifExists) " IF EXISTS" else ""
      s"ALTER COLUMN$ifExistsClause $columnName SET NOT NULL"
    }
  }
  case class DropColumnNotNull(columnName: String, ifExists: Boolean = false)
      extends AlterTableStatement {
    override def sql: String = {
      val ifExistsClause = if (ifExists) " IF EXISTS" else ""
      s"ALTER COLUMN$ifExistsClause $columnName DROP NOT NULL"
    }
  }
  case class AlterColumnFields(
    columnName: String,
    fields: Seq[Column],
    ifExists: Boolean = false
  ) extends AlterTableStatement {
    override def sql: String = {
      val ifExistsClause = if (ifExists) " IF EXISTS" else ""
      val fieldsSql = fields.map(_.sql).mkString("(\n\t\t", ",\n\t\t", "\n\t)")
      s"ALTER COLUMN$ifExistsClause $columnName SET FIELDS $fieldsSql"
    }
  }

  case class DropTable(table: String, ifExists: Boolean = false, cascade: Boolean = false)
      extends DdlStatement {
    override def sql: String = {
      val ifExistsClause = if (ifExists) "IF EXISTS " else ""
      val cascadeClause = if (cascade) " CASCADE" else ""
      s"DROP TABLE $ifExistsClause$table$cascadeClause"
    }
  }

  case class TruncateTable(table: String) extends DdlStatement {
    override def sql: String = s"TRUNCATE TABLE $table"
  }
}
