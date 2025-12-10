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

import app.softnetwork.elastic.sql.PainlessContextType.Processor
import app.softnetwork.elastic.sql.`type`.SQLType
import app.softnetwork.elastic.sql.query._
import app.softnetwork.elastic.sql.time.TimeUnit

package object schema {
  case class DdlColumn(
    name: String,
    dataType: SQLType,
    script: Option[PainlessScript] = None,
    multiFields: List[DdlColumn] = Nil,
    notNull: Boolean = false,
    defaultValue: Option[Value[_]] = None,
    options: Map[String, Value[_]] = Map.empty
  ) extends Token {
    def sql: String = {
      val opts = if (options.nonEmpty) {
        s" OPTIONS (${options.map { case (k, v) => s"$k = $v" }.mkString(", ")}) "
      } else {
        ""
      }
      val notNullOpt = if (notNull) " NOT NULL" else ""
      val defaultOpt = defaultValue.map(v => s" DEFAULT $v").getOrElse("")
      val fieldsOpt = if (multiFields.nonEmpty) {
        s" FIELDS (${multiFields.mkString(", ")})"
      } else {
        ""
      }
      val scriptOpt = script.map(s => s" SCRIPT AS ($s)").getOrElse("")
      s"$name $dataType$fieldsOpt$scriptOpt$notNullOpt$defaultOpt$opts"
    }

    def processorScript: Option[String] = {
      script.map { s =>
        val context = PainlessContext(Processor)
        val script = s.painless(Some(context))
        s"$context$script"
      }
    }
  }

  case class DdlPartition(column: String, granularity: TimeUnit = TimeUnit.DAYS) extends Token {
    def sql: String = s"PARTITION BY $column ($granularity)"

    val dateRounding: String = granularity.script.get

    val dateFormats: List[String] = granularity match {
      case TimeUnit.YEARS  => List("yyyy")
      case TimeUnit.MONTHS => List("yyyy-MM")
      case TimeUnit.DAYS   => List("yyyy-MM-dd")
      case TimeUnit.HOURS  => List("yyyy-MM-dd'T'HH", "yyyy-MM-dd HH")
      case TimeUnit.MINUTES =>
        List("yyyy-MM-dd'T'HH:mm", "yyyy-MM-dd HH:mm")
      case TimeUnit.SECONDS =>
        List("yyyy-MM-dd'T'HH:mm:ss", "yyyy-MM-dd HH:mm:ss")
      case _ => List.empty
    }
  }

  case class DdlColumnNotFound(column: String, table: String)
      extends Exception(s"Column $column  does not exist in table $table")

  case class DdlTable(
    name: String,
    columns: List[DdlColumn],
    primaryKey: List[String] = Nil,
    partitionBy: Option[DdlPartition] = None,
    defaultPipeline: Option[String] = None,
    finalPipeline: Option[String] = None
  ) extends Token {
    private[schema] lazy val cols: Map[String, DdlColumn] = columns.map(c => c.name -> c).toMap

    def sql: String = {
      val cols = columns.map(_.sql).mkString(", ")
      val pkStr = if (primaryKey.nonEmpty) {
        s", PRIMARY KEY (${primaryKey.mkString(", ")})"
      } else {
        ""
      }
      s"CREATE OR REPLACE TABLE $name ($cols$pkStr)${partitionBy.getOrElse("")}"
    }

    def merge(statements: Seq[AlterTableStatement]): DdlTable = {
      statements.foldLeft(this) { (table, statement) =>
        statement match {
          case AddColumn(column, ifNotExists) =>
            if (ifNotExists && table.cols.contains(column.name)) table
            else if (!table.cols.contains(column.name))
              table.copy(columns = table.columns :+ column)
            else throw DdlColumnNotFound(column.name, table.name)
          case DropColumn(columnName, ifExists) =>
            if (ifExists && !table.cols.contains(columnName)) table
            else if (table.cols.contains(columnName))
              table.copy(columns = table.columns.filterNot(_.name == columnName))
            else throw DdlColumnNotFound(columnName, table.name)
          case RenameColumn(oldName, newName) =>
            if (cols.contains(oldName))
              table.copy(
                columns = table.columns.map { col =>
                  if (col.name == oldName) col.copy(name = newName) else col
                }
              )
            else throw DdlColumnNotFound(oldName, table.name)
          case AlterColumnType(columnName, newType, ifExists) =>
            if (ifExists && !table.cols.contains(columnName)) table
            else if (table.cols.contains(columnName))
              table.copy(
                columns = table.columns.map { col =>
                  if (col.name == columnName) col.copy(dataType = newType)
                  else col
                }
              )
            else throw DdlColumnNotFound(columnName, table.name)
          case AlterColumnDefault(columnName, newDefault, ifExists) =>
            if (ifExists && !table.cols.contains(columnName)) table
            else if (table.cols.contains(columnName))
              table.copy(
                columns = table.columns.map { col =>
                  if (col.name == columnName) col.copy(defaultValue = Some(newDefault))
                  else col
                }
              )
            else throw DdlColumnNotFound(columnName, table.name)
          case DropColumnDefault(columnName, ifExists) =>
            if (ifExists && !table.cols.contains(columnName)) table
            else if (table.cols.contains(columnName))
              table.copy(
                columns = table.columns.map { col =>
                  if (col.name == columnName) col.copy(defaultValue = None)
                  else col
                }
              )
            else throw DdlColumnNotFound(columnName, table.name)
          case AlterColumnNotNull(columnName, ifExists) =>
            if (!table.cols.contains(columnName) && ifExists) table
            else if (table.cols.contains(columnName))
              table.copy(
                columns = table.columns.map { col =>
                  if (col.name == columnName) col.copy(notNull = true)
                  else col
                }
              )
            else throw DdlColumnNotFound(columnName, table.name)
          case DropColumnNotNull(columnName, ifExists) =>
            if (!table.cols.contains(columnName) && ifExists) table
            else if (table.cols.contains(columnName))
              table.copy(
                columns = table.columns.map { col =>
                  if (col.name == columnName) col.copy(notNull = false)
                  else col
                }
              )
            else throw DdlColumnNotFound(columnName, table.name)
          case AlterColumnOptions(columnName, newOptions, ifExists) =>
            if (ifExists && !table.cols.contains(columnName)) table
            else if (table.cols.contains(columnName))
              table.copy(
                columns = table.columns.map { col =>
                  if (col.name == columnName)
                    col.copy(options = col.options ++ newOptions)
                  else col
                }
              )
            else throw DdlColumnNotFound(columnName, table.name)
          case AlterColumnFields(columnName, newFields, ifExists) =>
            if (ifExists && !table.cols.contains(columnName)) table
            else if (table.cols.contains(columnName))
              table.copy(
                columns = table.columns.map { col =>
                  if (col.name == columnName)
                    col.copy(multiFields = newFields.toList)
                  else col
                }
              )
            else throw DdlColumnNotFound(columnName, table.name)
          case _ => table
        }
      }
    }

    override def validate(): Either[SQL, Unit] = {
      var errors = Seq[String]()
      // check that primary key columns exist
      primaryKey.foreach { pk =>
        if (!cols.contains(pk)) {
          errors = errors :+ s"Primary key column $pk does not exist in table $name"
        }
      }
      // check that partition column exists
      partitionBy.foreach { partition =>
        if (!cols.contains(partition.column)) {
          errors = errors :+ s"Partition column ${partition.column} does not exist in table $name"
        }
      }
      if (errors.isEmpty) Right(()) else Left(errors.mkString("\n"))
    }
  }

}
