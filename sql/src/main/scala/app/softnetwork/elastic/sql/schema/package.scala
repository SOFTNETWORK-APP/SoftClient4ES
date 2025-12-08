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
import app.softnetwork.elastic.sql.query._

package object schema {
  case class Column(
    name: String,
    dataType: SQLType,
    options: Map[String, Value[_]] = Map.empty,
    multiFields: List[Column] = Nil,
    notNull: Boolean = false,
    defaultValue: Option[Value[_]] = None
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
      s"$name $dataType$opts$notNullOpt$defaultOpt$fieldsOpt"
    }
  }

  case class Table(
    name: String,
    columns: List[Column],
    options: Map[String, Value[_]] = Map.empty
  ) extends Token {
    lazy val cols: Map[String, Column] = columns.map(c => c.name -> c).toMap

    def sql: String = {
      val cols = columns.map(_.sql).mkString(", ")
      val opts = if (options.nonEmpty) {
        s" OPTIONS (${options.map { case (k, v) => s"$k = $v" }.mkString(", ")}) "
      } else {
        ""
      }
      s"CREATE OR REPLACE TABLE $name ($cols)$opts"
    }

    def merge(statements: Seq[AlterTableStatement]): Table = {
      statements.foldLeft(this) { (table, statement) =>
        statement match {
          case AddColumn(column, ifNotExists) =>
            table.copy(columns = table.columns :+ column)
          case DropColumn(columnName, ifExists) =>
            table.copy(columns = table.columns.filterNot(_.name == columnName))
          case RenameColumn(oldName, newName) =>
            table.copy(
              columns = table.columns.map { col =>
                if (col.name == oldName) col.copy(name = newName) else col
              }
            )
          case AlterColumnType(columnName, newType, ifExists) =>
            table.copy(
              columns = table.columns.map { col =>
                if (col.name == columnName) col.copy(dataType = newType)
                else col
              }
            )
          case AlterColumnDefault(columnName, newDefault, ifExists) =>
            table.copy(
              columns = table.columns.map { col =>
                if (col.name == columnName) col.copy(defaultValue = Some(newDefault))
                else col
              }
            )
          case DropColumnDefault(columnName, ifExists) =>
            table.copy(
              columns = table.columns.map { col =>
                if (col.name == columnName) col.copy(defaultValue = None)
                else col
              }
            )
          case AlterColumnNotNull(columnName, ifExists) =>
            table.copy(
              columns = table.columns.map { col =>
                if (col.name == columnName) col.copy(notNull = true)
                else col
              }
            )
          case DropColumnNotNull(columnName, ifExists) =>
            table.copy(
              columns = table.columns.map { col =>
                if (col.name == columnName) col.copy(notNull = false)
                else col
              }
            )
          case AlterColumnOptions(columnName, newOptions, ifExists) =>
            table.copy(
              columns = table.columns.map { col =>
                if (col.name == columnName)
                  col.copy(options = col.options ++ newOptions)
                else col
              }
            )
          case _ => table
        }
      }
    }
  }

}
