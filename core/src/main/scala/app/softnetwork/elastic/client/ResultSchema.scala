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

package app.softnetwork.elastic.client

import app.softnetwork.elastic.sql.`type`.{SQLType, SQLTypes}
import app.softnetwork.elastic.sql.query.{Field, FromlessSelect, MultiSearch, SingleSearch}
import app.softnetwork.elastic.sql.query.MultiSearch.{SetOperationColumn, SetOperationType}
import app.softnetwork.elastic.sql.schema.Column

/** ONE column of a statement's result, declared BEFORE any row is read.
  *
  * @param name
  *   the name the column takes in every row (`Field.outputName`; a struct's field name; empty for a
  *   list's element)
  * @param sqlType
  *   its type at this depth: `KEYWORD`, `TEXT` and `CHAR` are `VARCHAR`, a `DATETIME` a
  *   `TIMESTAMP`; a date field whose format core cannot read for sure is `VARCHAR` (its text as
  *   stored); `ANY` where it is not known, `NUMERIC` / `TEMPORAL` for a number / a temporal of
  *   unknown width
  * @param nullable
  *   always `true` in this release
  * @param fields
  *   a `STRUCT`'s or a `GEO_POINT`'s fields, by name; empty for any other type and for a `STRUCT`
  *   whose fields are not known
  * @param element
  *   an `ARRAY`'s element; `None` for any other type
  */
final case class ResultColumn(
  name: String,
  sqlType: SQLType,
  nullable: Boolean = true,
  fields: Seq[ResultColumn] = Nil,
  element: Option[ResultColumn] = None
)

/** The columns of a statement's result, in the order a consumer presents them (the SELECT order;
  * `SELECT *` = the mapping's columns in their table order). Rows keep their documents' key order:
  * a consumer reads a value BY NAME.
  */
final case class ResultSchema(columns: Seq[ResultColumn]) {

  /** The column names, in schema order. */
  def names: Seq[String] = columns.map(_.name)
}

/** THE builder of a statement's [[ResultSchema]] (SCHEMA-2, D1): from the type each projection item
  * has as a set-operation position (`MultiSearch.fieldType` / `columnType`, #292) and the route of
  * each date field ([[DateRoute.of]]: a date read as stored is `VARCHAR`). Built once per
  * statement.
  */
private[client] object ResultSchemas {

  /** `None` where no schema is known: no mapping attached to the statement (a client that is not an
    * `IndicesApi`, an es8/es9 alias, a mapping that could not be read).
    */
  def of(single: SingleSearch, dates: SetOperationValues.DateReading): Option[ResultSchema] =
    of(single, dates, MultiSearch.fieldType _, MultiSearch.columnType _)

  /** The same, over the types its caller computed once per statement for its own use (`typeOf`
    * = `MultiSearch.fieldType`, `columnTypeOf` = `MultiSearch.columnType`): [[HitReading.of]].
    */
  private[client] def of(
    single: SingleSearch,
    dates: SetOperationValues.DateReading,
    typeOf: Field => SetOperationType,
    columnTypeOf: Column => SetOperationType
  ): Option[ResultSchema] =
    single.schema.map { table =>
      ResultSchema(single.select.fieldsWithComputedAliases.flatMap { field =>
        if (HitReading.isStar(field))
          table.columns.map(c => columnOf(c.name, columnTypeOf(c), dates))
        else Seq(columnOf(field.outputName, typeOf(field), dates))
      })
    }

  /** A set operation's schema (D12): its columns typed ONCE ([[SetOperationValues.routedColumns]],
    * the typing core's converters read too), named by the first branch. `None` where the first
    * branch is opaque (`SELECT *`), where no branch has a schema, or where the statement is
    * refused.
    */
  def of(multiple: MultiSearch, dates: SetOperationValues.DateReading): Option[ResultSchema] =
    multiple.requests.headOption.flatMap { first =>
      if (multiple.requests.forall(_.schema.isEmpty)) None
      else
        MultiSearch.projectionTypes(first).flatMap { declared =>
          SetOperationValues
            .routedColumns(multiple.requests, multiple.resolvedOperators, dates)
            .toOption
            .filter(_.size == declared.size)
            .map { columns =>
              ResultSchema(declared.zip(columns).map { case ((field, _), column) =>
                resultColumnOf(field.outputName, column)
              })
            }
        }
    }

  /** A FROM-less statement's schema: its items' declared types, under `columnNames`. */
  def of(fromless: FromlessSelect): ResultSchema =
    ResultSchema(fromless.columnNames.zip(fromless.select.fields).map { case (name, field) =>
      columnOf(name, MultiSearch.fieldType(field), SetOperationValues.DateReading.unknown)
    })

  private def resultColumnOf(name: String, column: SetOperationColumn): ResultColumn =
    columnOf(name, column.result, SetOperationValues.DateReading.unknown)

  /** One position, at every depth; a date position [[DateRoute.of]] answers as stored is `VARCHAR`.
    */
  private[client] def columnOf(
    name: String,
    t: SetOperationType,
    dates: SetOperationValues.DateReading
  ): ResultColumn =
    t.sqlType match {
      case SQLTypes.Struct | SQLTypes.GeoPoint =>
        ResultColumn(
          name,
          t.sqlType,
          fields = t.fields.map(f => columnOf(f.name, f.valueType, dates))
        )
      case SQLTypes.Array(_) =>
        t.element match {
          case Some(e) =>
            val element = columnOf("", e, dates)
            ResultColumn(name, SQLTypes.Array(element.sqlType), element = Some(element))
          case None => ResultColumn(name, t.sqlType)
        }
      case scalar =>
        DateRoute.of(scalar, t.format, dates) match {
          case DateRoute.Typed       => ResultColumn(name, scalar)
          case DateRoute.AsStored(_) => ResultColumn(name, SQLTypes.Varchar)
        }
    }
}
