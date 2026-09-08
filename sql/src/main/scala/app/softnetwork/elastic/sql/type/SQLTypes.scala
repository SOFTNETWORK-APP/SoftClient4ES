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

package app.softnetwork.elastic.sql.`type`

import app.softnetwork.elastic.schema.IndexField

object SQLTypes {
  case object Any extends SQLAny { val typeId = "ANY" }

  case object Null extends SQLNull { val typeId = "NULL" }

  case object Temporal extends SQLTemporal { val typeId = "TEMPORAL" }

  case object Date extends SQLTemporal with SQLDate { val typeId = "DATE" }
  case object Time extends SQLTemporal with SQLTime { val typeId = "TIME" }
  case object DateTime extends SQLTemporal with SQLDateTime { val typeId = "DATETIME" }
  case object Timestamp extends SQLTimestamp { val typeId = "TIMESTAMP" }

  case object Numeric extends SQLNumeric { val typeId = "NUMERIC" }

  case object TinyInt extends SQLTinyInt { val typeId = "TINYINT" }
  case object SmallInt extends SQLSmallInt { val typeId = "SMALLINT" }
  case object Int extends SQLInt { val typeId = "INT" }
  case object BigInt extends SQLBigInt { val typeId = "BIGINT" }
  case object Double extends SQLDouble { val typeId = "DOUBLE" }
  case object Real extends SQLReal { val typeId = "REAL" }

  case object Literal extends SQLLiteral { val typeId = "LITERAL" }

  case object Char extends SQLChar { val typeId = "CHAR" }
  case object Varchar extends SQLVarchar { val typeId = "VARCHAR" }
  case object Text extends EsqlText { val typeId = "TEXT" }
  case object Keyword extends EsqlKeyword { val typeId = "KEYWORD" }

  case object Boolean extends SQLBool { val typeId = "BOOLEAN" }

  case class Array(elementType: SQLType) extends SQLArray {
    val typeId = s"ARRAY<${elementType.typeId}>"
  }

  case object Struct extends SQLStruct { val typeId = "STRUCT" }

  case object GeoPoint extends EsqlGeoPoint { val typeId = "GEO_POINT" }

  case object VarBinary extends SQLVarBinary { val typeId = "VARBINARY" }

  def apply(typeName: String): SQLType = typeName.toLowerCase match {
    case "null"                     => Null
    case "boolean"                  => Boolean
    case "int" | "integer"          => Int
    case "long" | "bigint"          => BigInt
    case "short" | "smallint"       => SmallInt
    case "byte" | "tinyint"         => TinyInt
    case "keyword"                  => Keyword
    case "text"                     => Text
    case "varchar"                  => Varchar
    case "timestamp"                => Timestamp
    case "datetime"                 => DateTime
    case "date"                     => Date
    case "time"                     => Time
    case "double"                   => Double
    case "float" | "real"           => Real
    case "object" | "struct"        => Struct
    case "nested" | "array<struct>" => Array(Struct)
    case "geo_point"                => GeoPoint
    case "binary" | "varbinary"     => VarBinary
    case _                          => Any
  }

  /** The Elasticsearch-mapping direction. A field's DECLARED type is preserved verbatim: when we
    * created the index we recorded it in `_meta.columns.<c>.data_type`, and `IndexField.apply`
    * prefers that over Elasticsearch's own mapping type, so `CREATE TABLE t (c DATE)` reads back as
    * DATE and the `_meta` round trip is exact.
    *
    * 🔴 This deliberately does NOT map `date` to `Timestamp`. Story 21.5 tried that here and it was
    * the wrong layer: it conflated the type the USER DECLARED with the type Painless SEES at
    * runtime, and the consequences cascaded — a spurious `_meta.columns` diff on every existing
    * index with a date column, and from the workaround for that, an unparseable ALTER on ES 6.8 and
    * a materialized-view slowdown. The runtime type is now derived where it is actually needed, at
    * `GenericIdentifier.baseType`; see `SQLTypeUtils.runtimeType`.
    */
  def apply(field: IndexField): SQLType = apply(field.`type`)

}
