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

  /** The ELASTICSEARCH-mapping direction, which is NOT the same vocabulary as the SQL one above.
    *
    * 🔴 An Elasticsearch `date` field is a millisecond TIMESTAMP, not a calendar date — which is
    * precisely why `doc['f'].value` hands Painless a `ZonedDateTime`. Mapping it to `Date` claimed
    * a `LocalDate`, and once issue #306 let a column's mapped type reach `SQLTypeUtils.coerce` that
    * lie became executable:
    *   - `CAST(<date column> AS TIMESTAMP)` took `(Date, Timestamp)` and emitted
    *     `.atStartOfDay(ZoneId.of('Z'))`, which a `ZonedDateTime` does not have — measured on real
    *     ES 8.18 as `all shards failed`;
    *   - `CAST(<date column> AS DATE)` was `(Date, Date)`, i.e. the IDENTITY arm, so it returned
    *     the un-truncated timestamp — a SILENT wrong answer, and `CAST(ts AS DATE)` is the
    *     date-truncation idiom Superset and Tableau emit constantly. With `Timestamp` the
    *     already-correct `(Timestamp, Date) => .toLocalDate()`, `(Timestamp, Time) =>
    *     .toLocalTime()` and `(Timestamp, BigInt) => .toEpochMilli()` arms fire, and `CAST(x AS
    *     TIMESTAMP)` becomes the identity it should always have been. The LocalDate- shaped `(Date,
    *     ...)` arms stay for LITERAL operands, which genuinely are calendar dates.
    *
    * Safe in the inverse direction, and this was verified BEFORE the change rather than after:
    * `Column.diff` compares `elasticType(actual) != elasticType(desired)`, and `elasticType` sends
    * Date, Time, DateTime, Timestamp and Temporal ALL to `"date"` — so no existing index reports a
    * spurious type change in `SHOW CREATE TABLE`, table diff or ALTER. `CREATE TABLE t (c DATE)`
    * still creates an ES `date`; it now reads back as TIMESTAMP, which is the honest name for what
    * Elasticsearch stored (release note).
    *
    * `date_nanos` is deliberately untouched — it falls to `Any`, as BIDC-4 decided.
    *
    * Kept on THIS overload rather than in `apply(String)`: that one also parses SQL type names, so
    * moving the arm there would make `CREATE TABLE t (c DATE)` declare a TIMESTAMP column.
    */
  def apply(field: IndexField): SQLType = field.`type`.toLowerCase match {
    case "date" => Timestamp
    case other  => apply(other)
  }

}
