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

package app.softnetwork.elastic.client.metadata

import app.softnetwork.elastic.sql.`type`._

import java.sql.{JDBCType, Types}
import java.util.Locale
import scala.util.Try

/** The ONE authority for how this engine's types appear over JDBC and Flight SQL.
  *
  * Story BIDC-10a. Before this existed, `DatabaseMetaData.getTypeInfo` in the JDBC driver was a
  * hand-written list of eleven rows deriving from nothing, and the Arrow Flight SQL producer
  * hand-wrote its own eleven rows with `java.sql.Types` imported directly - two parallel
  * transcriptions of a mapping that already existed. That is why the catalogue was incomplete
  * (`getColumns.TYPE_NAME` reports `KEYWORD`, `TEXT`, `DATETIME`, `CHAR`, `STRUCT`, `GEO_POINT`,
  * `VARBINARY` and `ARRAY<...>`, none of which had a catalogue row to round-trip to), why it
  * inherited a javadoc-violating row order, and why the "agreement gate" between the two repos was
  * a convention rather than an interlock. It is Part D's lesson one layer up: two places encoding
  * one fact.
  *
  * This object lives in `core`, not `sql`: `sql` is the AST and parser module and must not gain a
  * `java.sql` dependency, while both the JDBC driver and the Flight producer already compile
  * against `core` (verified in their own sources, not inferred from a build file).
  */
object JdbcTypeCatalog {

  /** SQLType -> `java.sql.Types` code. THE mapping; nothing else may re-list these codes.
    *
    * Ordering of the cases matters: the specific `case object`s come before the trait patterns that
    * would also match them, and the trait fallbacks at the end are what give an unknown-but-typed
    * value a sensible code instead of `OTHER`.
    */
  def jdbcType(sqlType: SQLType): Int = sqlType match {
    case SQLTypes.Int       => Types.INTEGER
    case SQLTypes.BigInt    => Types.BIGINT
    case SQLTypes.Double    => Types.DOUBLE
    case SQLTypes.Real      => Types.REAL
    case SQLTypes.TinyInt   => Types.TINYINT
    case SQLTypes.SmallInt  => Types.SMALLINT
    case SQLTypes.Boolean   => Types.BOOLEAN
    case SQLTypes.Date      => Types.DATE
    case SQLTypes.Time      => Types.TIME
    case _: SQLDateTime     => Types.TIMESTAMP
    case SQLTypes.Char      => Types.CHAR
    case _: SQLVarchar      => Types.VARCHAR // includes Text, Keyword, Varchar
    case _: SQLArray        => Types.ARRAY
    case SQLTypes.Struct    => Types.STRUCT
    case SQLTypes.GeoPoint  => Types.VARCHAR // serialized as a string
    case SQLTypes.VarBinary => Types.VARBINARY
    case SQLTypes.Null      => Types.NULL
    case _: SQLNumeric      => Types.DOUBLE // fallback for unknown numeric
    case _: SQLTemporal     => Types.TIMESTAMP // fallback for unknown temporal
    case _: SQLLiteral      => Types.VARCHAR // fallback for unknown literal
    case _                  => Types.OTHER
  }

  /** One catalogue row. `typeId` and `dataType` are DERIVED - a row cannot state a code that
    * disagrees with [[jdbcType]], because it does not state one at all.
    *
    * @param columnSize
    *   the MAXIMUM precision the TYPE supports (JDBC `getTypeInfo.PRECISION`). Not to be confused
    *   with `ResultSetMetaData.getPrecision`, which reports the specified size of one COLUMN and is
    *   256 for character data by story 20.3's AD-5. Both are correct and they answer different
    *   questions (BIDC-10a lead ruling LR-3).
    */
  case class JdbcTypeInfo(
    sqlType: SQLType,
    columnSize: Int,
    literalPrefix: Option[String],
    literalSuffix: Option[String]
  ) {

    /** What `getColumns.TYPE_NAME` reports for a column of this type - the ENGINE's type id, not a
      * JDBC name. A character column comes back as `KEYWORD` or `TEXT`, never `VARCHAR`, which is
      * exactly why those types need catalogue rows of their own.
      */
    def typeId: String = sqlType.typeId
    def dataType: Int = jdbcType(sqlType)
  }

  private val Quote = Some("'")
  private val NoLiteral: Option[String] = None

  /** The engine's ceiling for character and binary payloads. Inherited from the eleven-row
    * catalogue's `VARCHAR` row, and applied UNIFORMLY to every type that shares a character or
    * binary `DATA_TYPE` - a constant justified by what the JDBC type can hold does not get
    * exceptions (see the size rule below).
    */
  private val CharacterCeiling = 65535

  /** No meaningful precision. `ResultSetMetaData.getPrecision` already answers `0` for these. */
  private val NoPrecision = 0

  /** 🔴 The size rule, stated once so that nine new rows are not nine invented numbers.
    *
    * `PRECISION` describes what the JDBC TYPE can hold, and the `DATA_TYPE` code IS the JDBC type ⇒
    * **types sharing a `DATA_TYPE` share its precision.** Every value below is either one of the
    * eleven inherited from the shipped catalogue (byte-identical - none moved) or forced by that
    * rule:
    *
    *   - `NUMERIC` shares `DOUBLE`'s code ⇒ 15. ES `scaled_float` is a long plus a scaling factor,
    *     so double precision is also what it actually carries.
    *   - `DATETIME` shares `TIMESTAMP`'s code ⇒ 23, the width of `yyyy-MM-dd HH:mm:ss.SSS`.
    *   - `TEXT`, `KEYWORD` and `GEO_POINT` share `VARCHAR`'s code ⇒ the character ceiling.
    *     `GEO_POINT` is a string as far as JDBC is concerned; its own serialized form is far
    *     shorter, but that is a property of the values, not of the type's maximum.
    *   - `CHAR` and `VARBINARY` have codes of their own and no engine-imposed bound, so they take
    *     the same ceiling rather than a number invented for each.
    *   - `STRUCT` and `ARRAY<STRUCT>` have no precision at all.
    *
    * The literal prefix/suffix follows the same shape: everything a user writes QUOTED carries
    * `'`/`'`; numbers and booleans carry neither; `STRUCT` and `ARRAY<STRUCT>` carry neither
    * because there is no literal syntax for them.
    *
    * ⚠️ **`NULL` is deliberately absent.** `getTypeInfo` describes types a column can HAVE;
    * `Types.NULL` is the absence of a value, not a storable type, and no mainstream driver lists
    * it. Its presence in [[jdbcType]] is legitimate and different - that maps an INFERRED RUNTIME
    * type, which is a separate question from what a column may be declared as. Do not "complete"
    * the catalogue by adding it.
    *
    * Declared in an arbitrary (source-natural) order on purpose: the shipped order is COMPUTED by
    * [[catalogOrdering]], so no one can hand-place a row.
    */
  private val declared: Seq[JdbcTypeInfo] = Seq(
    JdbcTypeInfo(SQLTypes.TinyInt, 3, NoLiteral, NoLiteral),
    JdbcTypeInfo(SQLTypes.SmallInt, 5, NoLiteral, NoLiteral),
    JdbcTypeInfo(SQLTypes.Int, 10, NoLiteral, NoLiteral),
    JdbcTypeInfo(SQLTypes.BigInt, 19, NoLiteral, NoLiteral),
    JdbcTypeInfo(SQLTypes.Real, 7, NoLiteral, NoLiteral),
    JdbcTypeInfo(SQLTypes.Double, 15, NoLiteral, NoLiteral),
    JdbcTypeInfo(SQLTypes.Numeric, 15, NoLiteral, NoLiteral),
    JdbcTypeInfo(SQLTypes.Boolean, 1, NoLiteral, NoLiteral),
    JdbcTypeInfo(SQLTypes.Date, 10, Quote, Quote),
    JdbcTypeInfo(SQLTypes.Time, 8, Quote, Quote),
    JdbcTypeInfo(SQLTypes.DateTime, 23, Quote, Quote),
    JdbcTypeInfo(SQLTypes.Timestamp, 23, Quote, Quote),
    JdbcTypeInfo(SQLTypes.Keyword, CharacterCeiling, Quote, Quote),
    JdbcTypeInfo(SQLTypes.Text, CharacterCeiling, Quote, Quote),
    JdbcTypeInfo(SQLTypes.Varchar, CharacterCeiling, Quote, Quote),
    JdbcTypeInfo(SQLTypes.Char, CharacterCeiling, Quote, Quote),
    JdbcTypeInfo(SQLTypes.Struct, NoPrecision, NoLiteral, NoLiteral),
    JdbcTypeInfo(SQLTypes.Array(SQLTypes.Struct), NoPrecision, NoLiteral, NoLiteral),
    JdbcTypeInfo(SQLTypes.GeoPoint, CharacterCeiling, Quote, Quote),
    JdbcTypeInfo(SQLTypes.VarBinary, CharacterCeiling, Quote, Quote)
  )

  /** The JDBC name of a type code, e.g. 12 -> `VARCHAR`. `None` for a code outside the standard
    * enumeration, which cannot happen for anything in [[declared]] but must not throw if it ever
    * does.
    */
  def canonicalJdbcName(dataType: Int): Option[String] =
    Try(JDBCType.valueOf(dataType).getName).toOption

  /** 🔴 The ordering rule, and it CHANGED when the catalogue grew.
    *
    * `DatabaseMetaData.getTypeInfo`'s javadoc: rows *"are ordered by DATA_TYPE and then by how
    * closely the data type maps to the corresponding JDBC SQL type"*. With eleven rows every code
    * was distinct, so ascending `DATA_TYPE` was a TOTAL order and the second clause never had to be
    * adjudicated - both repos asserted "sorted AND distinct" and were right to.
    *
    * That is no longer true. `KEYWORD`, `TEXT`, `VARCHAR` and `GEO_POINT` all carry
    * `Types.VARCHAR`; `NUMERIC` shares `Types.DOUBLE` with `DOUBLE`; `DATETIME` shares
    * `Types.TIMESTAMP` with `TIMESTAMP`. **Distinctness is gone and the second clause is live.**
    * Any assertion still pinning distinctness is asserting the old catalogue.
    *
    * The tie-break, in two steps, both mechanical:
    *
    *   1. **The canonically named type first.** "How closely the data type maps to the JDBC SQL
    *      type" is read as: the type whose own id IS that JDBC type's name is the closest possible
    *      match. Computed from [[canonicalJdbcName]], never hand-listed - so `VARCHAR` precedes
    *      `GEO_POINT`/`KEYWORD`/`TEXT`, `DOUBLE` precedes `NUMERIC`, and `TIMESTAMP` precedes
    *      `DATETIME`, with nothing enumerated by hand. (`INT` does not match `INTEGER` and
    *      `ARRAY<STRUCT>` does not match `ARRAY`; both are alone in their groups, so it costs
    *      nothing.) 2. **Then `typeId` ascending**, `Locale.ROOT`, purely to make the order TOTAL
    *      and stable. A tie-break that leaves ties is not a tie-break: two rows the sort considers
    *      equal may come back in either order and a positional fixture would flake.
    */
  val catalogOrdering: Ordering[JdbcTypeInfo] =
    Ordering.by { info: JdbcTypeInfo =>
      val canonical = canonicalJdbcName(info.dataType).contains(info.typeId)
      (info.dataType, if (canonical) 0 else 1, info.typeId.toUpperCase(Locale.ROOT))
    }

  /** The catalogue, in contract order. This is what `getTypeInfo` and `CommandGetXdbcTypeInfo`
    * stream; neither may re-order it, filter it, or add to it.
    */
  val entries: Seq[JdbcTypeInfo] = declared.sorted(catalogOrdering)

  /** Lookup by the engine's type id, for a driver resolving `getColumns.TYPE_NAME` back to a
    * catalogue row.
    */
  def find(typeId: String): Option[JdbcTypeInfo] =
    entries.find(_.typeId.equalsIgnoreCase(typeId))
}
