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

import app.softnetwork.elastic.sql.query.MultiSearch
import app.softnetwork.elastic.sql.query.MultiSearch.{
  SetOperationColumn,
  SetOperationType,
  StructField
}
import app.softnetwork.elastic.sql.query.SingleSearch
import app.softnetwork.elastic.sql.`type`.{SQLType, SQLTypes}

import java.nio.charset.StandardCharsets
import java.time.format.DateTimeFormatter
import java.time.{
  Instant,
  LocalDate,
  LocalDateTime,
  LocalTime,
  OffsetDateTime,
  OffsetTime,
  ZoneOffset,
  ZonedDateTime
}
import scala.collection.immutable.ListMap
import scala.jdk.CollectionConverters._

/** The values of a `UNION ALL` column, brought to the ONE type the column has
  * ([[MultiSearch.columnTypes]]: DuckDB 1.5.5's, or `VARCHAR` where DuckDB gives none), at every
  * depth by ONE converter ([[positionConverter]]): every value of a `DOUBLE` position is a
  * `Double`, of a `BIGINT` one a `Long`, of a `VARCHAR` one a `String`, of a `TIMESTAMP` one a
  * `ZonedDateTime`, of a `VARBINARY` one an `Array[Byte]`, of a `STRUCT` or a `GEO_POINT` one a map
  * holding the position's fields in DuckDB's order, of a list a list of its element's type -- never
  * a mix, which is what JDBC and Arrow, typing a column from its first value, would otherwise
  * report wrongly.
  *
  * 🔴 PER-ROW work on the extraction path, so it is decided ONCE per statement: [[converters]] is
  * `None` for every leg of a statement whose columns all agree -- no function, no rebuild -- and,
  * for a leg that does convert, holds a function only at the positions whose column converts. The
  * functions are applied as `rowProjector` rebuilds the row it builds anyway, never by a second
  * pass.
  */
private[client] object SetOperationValues {

  /** The per-position conversions of each leg of `multiple`, in leg order -- `None` for a leg with
    * nothing to convert. `multiple` carries its branches' schemas (`resolveWithSchema`).
    *
    * `version` is the Elasticsearch version the statement runs on (`VersionApi.version`): a date
    * field's values are read as THAT major reads its mapping format ([[MappedDateFormat]]). It is
    * asked at most ONCE per statement, and only when a converting position reads a date field.
    */
  def converters(
    multiple: MultiSearch,
    version: => Option[String]
  ): Seq[Option[Array[Any => Any]]] =
    MultiSearch.columnTypes(multiple.requests) match {
      case Right(columns) if columns.exists(_.converts) =>
        val names = columnNames(multiple, columns.size)
        val dates = DateReading(version)
        multiple.requests.indices.map(leg =>
          legConverters(multiple.requests(leg), leg, columns, names, dates)
        )
      case _ => multiple.requests.map(_ => None)
    }

  /** How a statement reads its date fields: as the Elasticsearch major it runs on reads their
    * mapping format ([[MappedDateFormat.Dialect]]). The version is asked once, the first time a
    * converting position reads a date field, and never for a statement that has none.
    */
  private[client] final class DateReading(
    version: => Option[String],
    val clock: java.time.Clock = java.time.Clock.systemUTC()
  ) {
    lazy val elasticVersion: Option[String] = version
    lazy val dialect: Option[MappedDateFormat.Dialect] =
      elasticVersion.flatMap(MappedDateFormat.Dialect.of)
  }

  private[client] object DateReading {
    def apply(version: => Option[String]): DateReading = new DateReading(version)

    /** A statement whose Elasticsearch version is not known. */
    def unknown: DateReading = new DateReading(None)
  }

  /** The name each column takes: the first declaring branch's, as SQL and DuckDB name it. */
  private def columnNames(multiple: MultiSearch, width: Int): Seq[String] =
    multiple.requests
      .map(_.select.fieldsWithComputedAliases)
      .collectFirst { case fields if fields.size == width => fields.map(_.outputName) }
      .getOrElse(Seq.fill(width)(""))

  private def legConverters(
    leg: SingleSearch,
    index: Int,
    columns: Seq[SetOperationColumn],
    names: Seq[String],
    dates: DateReading
  ): Option[Array[Any => Any]] =
    // a leg's rows are built from its declared projection, position by position (issue #354); a
    // projection of another width -- an opaque `SELECT *` -- has no position to convert at
    if (leg.select.fieldsWithComputedAliases.size != columns.size) None
    else {
      val functions: Array[Any => Any] = columns.zipWithIndex.map { case (column, position) =>
        // a NULL literal's branch too: its value is Elasticsearch's `[null]`, read as NULL
        if (!column.converts || column.branches.lift(index).flatten.isEmpty) null
        else {
          val item = leg.select.fieldsWithComputedAliases(position)
          val field = Option(item.identifier.name).filter(_.nonEmpty).getOrElse(item.outputName)
          positionConverter(column.result, column.branches, index, names(position), field, dates)
        }
      }.toArray
      if (functions.forall(_ == null)) None else Some(functions)
    }

  /** The conversion of branch `index`'s value at ONE converting position -- a column, a field of a
    * struct, the element of a list -- to the position's type, by ONE converter at every depth:
    * EVERY branch's value at a converting position is converted, the branch whose type is already
    * the position's too, so the position holds one class.
    *
    *   - A scalar is converted by [[converter]]: a `BIGINT` is a `Long`, a `DOUBLE` a `Double`, a
    *     `TIMESTAMP` a `ZonedDateTime`, a `VARBINARY` bytes, a `VARCHAR` DuckDB's spelling; a
    *     multi-valued value FAILS, in DuckDB's words ([[nonScalarFailure]]). A date field's value
    *     is read with its mapping format, as `dates` says the Elasticsearch major reads it; `field`
    *     is its path, which a failure names.
    *   - A struct is a map of the position's fields in DuckDB's order ([[structConverter]]), a
    *     field a branch does not have NULL.
    *   - A list is a list of converted elements ([[listConverter]]).
    *   - A point is a `{lat, lon}`, whatever way Elasticsearch stored it ([[geoPoint]]).
    *
    * Inside a converting struct or list, a position whose branches all agree keeps its values as
    * read ([[SetOperationColumn.converts]]); a point is always read as a point. Decided ONCE per
    * leg, never per row.
    */
  private[client] def positionConverter(
    target: SetOperationType,
    branches: Seq[Option[SetOperationType]],
    index: Int,
    name: String,
    field: String = "",
    dates: DateReading = DateReading.unknown
  ): Any => Any =
    target.sqlType match {
      case SQLTypes.GeoPoint                        => value => geoPoint(value, name)
      case SQLTypes.Struct if target.fields.isEmpty => identity
      case SQLTypes.Struct   => structConverter(target, branches, index, name, field, dates)
      case SQLTypes.Array(_) => listConverter(target, branches, index, name, field, dates)
      case result =>
        val branch = branches.lift(index).flatten
        val source = branch.fold[SQLType](SQLTypes.Null)(_.sqlType)
        converter(
          source,
          result,
          nonScalarFailure(result, branches.map(_.map(_.sqlType)), index, source, name),
          branch.flatMap(_.format),
          if (field.isEmpty) name else field,
          name,
          dates
        )
    }

  /** The conversion at a position INSIDE a converting one: [[positionConverter]] where its branches
    * disagree, and for a point; its values as read where they agree.
    */
  private def innerConverter(
    target: SetOperationType,
    branches: Seq[Option[SetOperationType]],
    index: Int,
    name: String,
    field: String,
    dates: DateReading
  ): Any => Any =
    if (
      target.sqlType == SQLTypes.GeoPoint ||
      SetOperationColumn.converts(target, branches.flatten)
    ) positionConverter(target, branches, index, name, field, dates)
    else identity

  /** The failure of a converting position that a NON-SCALAR value -- a multi-valued field, an
    * object -- reaches: the position holds ONE value of its type per row, and such a value has no
    * one value to convert. The message is in the words DuckDB uses for the same column, which it
    * makes the LIST of that branch's type before failing the cast of the first other branch into it
    * (MEASURED on DuckDB 1.5.5): `Unimplemented type for cast (INTEGER -> VARCHAR[]) when casting
    * from source column k`, or, the other branch being text, `Type VARCHAR with value 'c' can't be
    * cast to the destination type VARCHAR[]` -- whose value, another branch's, is not known here.
    */
  private def nonScalarFailure(
    result: SQLType,
    branchTypes: Seq[Option[SQLType]],
    index: Int,
    source: SQLType,
    name: String
  ): String = {
    val list = duckDbTypeName(source) + "[]"
    branchTypes.zipWithIndex.collectFirst {
      case (Some(other), i) if i != index && other != SQLTypes.Null => other
    } match {
      case Some(SQLTypes.Varchar) =>
        s"Conversion Error: Type VARCHAR can't be cast to the destination type $list when " +
          s"casting from source column $name"
      case Some(other) =>
        s"Conversion Error: Unimplemented type for cast (${duckDbTypeName(other)} -> $list) " +
          s"when casting from source column $name"
      case None =>
        s"Conversion Error: Unimplemented type for cast ($list -> " +
          s"${duckDbTypeName(result)}) when casting from source column $name"
    }
  }

  /** The name DuckDB gives a type in its messages. */
  private def duckDbTypeName(t: SQLType): String =
    t match {
      case SQLTypes.Varchar | SQLTypes.Char | SQLTypes.Text | SQLTypes.Keyword => "VARCHAR"
      case SQLTypes.Int                                                        => "INTEGER"
      case SQLTypes.Real                                                       => "FLOAT"
      case SQLTypes.Numeric                                                    => "DOUBLE"
      case SQLTypes.VarBinary                                                  => "BLOB"
      case SQLTypes.DateTime                                                   => "TIMESTAMP"
      case SQLTypes.Array(element) => duckDbTypeName(element) + "[]"
      case other                   => other.typeId
    }

  /** A struct's type as DuckDB names it: `STRUCT(lat DOUBLE, lon DOUBLE, a INTEGER, b VARCHAR)`. */
  private def duckDbStructName(fields: Seq[StructField]): String =
    fields
      .map { f =>
        val typeName =
          if (f.fields.nonEmpty) duckDbStructName(f.fields) else duckDbTypeName(f.sqlType)
        s"${f.name} $typeName"
      }
      .mkString("STRUCT(", ", ", ")")

  /** A value reaching a converting `STRUCT` position: a map holding the position's fields, in its
    * order -- DuckDB's merged struct, MEASURED. A field the branch does not have is NULL; every
    * other field is read under the branch's own name, whatever its case, and converted where its
    * branches disagree ([[innerConverter]]): an `INTEGER` beside a `VARCHAR` becomes `'1'`, an
    * `INTEGER` beside a `BIGINT` a `Long`, in every branch.
    *
    * A point branch is read as `{lat, lon}` whatever the way Elasticsearch stored it
    * ([[geoPoint]]). A value with several points or several objects FAILS the statement, in
    * DuckDB's words, as a multi-valued value does in every converting position; so does a value
    * that is not an object. Decided ONCE per leg: the source field each field reads, and its
    * conversion.
    */
  private[client] def structConverter(
    target: SetOperationType,
    branches: Seq[Option[SetOperationType]],
    index: Int,
    name: String,
    field: String = "",
    dates: DateReading = DateReading.unknown
  ): Any => Any = {
    val source = branches.lift(index).flatten
    val sourceFields = source.fold[Seq[StructField]](Nil)(_.fields)
    val read: Any => scala.collection.Map[String, Any] =
      if (source.exists(_.sqlType == SQLTypes.GeoPoint))
        value => geoPoint(value, name, target.fields)
      else value => objectOf(value, name, sourceFields, target.fields)
    val path = if (field.isEmpty) name else field
    val plan: Seq[(String, Option[(String, Any => Any)])] = target.fields.map { t =>
      t.name -> sourceFields.find(_.name.equalsIgnoreCase(t.name)).map { f =>
        f.name -> innerConverter(
          t.valueType,
          SetOperationColumn.fieldTypes(branches, t.name),
          index,
          name,
          s"$path.${f.name}",
          dates
        )
      }
    }
    value =>
      read(value) match {
        case null => null
        case map =>
          ListMap(plan.map { case (field, from) =>
            field -> from.map { case (key, convert) => convert(valueOf(map, key)) }.orNull
          }: _*)
      }
  }

  /** A value reaching a converting list position: a list of its elements, each converted where the
    * elements' branches disagree ([[innerConverter]]). A single value -- Elasticsearch stores a
    * list of one as the value itself -- is a list of one; NULL is NULL, a `NULL` literal's branch
    * (Elasticsearch's `[null]`) too.
    */
  private[client] def listConverter(
    target: SetOperationType,
    branches: Seq[Option[SetOperationType]],
    index: Int,
    name: String,
    field: String = "",
    dates: DateReading = DateReading.unknown
  ): Any => Any =
    if (branches.lift(index).flatten.forall(_.sqlType == SQLTypes.Null)) _ => null
    else {
      val element = innerConverter(
        target.element.getOrElse(SetOperationType(SQLTypes.Any)),
        SetOperationColumn.elementTypes(branches),
        index,
        name,
        if (field.isEmpty) name else field,
        dates
      )
      val list: Any => Any = {
        case null                 => null
        case l: java.util.List[_] => l.asScala.toList.map(element)
        case s: Seq[_]            => s.toList.map(element)
        case one                  => List(element(one))
      }
      list
    }

  /** A map's value for `key`, its case aside -- DuckDB matches a struct's fields whatever their
    * case.
    */
  private def valueOf(map: scala.collection.Map[String, Any], key: String): Any =
    map.get(key) match {
      case Some(value) => value
      case None        => map.collectFirst { case (k, v) if k.equalsIgnoreCase(key) => v }.orNull
    }

  private def keyed(map: scala.collection.Map[_, _]): scala.collection.Map[String, Any] =
    map.map { case (k, v) => String.valueOf(k) -> (v: Any) }

  /** An object's value: one map, NULL for no value, or the statement's failure. */
  private def objectOf(
    value: Any,
    name: String,
    fields: Seq[StructField],
    target: Seq[StructField]
  ): scala.collection.Map[String, Any] =
    value match {
      case null                          => null
      case m: scala.collection.Map[_, _] => keyed(m)
      case m: java.util.Map[_, _]        => keyed(m.asScala)
      case l: java.util.List[_]          => objectOf(l.asScala.toList, name, fields, target)
      case s: Seq[_] =>
        if (s.isEmpty) null
        else if (s.lengthCompare(1) == 0) objectOf(s.head, name, fields, target)
        else
          throw new IllegalArgumentException(
            s"Conversion Error: Unimplemented type for cast (${duckDbStructName(fields)}[] -> " +
            s"${duckDbStructName(target)}) when casting from source column $name"
          )
      case other =>
        throw new IllegalArgumentException(
          s"Conversion Error: Could not read '$other' as a STRUCT when casting from source " +
          s"column $name"
        )
    }

  private val Decimal = """[-+]?(?:\d+\.?\d*|\.\d+)(?:[eE][-+]?\d+)?"""
  private val LatLon =
    ("""\s*(""" + Decimal + """)\s*,\s*(""" + Decimal + """)\s*(?:,\s*""" + Decimal + """\s*)?""").r
  private val WktPoint = ("""(?i)\s*POINT\s*\(\s*(""" + Decimal + """)\s+(""" + Decimal +
    """)(?:\s+""" + Decimal + """)?\s*\)\s*""").r
  private val GeoHash = """(?i)[0-9b-hjkmnp-z]{1,12}""".r
  private val Base32 = "0123456789bcdefghjkmnpqrstuvwxyz"

  /** A `GEO_POINT` as `{lat, lon}`, whatever way Elasticsearch accepted it -- MEASURED on 8.18.3,
    * whose `_source` keeps each as it was written: an object `{"lat": .., "lon": ..}` (the numbers
    * possibly as text), a text `"lat,lon"`, an array `[lon, lat]` (a third number, the altitude,
    * ignored), a geohash (the south-west corner of its cell, as Elasticsearch reads one), a WKT
    * `POINT (lon lat)` or a GeoJSON `{"type": "Point", "coordinates": [lon, lat]}`. NULL for no
    * value; the statement's failure for several points, or for a value that is not a point.
    */
  private[client] def geoPoint(
    value: Any,
    name: String,
    target: Seq[StructField] = StructField.GeoPoint
  ): scala.collection.Map[String, Any] = {
    def point(lat: Any, lon: Any): scala.collection.Map[String, Any] =
      (number(lat), number(lon)) match {
        case (Some(la), Some(lo)) =>
          ListMap("lat" -> java.lang.Double.valueOf(la), "lon" -> java.lang.Double.valueOf(lo))
        case _ => notAPoint(value, name)
      }
    def coordinates(s: Seq[_]): scala.collection.Map[String, Any] =
      if (s.isEmpty) null
      else if (
        (s.lengthCompare(2) == 0 || s.lengthCompare(3) == 0) && s.forall(
          _.isInstanceOf[java.lang.Number]
        )
      )
        point(s(1), s.head)
      else if (s.lengthCompare(1) == 0) geoPoint(s.head, name, target)
      else
        throw new IllegalArgumentException(
          s"Conversion Error: Unimplemented type for cast (STRUCT(lat DOUBLE, lon DOUBLE)[] -> " +
          s"${duckDbStructName(target)}) when casting from source column $name"
        )
    value match {
      case null => null
      case m: scala.collection.Map[_, _] =>
        val map = keyed(m)
        (map.get("lat"), map.get("lon"), map.get("type"), map.get("coordinates")) match {
          case (Some(lat), Some(lon), _, _) => point(lat, lon)
          case (_, _, Some(t), Some(c: Seq[_])) if String.valueOf(t).equalsIgnoreCase("Point") =>
            coordinates(c)
          case (_, _, Some(t), Some(c: java.util.List[_]))
              if String.valueOf(t).equalsIgnoreCase("Point") =>
            coordinates(c.asScala.toList)
          case _ => notAPoint(value, name)
        }
      case m: java.util.Map[_, _] => geoPoint(m.asScala, name, target)
      case l: java.util.List[_]   => coordinates(l.asScala.toList)
      case s: Seq[_]              => coordinates(s)
      case s: String =>
        s match {
          case LatLon(lat, lon)   => point(lat, lon)
          case WktPoint(lon, lat) => point(lat, lon)
          case GeoHash()          => geoHash(s.toLowerCase)
          case _                  => notAPoint(value, name)
        }
      case _ => notAPoint(value, name)
    }
  }

  private def number(v: Any): Option[Double] =
    v match {
      case n: java.lang.Number => Some(n.doubleValue())
      case s: String           => scala.util.Try(s.trim.toDouble).toOption
      case _                   => None
    }

  /** A geohash's cell, read as Elasticsearch reads it: its south-west corner (MEASURED, 8.18.3:
    * `u09tv` is `48.8232421875, 2.3291015625`).
    */
  private def geoHash(hash: String): scala.collection.Map[String, Any] = {
    var lat = (-90.0, 90.0)
    var lon = (-180.0, 180.0)
    var even = true
    hash.foreach { c =>
      val bits = Base32.indexOf(c)
      Seq(16, 8, 4, 2, 1).foreach { mask =>
        if (even) {
          val mid = (lon._1 + lon._2) / 2
          lon = if ((bits & mask) != 0) (mid, lon._2) else (lon._1, mid)
        } else {
          val mid = (lat._1 + lat._2) / 2
          lat = if ((bits & mask) != 0) (mid, lat._2) else (lat._1, mid)
        }
        even = !even
      }
    }
    ListMap("lat" -> java.lang.Double.valueOf(lat._1), "lon" -> java.lang.Double.valueOf(lon._1))
  }

  private def notAPoint(value: Any, name: String): Nothing =
    throw new IllegalArgumentException(
      s"Conversion Error: Could not read '$value' as a GEO_POINT when casting from source column " +
      s"$name: a point is an object {lat, lon}, a text 'lat,lon', an array [lon, lat], a geohash " +
      "or a WKT POINT (lon lat)"
    )

  /** The conversion of a value of `source` (the type the branch declares, as
    * [[MultiSearch.columnTypes]] folds it) to `target`, the column's: the value is first READ AS
    * `source` ([[readAs]]: a text Elasticsearch accepted into the field is the value it indexed),
    * then converted -- never from the class its JSON happened to give it.
    *
    * A `script_fields` value arrives wrapped in Elasticsearch's one-element array; it is unwrapped,
    * since the column is ONE value per row, and an empty array, Elasticsearch's spelling of no
    * value, is NULL. A value with SEVERAL values -- a multi-valued field, an object -- has no one
    * value to convert, and FAILS the statement ([[nonScalarFailure]]). A scalar of a class the
    * conversion does not expect is returned as it is.
    */
  def converter(source: SQLType, target: SQLType): Any => Any =
    converter(
      source,
      target,
      s"Conversion Error: Unimplemented type for cast (${duckDbTypeName(source)}[] -> " +
      s"${duckDbTypeName(target)})"
    )

  private[client] def converter(source: SQLType, target: SQLType, nonScalar: String): Any => Any =
    converter(source, target, nonScalar, None, "", "", DateReading.unknown)

  /** [[converter]] for a branch that reads a date FIELD, whose mapping `format` -- its own, or
    * Elasticsearch's default -- reads its values as the major `dates` names reads them
    * ([[readAs]]); `field` is its path and `column` the column it reaches, which a failure names.
    */
  private[client] def converter(
    source: SQLType,
    target: SQLType,
    nonScalar: String,
    format: Option[String],
    field: String,
    column: String,
    dates: DateReading
  ): Any => Any = {
    val convert: Any => Any = target match {
      case SQLTypes.Varchar   => text(source)
      case SQLTypes.Double    => toDouble
      case SQLTypes.Real      => toFloat
      case SQLTypes.BigInt    => toLong
      case SQLTypes.Int       => toInt
      case SQLTypes.SmallInt  => toShort
      case SQLTypes.TinyInt   => toByte
      case SQLTypes.Timestamp => toTimestamp
      case SQLTypes.VarBinary => bytes(source)
      case _                  => identity
    }
    readAs(source, format, field, column, dates) match {
      case None       => scalarOf(convert, nonScalar)
      case Some(read) => scalarOf(read.andThen(convert), nonScalar)
    }
  }

  /** How a value of a branch is READ AS the type the branch declares, before it is converted: as
    * Elasticsearch indexed it -- what its queries and aggregations see -- whatever the JSON
    * `_source` holds (MEASURED, field type by field type, `docvalue_fields` against `_source`):
    *
    *   - an integer field (`BIGINT`, `INT`, `SMALLINT`, `TINYINT`) indexes a text or a fractional
    *     number truncated toward zero: `"5"`, `"5.0"`, `"5.5"`, `5.5` and `"5.5e0"` are 5, `"-5.9"`
    *     is -5, `"5e2"` is 500; an empty text is no value;
    *   - a `DOUBLE` or a `REAL` field indexes a text as its number: `"2.5"`, `" 2.5 "`, `"+2.5"`,
    *     `"1e3"` (1000.0), `"5"` (5.0); an empty text is no value;
    *   - a `BOOLEAN` field indexes `"true"` and `"false"`, and an empty text as `false`;
    *   - a `DATE`, `TIMESTAMP` or `TIME` FIELD indexes a value as its mapping `format` reads it --
    *     its own, else Elasticsearch's default `strict_date_optional_time||epoch_millis` -- on the
    *     Elasticsearch major the statement runs on ([[dateByFormat]]): a `TIMESTAMP` is that
    *     instant, a `DATE` its day and a `TIME` its time of day, in UTC;
    *   - a computed `DATE` or `TIMESTAMP` (no field, so no format) reads digits (`"1706697000000"`,
    *     a number) as epoch milliseconds and an ISO text as its instant, a date-only or zone-less
    *     one in UTC.
    *
    * `None` for a type read as it comes. Decided once per leg; applied only where a position
    * converts.
    */
  private def readAs(
    source: SQLType,
    format: Option[String],
    field: String,
    column: String,
    dates: DateReading
  ): Option[Any => Any] =
    source match {
      case SQLTypes.Date | SQLTypes.Timestamp | SQLTypes.Time if format.isDefined =>
        Some(dateByFormat(source, format.get, field, column, dates))
      case SQLTypes.TinyInt | SQLTypes.SmallInt | SQLTypes.Int | SQLTypes.BigInt => Some(integral)
      case SQLTypes.Double                                                       => Some(double)
      case SQLTypes.Real                                                         => Some(real)
      case SQLTypes.Boolean                                                      => Some(boolean)
      case SQLTypes.Date                                                         => Some(date)
      case SQLTypes.Timestamp                                                    => Some(timestamp)
      case _                                                                     => None
    }

  /** A value of a date field read with its mapping format `spec`, as the Elasticsearch major the
    * statement runs on reads it ([[MappedDateFormat]], built ONCE per position and leg): a
    * `TIMESTAMP` is the instant that major indexed, a `DATE` its day and a `TIME` its time of day,
    * in UTC. A value the format does not read -- or one core cannot read without guessing, or any
    * value when the major is not known -- FAILS the statement, naming the value, the field and the
    * format: never a wrong date, never a value passed on unread.
    */
  private def dateByFormat(
    source: SQLType,
    spec: String,
    field: String,
    column: String,
    dates: DateReading
  ): Any => Any = {
    val typeName = source match {
      case SQLTypes.Date => "DATE"
      case SQLTypes.Time => "TIME"
      case _             => "TIMESTAMP"
    }
    val result: Instant => Any = source match {
      case SQLTypes.Date => _.atZone(ZoneOffset.UTC).toLocalDate
      case SQLTypes.Time => _.atZone(ZoneOffset.UTC).toLocalTime
      case _             => _.atZone(ZoneOffset.UTC)
    }
    def failure(value: Any, detail: String): Nothing =
      throw new IllegalArgumentException(
        s"Conversion Error: Could not read '$value' as a $typeName when casting from source " +
        s"column $column: $detail"
      )
    dates.dialect match {
      case None =>
        val detail =
          s"the date format '$spec' of field $field reads it as the Elasticsearch version does, " +
          dates.elasticVersion.fold("which is not known")(v => s"and core reads dates of no $v")
        value => failure(value, detail)
      case Some(dialect) =>
        val format = MappedDateFormat(spec, dialect, dates.clock)
        value =>
          format.readValue(value) match {
            case MappedDateFormat.Read(instant) => result(instant)
            case MappedDateFormat.Rejected =>
              failure(
                value,
                s"the date format '$spec' of field $field does not read it as ${dialect.label} does"
              )
            case MappedDateFormat.Undecidable(reason) =>
              failure(
                value,
                s"core cannot read it with the date format '$spec' of field $field as " +
                s"${dialect.label} does: $reason"
              )
          }
    }
  }

  private val EpochMillis = """-?\d+""".r

  private def decimal(text: String): Option[java.math.BigDecimal] =
    scala.util.Try(new java.math.BigDecimal(text)).toOption

  private val integral: Any => Any = {
    case s: String =>
      val t = s.trim
      if (t.isEmpty) null
      else decimal(t).fold[Any](s)(d => java.lang.Long.valueOf(d.longValue()))
    case d @ (_: java.lang.Double | _: java.lang.Float | _: java.math.BigDecimal) =>
      java.lang.Long.valueOf(d.asInstanceOf[java.lang.Number].longValue())
    case other => other
  }

  private val double: Any => Any = {
    case s: String =>
      val t = s.trim
      if (t.isEmpty) null
      else decimal(t).fold[Any](s)(d => java.lang.Double.valueOf(d.doubleValue()))
    case other => other
  }

  private val real: Any => Any = {
    case s: String =>
      val t = s.trim
      if (t.isEmpty) null
      else decimal(t).fold[Any](s)(d => java.lang.Float.valueOf(d.floatValue()))
    case other => other
  }

  private val boolean: Any => Any = {
    case "true"       => java.lang.Boolean.TRUE
    case "false" | "" => java.lang.Boolean.FALSE
    case other        => other
  }

  private val timestamp: Any => Any = {
    case s: String if EpochMillis.pattern.matcher(s).matches() =>
      Instant.ofEpochMilli(s.toLong).atZone(ZoneOffset.UTC)
    case other => toTimestamp(other)
  }

  private val date: Any => Any = {
    case d: LocalDate => d
    case other =>
      timestamp(other) match {
        case z: ZonedDateTime => z.withZoneSameInstant(ZoneOffset.UTC).toLocalDate
        case unread           => unread
      }
  }

  /** A value reaching a `VARBINARY` column (DuckDB's `BLOB`) as BYTES -- `Array[Byte]`, the class
    * of core's `VARBINARY` cell (`ValueCoercion.inferType`, `JavaValueConversion`).
    *
    *   - A `VARBINARY` value is the bytes of the text Elasticsearch holds for it, a `binary`
    *     field's base64, in UTF-8: the bytes the relational engine is handed for that field
    *     (arrow's `VarBinaryVector`) and the bytes the JDBC driver's `getBytes` answers.
    *   - A text value is the bytes DuckDB's cast from `VARCHAR` to `BLOB` makes of it
    *     ([[blobOfText]]).
    */
  private def bytes(source: SQLType): Any => Any =
    source match {
      case SQLTypes.VarBinary => {
        case null           => null
        case b: Array[Byte] => b
        case other          => other.toString.getBytes(StandardCharsets.UTF_8)
      }
      case _ => {
        case null           => null
        case b: Array[Byte] => b
        case other          => blobOfText(textOf(other))
      }
    }

  /** DuckDB 1.5.5's cast of a `VARCHAR` to a `BLOB`, MEASURED value by value: every ASCII character
    * is its byte, `\xHH` (two hexadecimal digits) is the byte `HH`, and the cast FAILS -- the
    * statement with it, as DuckDB's does -- on a character outside ASCII, on a backslash followed
    * by anything else, and on a backslash with fewer than three bytes after it. The text is read as
    * its UTF-8 bytes, left to right, and the first fault met names the failure, in DuckDB's words.
    */
  private[client] def blobOfText(text: String): Array[Byte] = {
    val in = text.getBytes(StandardCharsets.UTF_8)
    val out = new java.io.ByteArrayOutputStream(in.length)
    def hex(b: Byte): Int = if (b < 0) -1 else Character.digit(b.toInt, 16)
    def escapeFault(detail: String) =
      new IllegalArgumentException(
        "Conversion Error: Invalid hex escape code encountered in string -> blob conversion of " +
        "string \"" + text + "\": " + detail
      )
    var i = 0
    while (i < in.length) {
      val b = in(i)
      if (b == '\\'.toByte) {
        if (in.length - i - 1 < 3) throw escapeFault("unterminated escape code at end of blob")
        val high = hex(in(i + 2))
        val low = hex(in(i + 3))
        if (in(i + 1) != 'x'.toByte || high < 0 || low < 0)
          throw escapeFault(new String(in, i, 4, StandardCharsets.UTF_8))
        out.write(high * 16 + low)
        i += 4
      } else if (b < 0) {
        // a byte of a character outside ASCII
        throw new IllegalArgumentException(
          "Conversion Error: Invalid byte encountered in STRING -> BLOB conversion of string \"" +
          text + "\". All non-ascii characters must be escaped with hex codes (e.g. \\xAA)"
        )
      } else {
        out.write(b.toInt)
        i += 1
      }
    }
    out.toByteArray
  }

  /** Applies `f` to the ONE value a row holds: a one-element array unwrapped, an empty one NULL,
    * and a value of several values -- an array of more, a map -- the statement's failure,
    * `nonScalar`.
    */
  private def scalarOf(f: Any => Any, nonScalar: String): Any => Any = {
    def one(value: Any): Any =
      value match {
        case null => null
        case s: Seq[_] =>
          if (s.isEmpty) null
          else if (s.lengthCompare(1) == 0) one(s.head)
          else throw new IllegalArgumentException(nonScalar)
        case l: java.util.List[_] =>
          if (l.isEmpty) null
          else if (l.size() == 1) one(l.get(0))
          else throw new IllegalArgumentException(nonScalar)
        case _: scala.collection.Map[_, _] | _: java.util.Map[_, _] =>
          throw new IllegalArgumentException(nonScalar)
        case v => f(v)
      }
    one
  }

  /** A text branch's value as its text. A value core re-read as a temporal when the response was
    * parsed (issue #418) is spelled back by the ISO formatters, which keep its seconds --
    * `10:30:00`, `2024-01-31T10:30:00Z` -- where `toString` drops them (`10:30`). Bytes are their
    * base64 text, the text Elasticsearch holds for a `binary` field.
    */
  private def textOf(value: Any): String =
    value match {
      case s: String         => s
      case t: LocalTime      => DateTimeFormatter.ISO_LOCAL_TIME.format(t)
      case t: OffsetTime     => DateTimeFormatter.ISO_OFFSET_TIME.format(t)
      case d: LocalDateTime  => DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(d)
      case z: ZonedDateTime  => DateTimeFormatter.ISO_ZONED_DATE_TIME.format(z)
      case o: OffsetDateTime => DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(o)
      case i: Instant        => DateTimeFormatter.ISO_INSTANT.format(i)
      case b: Array[Byte]    => java.util.Base64.getEncoder.encodeToString(b)
      case other             => other.toString
    }

  private val toDouble: Any => Any = {
    case null                 => null
    case d: java.lang.Double  => d
    case n: java.lang.Number  => java.lang.Double.valueOf(n.doubleValue())
    case b: java.lang.Boolean => java.lang.Double.valueOf(if (b) 1.0 else 0.0)
    case other                => other
  }

  private val toFloat: Any => Any = {
    case null                 => null
    case f: java.lang.Float   => f
    case n: java.lang.Number  => java.lang.Float.valueOf(n.floatValue())
    case b: java.lang.Boolean => java.lang.Float.valueOf(if (b) 1.0f else 0.0f)
    case other                => other
  }

  private val toLong: Any => Any = {
    case null                 => null
    case l: java.lang.Long    => l
    case n: java.lang.Number  => java.lang.Long.valueOf(n.longValue())
    case b: java.lang.Boolean => java.lang.Long.valueOf(if (b) 1L else 0L)
    case other                => other
  }

  private val toInt: Any => Any = {
    case null                 => null
    case i: java.lang.Integer => i
    case n: java.lang.Number  => java.lang.Integer.valueOf(n.intValue())
    case b: java.lang.Boolean => java.lang.Integer.valueOf(if (b) 1 else 0)
    case other                => other
  }

  private val toShort: Any => Any = {
    case null                 => null
    case s: java.lang.Short   => s
    case n: java.lang.Number  => java.lang.Short.valueOf(n.shortValue())
    case b: java.lang.Boolean => java.lang.Short.valueOf(if (b) 1.toShort else 0.toShort)
    case other                => other
  }

  private val toByte: Any => Any = {
    case null                 => null
    case b: java.lang.Byte    => b
    case n: java.lang.Number  => java.lang.Byte.valueOf(n.byteValue())
    case b: java.lang.Boolean => java.lang.Byte.valueOf(if (b) 1.toByte else 0.toByte)
    case other                => other
  }

  /** A `DATE` becomes the instant its day starts at, in UTC -- DuckDB's `CAST(DATE AS TIMESTAMP)`,
    * and the reading every date rule of the engine already uses; a `TIMESTAMP` keeps its instant.
    */
  private val toTimestamp: Any => Any = {
    case null                => null
    case z: ZonedDateTime    => z
    case d: LocalDate        => d.atStartOfDay(ZoneOffset.UTC)
    case l: LocalDateTime    => l.atZone(ZoneOffset.UTC)
    case o: OffsetDateTime   => o.toZonedDateTime
    case i: Instant          => i.atZone(ZoneOffset.UTC)
    case n: java.lang.Number => Instant.ofEpochMilli(n.longValue()).atZone(ZoneOffset.UTC)
    case other               => other
  }

  /** A value as TEXT, spelled as DuckDB 1.5.5 spells its type when it casts it to `VARCHAR`: text,
    * a `BOOLEAN` (`true` / `false`), a whole number, a `DOUBLE` or a `REAL` ([[FloatingPointText]]:
    * `250000000000.0`, `1e-05`), a `DATE` (`2024-01-31`), a `TIMESTAMP` (`2024-01-31 10:30:00`,
    * `2023-12-25 23:59:59.999`) and a `TIME` (`10:30:00`). A temporal is read in UTC, as every date
    * rule of the engine reads it. A value Elasticsearch returns as text stays that text: a
    * `VARBINARY` value is its base64 text.
    */
  private def text(source: SQLType): Any => Any =
    source match {
      case SQLTypes.TinyInt | SQLTypes.SmallInt | SQLTypes.Int | SQLTypes.BigInt => {
        case s: String           => s
        case n: java.lang.Number => java.lang.Long.toString(n.longValue())
        case other               => other.toString
      }
      // a number of unknown width is a DOUBLE to DuckDB: the relational engine loads it as one
      case SQLTypes.Double | SQLTypes.Numeric => doubleText
      case SQLTypes.Real                      => realText
      case SQLTypes.Date                      => dateText
      case SQLTypes.Timestamp                 => timestampText
      case SQLTypes.Time                      => timeText
      // a temporal of unknown kind is spelled as the kind its value is
      case SQLTypes.Temporal => {
        case d: LocalDate => dateText(d)
        case t: LocalTime => timeText(t)
        case other        => timestampText(other)
      }
      // text, a BOOLEAN (`true` / `false`, DuckDB's spelling too)
      case _ => textOf
    }

  private val doubleText: Any => Any = {
    case null                => null
    case s: String           => s
    case n: java.lang.Number => FloatingPointText.double(n.doubleValue())
    case other               => other.toString
  }

  private val realText: Any => Any = {
    case null                => null
    case s: String           => s
    case n: java.lang.Number => FloatingPointText.real(n.floatValue())
    case other               => other.toString
  }

  private val dateText: Any => Any = {
    case null              => null
    case s: String         => s
    case d: LocalDate      => TemporalText.date(d)
    case z: ZonedDateTime  => TemporalText.date(z.withZoneSameInstant(ZoneOffset.UTC).toLocalDate)
    case o: OffsetDateTime => TemporalText.date(o.atZoneSameInstant(ZoneOffset.UTC).toLocalDate)
    case l: LocalDateTime  => TemporalText.date(l.toLocalDate)
    case i: Instant        => TemporalText.date(i.atZone(ZoneOffset.UTC).toLocalDate)
    case n: java.lang.Number =>
      TemporalText.date(Instant.ofEpochMilli(n.longValue()).atZone(ZoneOffset.UTC).toLocalDate)
    case other => other.toString
  }

  /** A `DATE` value (a date-only timestamp) is the instant its day starts at, as [[toTimestamp]]
    * reads it.
    */
  private val timestampText: Any => Any = {
    case null             => null
    case s: String        => s
    case l: LocalDateTime => TemporalText.timestamp(l)
    case z: ZonedDateTime =>
      TemporalText.timestamp(z.withZoneSameInstant(ZoneOffset.UTC).toLocalDateTime)
    case o: OffsetDateTime =>
      TemporalText.timestamp(o.atZoneSameInstant(ZoneOffset.UTC).toLocalDateTime)
    case d: LocalDate => TemporalText.timestamp(d.atStartOfDay)
    case i: Instant   => TemporalText.timestamp(LocalDateTime.ofInstant(i, ZoneOffset.UTC))
    case n: java.lang.Number =>
      TemporalText.timestamp(
        LocalDateTime.ofInstant(Instant.ofEpochMilli(n.longValue()), ZoneOffset.UTC)
      )
    case other => other.toString
  }

  private val timeText: Any => Any = {
    case null              => null
    case s: String         => s
    case t: LocalTime      => TemporalText.time(t)
    case o: OffsetTime     => TemporalText.time(o.withOffsetSameInstant(ZoneOffset.UTC).toLocalTime)
    case l: LocalDateTime  => TemporalText.time(l.toLocalTime)
    case z: ZonedDateTime  => TemporalText.time(z.withZoneSameInstant(ZoneOffset.UTC).toLocalTime)
    case o: OffsetDateTime => TemporalText.time(o.atZoneSameInstant(ZoneOffset.UTC).toLocalTime)
    case other             => other.toString
  }
}
