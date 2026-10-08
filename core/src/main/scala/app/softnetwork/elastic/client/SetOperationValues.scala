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

import app.softnetwork.elastic.sql.operator.SetOperator
import app.softnetwork.elastic.sql.query.MultiSearch
import app.softnetwork.elastic.sql.query.MultiSearch.{
  SetOperationColumn,
  SetOperationType,
  StructField
}
import app.softnetwork.elastic.sql.query.{SingleSearch, TemporalLiterals}
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
  * The same converter, in its other mode, reads each leg of a set operation AS ITS BRANCH DECLARES
  * IT, at every depth ([[legReaders]]): the reader the relational engine (arrow) types its legs
  * with, and core's `UNION ALL` reads a date column of a mapping format of its own with.
  *
  * 🔴 PER-ROW work on the extraction path, so it is decided ONCE per statement: [[converters]] is
  * `None` for every leg of a statement whose columns all agree -- no function, no rebuild -- and,
  * for a leg that does convert, holds a function only at the positions whose column converts or
  * reads a mapping format. The functions are applied as `rowProjector` rebuilds the row it builds
  * anyway, never by a second pass.
  */
private[elastic] object SetOperationValues {

  /** The per-position conversions of each leg of `multiple`, in leg order -- `None` for a leg with
    * nothing to convert. `multiple` carries its branches' schemas (`resolveWithSchema`).
    *
    *   - A column whose branches disagree ([[SetOperationColumn.converts]]): every branch's value
    *     converted to the column's type ([[positionConverter]]).
    *   - A column whose branches agree, holding -- at the column or in a field or an element at any
    *     depth -- a date field with a mapping format of its own ([[readsMappedDates]]): the dates
    *     there read as their branches declare them ([[mappedDates]]) -- the date Elasticsearch
    *     indexed, never the JSON spelling -- every other value as read; a date core cannot read,
    *     and a multi-valued value, as stored.
    *   - Any other column: its values as read.
    *
    * `version` is the Elasticsearch version the statement runs on (`VersionApi.version`): a date
    * field's values are read as THAT major reads its mapping format ([[MappedDateFormat]]). It is
    * asked at most ONCE per statement, and only when a converting position reads a date field.
    */
  private[client] def converters(
    multiple: MultiSearch,
    version: => Option[String]
  ): Seq[Option[Array[Any => Any]]] =
    MultiSearch.columnTypes(multiple.requests) match {
      case Right(columns) if columns.exists(c => c.converts || readsMappedDates(c)) =>
        val names = columnNames(multiple.requests, columns.size)
        val dates = DateReading(version)
        multiple.requests.indices.map(leg =>
          legConverters(multiple.requests(leg), leg, columns, names, dates)
        )
      case _ => multiple.requests.map(_ => None)
    }

  /** THE READER of each leg of a set operation, column by column -- in leg order, `None` for a leg
    * with nothing to read -- or, `Left`, the statement's refusal (`MultiSearch.branchTypes`'
    * message word for word).
    *
    * THE CONTRACT read by the relational engine (arrow, `app.softnetwork.elastic.arrow`), which
    * types each leg of a set operation by its branch's declared type, as core's `UNION ALL` reads
    * it -- which is why it is `private[elastic]`:
    *
    *   - INPUT: `MultiSearch.columnTypes`' -- the branches in text order, each with its schema
    *     attached, and `MultiSearch.resolvedOperators` (`Nil` is `UNION ALL` everywhere) -- and the
    *     Elasticsearch version the statement runs on (`VersionApi.version`, cached per client),
    *     asked at most ONCE, and only when a reader reads a date field.
    *   - Each function reads ONE raw value of its leg's column, as the response parser gives it
    *     (`ElasticConversion.jsonNodeToAny`), AS ITS BRANCH DECLARES IT
    *     (`SetOperationColumn.branches`), with its mapping format, as Elasticsearch indexed it --
    *     at every depth, where the branches agree too. One class per type: an `INT` is an
    *     `Integer`, a `BIGINT` a `Long`, a `SMALLINT` a `Short`, a `TINYINT` a `Byte`, a `DOUBLE` a
    *     `Double`, a `REAL` a `Float`, a `BOOLEAN` a `Boolean`, a `DATE` a `LocalDate`, a
    *     `TIMESTAMP` a `ZonedDateTime` in UTC, a `TIME` a `LocalTime`, a `VARCHAR` a `String`, a
    *     `VARBINARY` the UTF-8 bytes of its base64 text, a `STRUCT` a `ListMap` of its fields in
    *     the branch's order, a `GEO_POINT` a `ListMap` of `lat` and `lon` (`Double`s), an `ARRAY` a
    *     `List` of its element's class.
    *   - Elasticsearch's one-element array (`script_fields`) is ONE value, its empty array NULL.
    *   - A value core cannot read fails the statement, in the words core's `UNION ALL` uses: a date
    *     its format does not read, or does not read without guessing (a zone name), a point that is
    *     not one, a multi-valued value at a scalar position -- where the branches agree too, in the
    *     words of a converting position ([[nonScalarFailure]]).
    *   - `unreadable` ([[Unreadable]]) says what a DATE core cannot read answers at a position
    *     whose branches AGREE: [[Unreadable.Fail]], the default, fails the statement as above;
    *     [[Unreadable.KeepAsStored]] answers the value exactly as the response parser gives it --
    *     not of the position's class, which the caller must then type as it was stored -- as core's
    *     own `UNION ALL` answers it there. Where the branches disagree such a date always fails, as
    *     core's `UNION ALL` fails it.
    *   - `null` -- or `None` for the whole leg -- where nothing is read: a column whose type is not
    *     known (`ANY`), a branch that declares nothing (`SELECT *`), a `NULL` literal.
    *
    * Built ONCE per leg; no decision is made per row.
    */
  private[elastic] def legReaders(
    requests: Seq[SingleSearch],
    operators: Seq[SetOperator],
    version: => Option[String],
    unreadable: Unreadable = Unreadable.Fail
  ): Either[String, Seq[Option[Array[Any => Any]]]] =
    MultiSearch.columnTypes(requests, operators).map { columns =>
      val names = columnNames(requests, columns.size)
      val dates = DateReading(version)
      requests.indices.map { leg =>
        val search = requests(leg)
        // a leg's rows are built from its declared projection, position by position; a
        // projection of another width -- an opaque `SELECT *` -- has no position to read at
        if (search.select.fieldsWithComputedAliases.size != columns.size) None
        else {
          val readers: Array[Any => Any] = columns.indices.map { position =>
            legReader(
              columns(position),
              leg,
              names(position),
              fieldOf(search, position),
              dates,
              unreadable
            )
          }.toArray
          if (readers.forall(_ == null)) None else Some(readers)
        }
      }
    }

  /** What a leg's reader ([[legReaders]]) answers for a DATE it cannot read -- one its mapping
    * format does not read, or does not read without guessing (a zone name, a pattern letter no
    * measurement covers), or any date of a format when the Elasticsearch version is not known -- at
    * a position whose branches AGREE. Where the branches disagree, the value converts, and such a
    * date FAILS the statement whatever this says, as core's `UNION ALL` fails it.
    */
  private[elastic] sealed trait Unreadable

  private[elastic] object Unreadable {

    /** The statement fails, naming the value, the field and the format. */
    case object Fail extends Unreadable

    /** The value is answered AS STORED, exactly as the response parser gives it (a text, a number,
      * Elasticsearch's array) -- as core's own `UNION ALL` answers it where the branches agree.
      */
    case object KeepAsStored extends Unreadable
  }

  /** How [[positionConverter]] treats a value. */
  private[client] sealed trait Reading

  private[client] object Reading {

    /** Converted to the position's type: core's `UNION ALL` where the branches disagree. */
    case object Converted extends Reading

    /** Read as the branch declares it, at every depth ([[legReaders]]); `unreadable` where the
      * branches agree.
      */
    final case class Declared(unreadable: Unreadable) extends Reading
  }

  /** Whether core's `UNION ALL` reads the dates of a column whose branches AGREE ([[mappedDates]]):
    * the column, or a field or an element at any depth inside it, is a date field with a mapping
    * format of its own, not Elasticsearch's default (`epoch_second`, `yyyy/MM/dd`, ...), whose
    * values the response parser gives as their JSON spells them (`1706697000`, `"2024/01/31"`),
    * never as the date Elasticsearch indexed. Only a date field carries a format; a column of the
    * default format keeps its values as read, at no cost; beside a `SELECT *` branch, whose values
    * are not typed here, nothing is read. Decided once per statement.
    */
  private def readsMappedDates(column: SetOperationColumn): Boolean =
    !column.converts && column.branches.forall(_.isDefined) &&
    column.branches.exists(_.exists(mappedInside))

  /** A date field's mapping format of its own, not Elasticsearch's default. */
  private def mapped(t: SetOperationType): Boolean =
    t.format.exists(_ != TemporalLiterals.DefaultDateFormat)

  private def mappedInside(t: SetOperationType): Boolean =
    mapped(t) || t.fields.exists(f => mappedInside(f.valueType)) || t.element.exists(mappedInside)

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
  private def columnNames(requests: Seq[SingleSearch], width: Int): Seq[String] =
    requests
      .map(_.select.fieldsWithComputedAliases)
      .collectFirst { case fields if fields.size == width => fields.map(_.outputName) }
      .getOrElse(Seq.fill(width)(""))

  /** The field a leg's item reads at `position` -- a failure names it -- or its output name. */
  private def fieldOf(leg: SingleSearch, position: Int): String = {
    val item = leg.select.fieldsWithComputedAliases(position)
    Option(item.identifier.name).filter(_.nonEmpty).getOrElse(item.outputName)
  }

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
        if (column.branches.lift(index).flatten.isEmpty) null
        else if (column.converts)
          positionConverter(
            column.result,
            column.branches,
            index,
            names(position),
            fieldOf(leg, position),
            dates
          )
        else if (readsMappedDates(column))
          mappedDates(
            column.result,
            column.branches,
            index,
            names(position),
            fieldOf(leg, position),
            dates
          ).orNull
        else null
      }.toArray
      if (functions.forall(_ == null)) None else Some(functions)
    }

  /** Core's own `UNION ALL` at a position whose branches AGREE ([[readsMappedDates]]): the reading
    * of the dates of a mapping format of their own at it, at every depth -- `None` where it holds
    * none, its values then as read, at no cost.
    *
    *   - A date position one of whose branches has a format of its own: each branch's date read as
    *     its branch declares it ([[legReaders]]' reader), the one of the default format too, so the
    *     position holds one class; a date core cannot read ([[Unreadable.KeepAsStored]]) and a
    *     multi-valued value ([[severalAsRead]]) as stored.
    *   - An object holding such a position: the same object, every other entry as read, in its
    *     order ([[objectPatch]]); a list of them, or of such dates: each element so
    *     ([[listPatch]]).
    *   - Any other position: as read.
    *
    * Decided ONCE per leg: only an object that holds such a date is rebuilt.
    */
  private def mappedDates(
    target: SetOperationType,
    branches: Seq[Option[SetOperationType]],
    index: Int,
    name: String,
    field: String,
    dates: DateReading
  ): Option[Any => Any] =
    branches.lift(index).flatten.flatMap { own =>
      val path = if (field.isEmpty) name else field
      own.sqlType match {
        case SQLTypes.Struct =>
          val patches = own.fields.flatMap { f =>
            target.fields.find(_.name.equalsIgnoreCase(f.name)).flatMap { t =>
              mappedDates(
                t.valueType,
                SetOperationColumn.fieldTypes(branches, f.name),
                index,
                name,
                s"$path.${f.name}",
                dates
              ).map(f.name -> _)
            }
          }
          if (patches.isEmpty) None else Some(objectPatch(patches))
        case SQLTypes.Array(_) =>
          mappedDates(
            target.element.getOrElse(SetOperationType(SQLTypes.Any)),
            SetOperationColumn.elementTypes(branches),
            index,
            name,
            path,
            dates
          ).map(listPatch)
        case _
            if own.format.isDefined && target.sqlType.isTemporal && branches
              .exists(_.exists(mapped)) =>
          Some(
            severalAsRead(
              positionConverter(
                target,
                branches,
                index,
                name,
                path,
                dates,
                Reading.Declared(Unreadable.KeepAsStored),
                agrees = Some(true)
              )
            )
          )
        case _ => None
      }
    }

  /** An object whose fields `patches` name are read by their function, whatever their case -- every
    * other entry as read, in its order. An object wrapped in Elasticsearch's array is read in it;
    * an array of several objects -- a multi-valued field -- and any other value are answered as
    * read.
    */
  private def objectPatch(patches: Seq[(String, Any => Any)]): Any => Any = {
    val names = patches.map(_._1).toArray
    val reads = patches.map(_._2).toArray
    def readOf(key: String): Any => Any = {
      var i = 0
      while (i < names.length) {
        if (names(i).equalsIgnoreCase(key)) return reads(i)
        i += 1
      }
      null
    }
    def patched(map: scala.collection.Map[_, _]): ListMap[String, Any] = {
      val builder = ListMap.newBuilder[String, Any]
      map.foreach { case (k, v) =>
        val key = String.valueOf(k)
        val read = readOf(key)
        builder += key -> (if (read eq null) v else read(v))
      }
      builder.result()
    }
    def one(value: Any): Any =
      value match {
        case m: scala.collection.Map[_, _] => patched(m)
        case m: java.util.Map[_, _]        => patched(m.asScala)
        case other                         => other
      }
    {
      case s: Seq[_] if s.lengthCompare(1) == 0  => List(one(s.head))
      case l: java.util.List[_] if l.size() == 1 => List(one(l.get(0)))
      case value                                 => one(value)
    }
  }

  /** A list each element of which is read by `element`; a single value -- Elasticsearch stores a
    * list of one as the value itself -- is read as it is.
    */
  private def listPatch(element: Any => Any): Any => Any = {
    case null                 => null
    case s: Seq[_]            => s.toList.map(element)
    case l: java.util.List[_] => l.asScala.toList.map(element)
    case one                  => element(one)
  }

  /** A reader as core's own `UNION ALL` applies it where the branches agree ([[mappedDates]]): a
    * value of several values -- a multi-valued field, an array of them in Elasticsearch's array
    * (`[[1706697000, 1706697001]]`), an object -- is answered as stored there, as it always was,
    * where the reader would fail it (the lead's ruling of 2026-10-08); every other value is read.
    * The value is judged as the reader sees it: Elasticsearch's one-element arrays unwrapped.
    */
  private def severalAsRead(read: Any => Any): Any => Any = {
    @scala.annotation.tailrec
    def several(value: Any): Boolean =
      value match {
        case s: Seq[_] =>
          if (s.lengthCompare(1) == 0) several(s.head) else s.lengthCompare(1) > 0
        case l: java.util.List[_] =>
          if (l.size() == 1) several(l.get(0)) else l.size() > 1
        case _: scala.collection.Map[_, _] | _: java.util.Map[_, _] => true
        case _                                                      => false
      }
    value => if (several(value)) value else read(value)
  }

  /** The reader of branch `index`'s values at ONE column ([[legReaders]]): [[positionConverter]]
    * reading each value as the branch declares it, at every depth -- `null` where nothing is read:
    * a column whose type is not known (`ANY`), a branch that declares nothing there (`SELECT *`), a
    * `NULL` literal.
    */
  private def legReader(
    column: SetOperationColumn,
    index: Int,
    name: String,
    field: String,
    dates: DateReading,
    unreadable: Unreadable
  ): Any => Any =
    column.branches.lift(index).flatten match {
      case Some(branch) if branch.sqlType != SQLTypes.Null && column.resultType != SQLTypes.Any =>
        positionConverter(
          column.result,
          column.branches,
          index,
          name,
          field,
          dates,
          Reading.Declared(unreadable),
          // the column's branches agree as core's `UNION ALL` reads them: a `SELECT *` branch
          // beside them converts nothing
          agrees = Some(!column.converts)
        )
      case _ => null
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
    *
    * [[Reading.Declared]], the leg's READER ([[legReaders]]): the value is read AS ITS BRANCH
    * DECLARES IT -- a scalar as the branch's type, a struct as the branch's fields in the branch's
    * order, a point as a point -- at EVERY depth, where the branches agree too; `target`, the
    * position's type, then names the position's type in a failure, and says whether the branches
    * agree there -- `agrees`, where given, says it instead -- for a date core cannot read.
    */
  private[client] def positionConverter(
    target: SetOperationType,
    branches: Seq[Option[SetOperationType]],
    index: Int,
    name: String,
    field: String = "",
    dates: DateReading = DateReading.unknown,
    reading: Reading = Reading.Converted,
    agrees: Option[Boolean] = None
  ): Any => Any = {
    val branch = branches.lift(index).flatten
    val read = reading != Reading.Converted
    // the type the value is read as: the position's, or, for a leg's reader, the branch's own
    val as = if (read) branch.getOrElse(target) else target
    as.sqlType match {
      case SQLTypes.GeoPoint => value => geoPoint(value, name)
      // a STRUCT whose fields are not known: its values as read
      case SQLTypes.Struct if as.fields.isEmpty || target.fields.isEmpty => identity
      case SQLTypes.Struct =>
        structConverter(target, branches, index, name, field, dates, reading)
      case SQLTypes.Array(_) =>
        listConverter(target, branches, index, name, field, dates, reading)
      case scalar =>
        val source = branch.fold[SQLType](SQLTypes.Null)(_.sqlType)
        // a date core cannot read is answered as stored where the reader is told so and the
        // branches agree -- never where they disagree (the lead's ruling of 2026-10-08)
        val keep = reading match {
          case Reading.Declared(Unreadable.KeepAsStored) =>
            agrees.getOrElse(!SetOperationColumn.converts(target, branches.flatten))
          case _ => false
        }
        // a value of several values fails, in the words of a converting position -- read as
        // declared where the branches agree too: a leg typed by its branch's scalar type cannot
        // carry a list, which core's own `UNION ALL` answers as read there (the lead's ruling of
        // 2026-10-08)
        val convert = converter(
          source,
          scalar,
          nonScalarFailure(target.sqlType, branches.map(_.map(_.sqlType)), index, source, name),
          branch.flatMap(_.format),
          if (field.isEmpty) name else field,
          name,
          dates,
          keepUnreadable = keep
        )
        val canonical =
          if (read && scalar == SQLTypes.Timestamp) convert.andThen(utc) else convert
        if (keep) asStored(canonical) else canonical
    }
  }

  /** A date core cannot read, answered by its reader as [[Unread]]: the value AS STORED, exactly as
    * the reader received it -- Elasticsearch's array and all.
    */
  private def asStored(read: Any => Any): Any => Any =
    value =>
      read(value) match {
        case Unread => value
        case other  => other
      }

  /** What a date reader answers for a date it cannot read, where it keeps the value as stored. */
  private case object Unread

  /** A `TIMESTAMP` a leg's reader reads: in UTC, whatever offset its text carried. */
  private val utc: Any => Any = {
    case z: ZonedDateTime if z.getZone != ZoneOffset.UTC => z.withZoneSameInstant(ZoneOffset.UTC)
    case other                                           => other
  }

  /** The conversion at a position INSIDE a converting one: [[positionConverter]] where its branches
    * disagree, and for a point; where they agree, its values as read, its dates of a mapping format
    * of their own read ([[mappedDates]]) -- read as the branch declares them, with
    * [[Reading.Declared]], wherever they agree or not.
    */
  private def innerConverter(
    target: SetOperationType,
    branches: Seq[Option[SetOperationType]],
    index: Int,
    name: String,
    field: String,
    dates: DateReading,
    reading: Reading
  ): Any => Any =
    if (
      reading != Reading.Converted || target.sqlType == SQLTypes.GeoPoint ||
      SetOperationColumn.converts(target, branches.flatten)
    ) positionConverter(target, branches, index, name, field, dates, reading)
    else mappedDates(target, branches, index, name, field, dates).getOrElse(identity)

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
    *
    * With [[Reading.Declared]] (a leg's reader, [[positionConverter]]), the map holds the BRANCH's
    * fields, in its order and under its names, each read as the branch declares it.
    */
  private[client] def structConverter(
    target: SetOperationType,
    branches: Seq[Option[SetOperationType]],
    index: Int,
    name: String,
    field: String = "",
    dates: DateReading = DateReading.unknown,
    reading: Reading = Reading.Converted
  ): Any => Any = {
    val source = branches.lift(index).flatten
    val sourceFields = source.fold[Seq[StructField]](Nil)(_.fields)
    val mapOf: Any => scala.collection.Map[String, Any] =
      if (source.exists(_.sqlType == SQLTypes.GeoPoint))
        value => geoPoint(value, name, target.fields)
      else value => objectOf(value, name, sourceFields, target.fields)
    val path = if (field.isEmpty) name else field
    def inner(t: SetOperationType, f: StructField): Any => Any =
      innerConverter(
        t,
        SetOperationColumn.fieldTypes(branches, f.name),
        index,
        name,
        s"$path.${f.name}",
        dates,
        reading
      )
    val plan: Seq[(String, Option[(String, Any => Any)])] =
      if (reading != Reading.Converted)
        // the branch's fields, each typed at the position by the field of the column it is
        sourceFields.map { f =>
          val t = target.fields.find(_.name.equalsIgnoreCase(f.name)).fold(f.valueType)(_.valueType)
          f.name -> Some(f.name -> inner(t, f))
        }
      else
        target.fields.map { t =>
          t.name -> sourceFields.find(_.name.equalsIgnoreCase(t.name)).map { f =>
            f.name -> inner(t.valueType, f)
          }
        }
    value =>
      mapOf(value) match {
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
    dates: DateReading = DateReading.unknown,
    reading: Reading = Reading.Converted
  ): Any => Any =
    if (branches.lift(index).flatten.forall(_.sqlType == SQLTypes.Null)) _ => null
    else {
      val element = innerConverter(
        target.element.getOrElse(SetOperationType(SQLTypes.Any)),
        SetOperationColumn.elementTypes(branches),
        index,
        name,
        if (field.isEmpty) name else field,
        dates,
        reading
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
  private[client] def converter(source: SQLType, target: SQLType): Any => Any =
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
    * With `keepUnreadable`, a date the format does not read is [[Unread]] instead of a failure.
    */
  private[client] def converter(
    source: SQLType,
    target: SQLType,
    nonScalar: String,
    format: Option[String],
    field: String,
    column: String,
    dates: DateReading,
    keepUnreadable: Boolean = false
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
    readAs(source, format, field, column, dates, keepUnreadable) match {
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
    dates: DateReading,
    keepUnreadable: Boolean
  ): Option[Any => Any] =
    source match {
      case SQLTypes.Date | SQLTypes.Timestamp | SQLTypes.Time if format.isDefined =>
        Some(dateByFormat(source, format.get, field, column, dates, keepUnreadable))
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
    * format: never a wrong date, never a value passed on unread -- or, with `keepUnreadable`, is
    * [[Unread]], which its reader answers as stored ([[asStored]]).
    */
  private def dateByFormat(
    source: SQLType,
    spec: String,
    field: String,
    column: String,
    dates: DateReading,
    keepUnreadable: Boolean
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
    def failure(value: Any, detail: => String): Any =
      if (keepUnreadable) Unread
      else
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
