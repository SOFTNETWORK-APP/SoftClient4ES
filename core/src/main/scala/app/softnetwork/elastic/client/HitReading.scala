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

import app.softnetwork.elastic.sql.`type`.SQLTypes
import app.softnetwork.elastic.sql.query.{
  Field,
  GroupedRowOrder,
  MultiSearch,
  SingleSearch,
  UnnestScope
}
import app.softnetwork.elastic.sql.query.MultiSearch.SetOperationType
import app.softnetwork.elastic.sql.schema.Column
import com.fasterxml.jackson.databind.JsonNode

import java.time.{Instant, LocalDate, LocalDateTime, LocalTime, ZoneOffset, ZonedDateTime}
import scala.collection.immutable.ListMap
import scala.collection.mutable.ListBuffer

/** SCHEMA-2 (D7) -- what a SEVERAL-element cell in a SCALAR column answers. */
private[client] sealed trait MultiValued

private[client] object MultiValued {

  /** The cell is a `List` of its elements, each read by the column's type (R-22). */
  case object AsRead extends MultiValued

  /** The statement fails, naming the column, the document and the `ALTER` that declares it a list.
    */
  case object Fail extends MultiValued
}

/** SCHEMA-2 (D4-D9) -- how ONE statement reads the values of its hits: by its columns' declared
  * types, where each value is first read (a Jackson node), before any re-read as a temporal.
  *
  *   - `source`: the top-level `_source` keys the projection touches -- a bare column, a dotted
  *     column's parent and its flat spelling (`"obj.a"`), an alias source, every mapped column for
  *     `*` -- each with the reader of its type;
  *   - `scripted`: a computed column's hit `fields` entry, by output name (one-element array
  *     unwrapped);
  *   - `inner`: an UNNEST inner hit's element fields, by inner-hit name then field;
  *   - `ignoredDates`: the `_source` keys read as dates by their format, each with the reader of
  *     its cell where the hit's `_ignored` names it (D8): Elasticsearch's verdict is per VALUE, so
  *     a cell of one value is NULL and an array keeps the elements the format reads, the others
  *     NULL;
  *   - `strict` / `keep`: the projection drops every undeclared key, but `keep` (D9);
  *   - `star`: a bare `SELECT *` -- unmapped top-level keys dropped, a flat dotted key folded into
  *     its mapped parent (A-3).
  *
  * A key the plan does not hold is read as it always was (`ElasticConversion.jsonNodeToAny`). Built
  * ONCE per statement: per row, only the readers run.
  */
private[client] final class HitReading(
  val schema: Option[ResultSchema],
  private[client] val source: java.util.HashMap[String, JsonNode => Any],
  private[client] val scripted: java.util.HashMap[String, JsonNode => Any],
  private[client] val inner: Map[String, java.util.HashMap[String, JsonNode => Any]],
  private[client] val ignoredDates: java.util.HashMap[String, JsonNode => Any],
  val strict: Boolean,
  val keep: Set[String],
  private[client] val star: Boolean,
  private[client] val flatReaders: String => JsonNode => Any
) {

  /** Whether a hit's `_ignored` must be read (D8): the plan reads some `_source` key as a date. */
  private[client] val readsIgnored: Boolean = !ignoredDates.isEmpty

  /** The reader of `key`'s cell when the hit's `_ignored` names it (D8); `null` for a key that is
    * not read as a date, which keeps its own reader (a keyword keeps its stored text).
    */
  private[client] def onIgnored(key: String): JsonNode => Any = ignoredDates.get(key)

  /** Whether a (non-plan) top-level key is folded into its mapped parent under `*` (A-3). */
  private[client] def foldsUnder(key: String): String = {
    val dot = key.indexOf('.')
    if (star && dot > 0) {
      val parent = key.substring(0, dot)
      if (source.containsKey(parent)) parent else null
    } else null
  }
}

private[client] object HitReading {

  /** SCHEMA-6 flips it to [[MultiValued.Fail]], in the same core release (D7). */
  private[client] val Default: MultiValued = MultiValued.AsRead

  /** A bare `*` item. */
  private[client] def isStar(field: Field): Boolean =
    field.identifier.identifierName == "*" && field.identifier.functions.isEmpty

  /** Thrown by a scalar reader under [[MultiValued.Fail]]; the hit loop names the document. */
  private[client] final class MultiValuedCell(val column: String, val table: String)
      extends RuntimeException(column, null, false, false)

  /** The message a several-element cell fails with under [[MultiValued.Fail]] (D7). */
  private[client] def multiValuedMessage(cell: MultiValuedCell, id: Option[String]): String =
    s"Column ${cell.column} holds several values${id.fold("")(i => s" in document $i")}, but is " +
    s"declared as one value: declare it a list with ALTER TABLE ${cell.table} ALTER COLUMN " +
    s"${cell.column} TYPE ARRAY<...>"

  def of(
    single: SingleSearch,
    dates: SetOperationValues.DateReading,
    multiValued: MultiValued = Default,
    elements: Set[String] = Set.empty
  ): HitReading = {
    // each item's and each mapped column's type, computed ONCE per statement: the schema and the
    // readers read the same value; so do the date formats and routes, built once by `dates`
    val itemTypes = new java.util.IdentityHashMap[Field, SetOperationType]()
    val typeOf: Field => SetOperationType = f => {
      val known = itemTypes.get(f)
      if (known ne null) known
      else {
        val t = MultiSearch.fieldType(f)
        itemTypes.put(f, t)
        t
      }
    }
    val columnTypes = new java.util.IdentityHashMap[Column, SetOperationType]()
    val columnTypeOf: Column => SetOperationType = c => {
      val known = columnTypes.get(c)
      if (known ne null) known
      else {
        val t = MultiSearch.columnType(c)
        columnTypes.put(c, t)
        t
      }
    }
    val schema = ResultSchemas.of(single, dates, typeOf, columnTypeOf)
    val table = single.sources.headOption.getOrElse("")
    val aliases = single.fieldAliases
    val readers = new Readers(aliases, dates, multiValued, table)
    val fields = single.select.fieldsWithComputedAliases
    val star = fields.exists(isStar)
    val source = new java.util.HashMap[String, JsonNode => Any]()
    val scripted = new java.util.HashMap[String, JsonNode => Any]()
    val ignoredDates = new java.util.HashMap[String, JsonNode => Any]()
    // the dotted paths the projection selects, relative to their parent (D4)
    val selectedPaths: Map[String, Seq[String]] = fields
      .filterNot(f => isStar(f) || f.identifier.nested || f.identifier.functions.nonEmpty)
      .map(_.identifier.name)
      .filter(n => n.indexOf('.') > 0)
      .groupBy(n => n.substring(0, n.indexOf('.')))
      .map { case (parent, paths) => parent -> paths.map(_.substring(parent.length + 1)) }
    // a parent read whole (`SELECT obj` or `*`) keeps the undeclared keys spelling a selected path
    def keepUnder(parent: String): Set[String] =
      selectedPaths
        .getOrElse(parent, Nil)
        .flatMap { p =>
          val segments = p.split("\\.")
          (1 to segments.length).map(i => segments.take(i).mkString("."))
        }
        .toSet
    def put(key: String, t: SetOperationType, keepKeys: Set[String]): Unit = {
      val (reader, onIgnored) = readers.column(key, t, keepKeys)
      source.put(key, reader)
      onIgnored.foreach(ignoredDates.put(key, _))
    }
    single.schema.foreach { table =>
      if (star)
        table.columns.foreach { c =>
          if (!source.containsKey(c.name))
            put(c.name, columnTypeOf(c), keepUnder(c.name))
        }
    }
    // explicit columns: a bare column (or a dotted column's parent and its flat spelling) by its
    // type; a computed column's `fields` entry by output name
    val paths =
      scala.collection.mutable.LinkedHashMap.empty[String, ListBuffer[(String, SetOperationType)]]
    fields.filterNot(isStar).foreach { f =>
      val id = f.identifier
      if (id.functions.nonEmpty || f.isScriptField) {
        val name = f.outputName
        val t = typeOf(f)
        if (name.nonEmpty && !scripted.containsKey(name)) {
          if (single.schema.isDefined) scripted.put(name, readers.scripted(name, t))
          // no schema (D10): rows as today -- a column declared VARCHAR is its text, never re-read
          // as a temporal, Elasticsearch's array kept
          else if (t.sqlType == SQLTypes.Varchar) scripted.put(name, readers.textAsRead)
        }
      } else if (!id.nested && id.name.nonEmpty && single.schema.isDefined) {
        val name = id.name
        val t = typeOf(f)
        val dot = name.indexOf('.')
        if (dot < 0) {
          if (!source.containsKey(name)) put(name, t, keepUnder(name))
        } else {
          val parent = name.substring(0, dot)
          paths.getOrElseUpdate(parent, ListBuffer.empty) += (name.substring(dot + 1) -> t)
          // SCHEMA-12 A-3's flat spelling, read first by `flatValue`
          if (!source.containsKey(name)) {
            val (reader, _) = readers.column(name, t, Set.empty)
            source.put(name, reader)
          }
        }
      }
    }
    // a dotted column's parent not read whole: its selected paths by their types, every other key
    // as read
    paths.foreach { case (parent, under) =>
      if (!source.containsKey(parent)) source.put(parent, readers.paths(parent, under.toList))
    }
    // UNNEST element fields, by inner hit; a column with an element conversion keeps its raw value
    val inner: Map[String, java.util.HashMap[String, JsonNode => Any]] =
      if (single.schema.isEmpty) Map.empty
      else
        (single.nestedHitsMappings ++ UnnestScope.rowExplosion(single)).flatMap {
          case (hit, mappings) =>
            val byField = new java.util.HashMap[String, JsonNode => Any]()
            mappings.foreach { case (subField, output) =>
              if (subField.indexOf('.') < 0 && !elements.contains(output))
                fields.find(_.outputName == output).foreach { f =>
                  val t = typeOf(f)
                  if (readers.typed(t))
                    byField.put(subField, readers.column(output, t, Set.empty)._1)
                }
            }
            if (byField.isEmpty) None else Some(hit -> byField)
        }
    val strict =
      schema.isDefined && !single.windowRowQuery && single.sqlAggregations.isEmpty &&
      GroupedRowOrder.of(single).isEmpty && !fields.exists(isStar)
    // `_id` when it is kept, and every inner hit the row explosion reads AFTER the scroll's
    // normalizer -- an UNNEST its WHERE filters per element included (`UnnestScope.rowExplosion`)
    val keep: Set[String] = Set(ElasticConversion.DocumentIdField) ++
      single.nestedHitsMappings.keySet ++ UnnestScope.rowExplosion(single).keySet
    // a flat dotted key folded under `*` (rare): its reader built once per path, on first use
    val flatCache = new java.util.concurrent.ConcurrentHashMap[String, JsonNode => Any]()
    val flatReaders: String => JsonNode => Any = path => {
      val cached = flatCache.get(path)
      if (cached ne null) cached
      else {
        val reader = single.schema.flatMap(_.find(path)) match {
          case Some(c) => readers.column(path, MultiSearch.columnType(c), Set.empty)._1
          case None    => readers.asRead
        }
        flatCache.putIfAbsent(path, reader)
        reader
      }
    }
    new HitReading(
      schema,
      source,
      scripted,
      inner,
      ignoredDates,
      strict,
      keep,
      star && schema.isDefined && fields.forall(isStar),
      flatReaders
    )
  }

  /** The readers of one statement (D5), built once: `JsonNode => Any`, a node never `null` (the
    * caller hands `null` / a JSON `null` to none of them but the scalar wrapper).
    */
  private[client] final class Readers(
    aliases: ListMap[String, String],
    dates: SetOperationValues.DateReading,
    multiValued: MultiValued,
    table: String
  ) {

    /** Today's reading, the plan's answer for a key it does not type (ANY, NUMERIC, TEMPORAL) and
      * for a value a typed reader cannot read (D6, `Unreadable.KeepAsStored`).
      */
    val asRead: JsonNode => Any = node => ElasticConversion.jsonNodeToAny(node, aliases)

    /** Today's reading with no temporal inference: a text is its own value, an array a list. */
    val textAsRead: JsonNode => Any = node =>
      if (node == null || node.isNull) null
      else if (node.isTextual) node.asText()
      else if (node.isArray) {
        val values = ListBuffer.empty[Any]
        val it = node.elements()
        while (it.hasNext) values += textAsRead(it.next())
        values.toList
      } else asRead(node)

    /** Whether a type is read by a typed reader: anything but a type read as it comes. */
    def typed(t: SetOperationType): Boolean =
      t.sqlType match {
        case SQLTypes.Any | SQLTypes.Numeric | SQLTypes.Temporal | SQLTypes.Null => false
        case SQLTypes.Struct => t.fields.nonEmpty
        case _               => true
      }

    /** A `_source` column of type `t`, and -- for a date read by its format -- the reader of its
      * cell when the hit's `_ignored` names it (D8): Elasticsearch refused some value of it, and
      * its verdict is per VALUE. A cell of one value is NULL (the value refused); an array keeps
      * each element the format reads -- a value Elasticsearch indexed -- and answers the others
      * NULL.
      */
    def column(
      name: String,
      t: SetOperationType,
      keepKeys: Set[String]
    ): (JsonNode => Any, Option[JsonNode => Any]) =
      t.sqlType match {
        case SQLTypes.Array(_) =>
          (list(name, t.element.getOrElse(SetOperationType(SQLTypes.Any))), None)
        case SQLTypes.Struct if t.fields.nonEmpty => (scalar(name, struct(name, t, keepKeys)), None)
        case SQLTypes.GeoPoint                    => (geo(name), None)
        case _ if !typed(t)                       => (asRead, None)
        case scalarType =>
          val (one, ignoredOne) = scalarOne(name, scalarType, t.format)
          (
            scalar(name, one),
            ignoredOne.map(element =>
              (node: JsonNode) =>
                if (node != null && node.isArray) scalarValue(name, element, node) else null
            )
          )
      }

    /** A computed column's `fields` entry: Elasticsearch's array unwrapped (#418 part 1). */
    def scripted(name: String, t: SetOperationType): JsonNode => Any =
      t.sqlType match {
        case SQLTypes.Array(_) => column(name, t, Set.empty)._1
        case _ if !typed(t)    => scalar(name, asRead)
        case _                 => column(name, t, Set.empty)._1
      }

    /** A dotted column's parent read for its selected paths only: each by its type, every other key
      * as read, in source order (D4).
      */
    def paths(name: String, under: List[(String, SetOperationType)]): JsonNode => Any = {
      val leaves = new java.util.HashMap[String, JsonNode => Any]()
      val nested =
        scala.collection.mutable.LinkedHashMap.empty[String, ListBuffer[(String, SetOperationType)]]
      under.foreach { case (p, t) =>
        val dot = p.indexOf('.')
        if (dot < 0) leaves.put(p, column(s"$name.$p", t, Set.empty)._1)
        else
          nested.getOrElseUpdate(p.substring(0, dot), ListBuffer.empty) += (p.substring(
            dot + 1
          ) -> t)
        // the flat spelling of a deeper path under this parent (`obj: {"x.y": 10}`)
        if (dot >= 0 && !leaves.containsKey(p)) leaves.put(p, column(s"$name.$p", t, Set.empty)._1)
      }
      nested.foreach { case (k, sub) =>
        if (!leaves.containsKey(k)) leaves.put(k, paths(s"$name.$k", sub.toList))
      }
      node =>
        if (node.isObject) {
          val entries = ListMap.newBuilder[String, Any]
          val it = node.properties().iterator()
          while (it.hasNext) {
            val e = it.next()
            val reader = leaves.get(e.getKey)
            entries += aliases.getOrElse(e.getKey, e.getKey) -> (
              if (reader ne null) reader(e.getValue) else asRead(e.getValue)
            )
          }
          entries.result()
        } else asRead(node)
    }

    /** ONE value of a scalar column: `[v]` is `v` -- `[[v]]` too --, `[]` NULL; several values per
      * [[MultiValued]]. An array of one holding several values (`[[a, b]]`) is as stored (D6): its
      * element is no value of the column, and unwrapping it would drop the nesting it was stored
      * with.
      */
    def scalar(name: String, one: JsonNode => Any): JsonNode => Any =
      node => scalarValue(name, one, node)

    private def scalarValue(name: String, one: JsonNode => Any, node: JsonNode): Any =
      if (node == null || node.isNull) null
      else if (node.isArray) {
        val size = node.size()
        if (size == 0) null
        else if (size == 1) {
          val only = node.get(0)
          if (only != null && only.isArray) {
            if (only.size() == 1) scalarValue(name, one, only)
            else if (only.size() > 1 && multiValued == MultiValued.Fail)
              throw new MultiValuedCell(name, table)
            else asRead(node)
          } else element(one)(only)
        } else
          multiValued match {
            case MultiValued.AsRead =>
              val values = ListBuffer.empty[Any]
              val it = node.elements()
              while (it.hasNext) values += element(one)(it.next())
              values.toList
            case MultiValued.Fail => throw new MultiValuedCell(name, table)
          }
      } else one(node)

    private def element(one: JsonNode => Any)(node: JsonNode): Any =
      if (node == null || node.isNull) null else one(node)

    /** An `ARRAY`: a `List` of its element reads, a scalar a list of one. */
    def list(name: String, element: SetOperationType): JsonNode => Any = {
      val one: JsonNode => Any = element.sqlType match {
        case SQLTypes.Struct if element.fields.nonEmpty => struct(name, element, Set.empty)
        case SQLTypes.GeoPoint                          => geoOne(name)
        case SQLTypes.Array(_) =>
          list(name, element.element.getOrElse(SetOperationType(SQLTypes.Any)))
        case _ if !typed(element) => asRead
        case scalarType           => scalarOne(name, scalarType, element.format)._1
      }
      node =>
        if (node == null || node.isNull) null
        else if (node.isArray) {
          val values = ListBuffer.empty[Any]
          val it = node.elements()
          while (it.hasNext) {
            val e = it.next()
            values += (if (e == null || e.isNull) null else one(e))
          }
          values.toList
        } else List(one(node))
    }

    /** A `STRUCT` with declared fields: those fields by name, each by its type, in source order; an
      * undeclared key dropped unless it spells a selected path (`keepKeys`), then as read.
      */
    def struct(name: String, t: SetOperationType, keepKeys: Set[String]): JsonNode => Any = {
      val fieldReaders = new java.util.HashMap[String, JsonNode => Any]()
      t.fields.foreach { f =>
        val under = keepKeys.collect {
          case k if k.startsWith(f.name + ".") => k.substring(f.name.length + 1)
        }
        fieldReaders.put(f.name, column(s"$name.${f.name}", f.valueType, under)._1)
      }
      node =>
        if (node.isObject) {
          val entries = ListMap.newBuilder[String, Any]
          val it = node.properties().iterator()
          while (it.hasNext) {
            val e = it.next()
            val reader = fieldReaders.get(e.getKey)
            if (reader ne null) entries += e.getKey -> reader(e.getValue)
            else if (keepKeys.contains(e.getKey)) entries += e.getKey -> asRead(e.getValue)
          }
          entries.result()
        } else asRead(node)
    }

    /** A `GEO_POINT` column: `{lat, lon}` (`SetOperationValues.geoPoint`) on a temporal-free value;
      * `[lon, lat]` is ONE point, several points per [[MultiValued]]; a value that is not a point
      * as read (D6).
      */
    def geo(name: String): JsonNode => Any = {
      val one = geoOne(name)
      node =>
        if (node == null || node.isNull) null
        else if (node.isArray && !coordinates(node)) {
          val size = node.size()
          if (size == 0) null
          else if (size == 1) element(one)(node.get(0))
          else
            multiValued match {
              case MultiValued.AsRead =>
                try {
                  val values = ListBuffer.empty[Any]
                  val it = node.elements()
                  while (it.hasNext) {
                    val e = it.next()
                    values += (if (e == null || e.isNull) null else point(name, e))
                  }
                  values.toList
                } catch { case _: IllegalArgumentException => asRead(node) }
              case MultiValued.Fail => throw new MultiValuedCell(name, table)
            }
        } else one(node)
    }

    private def geoOne(name: String): JsonNode => Any =
      node =>
        try point(name, node)
        catch { case _: IllegalArgumentException => asRead(node) }

    private def point(name: String, node: JsonNode): Any =
      SetOperationValues.geoPoint(HitReading.plain(node), name)

    /** `[lon, lat]` or `[lon, lat, z]`: one point, not a list (R-34). */
    private def coordinates(node: JsonNode): Boolean = {
      val size = node.size()
      (size == 2 || size == 3) && {
        var all = true
        val it = node.elements()
        while (all && it.hasNext) all = it.next().isNumber
        all
      }
    }

    /** ONE non-null, non-array value of a scalar type, and -- for a date read by its format -- the
      * same read answering NULL where the format does not read the value (D8).
      */
    def scalarOne(
      name: String,
      sqlType: app.softnetwork.elastic.sql.`type`.SQLType,
      format: Option[String]
    ): (JsonNode => Any, Option[JsonNode => Any]) =
      sqlType match {
        case SQLTypes.TinyInt | SQLTypes.SmallInt | SQLTypes.Int =>
          val range = sqlType match {
            case SQLTypes.TinyInt  => WholeRange.ForTinyInt
            case SQLTypes.SmallInt => WholeRange.ForSmallInt
            case _                 => WholeRange.ForInt
          }
          val min = range.min
          val max = range.max
          (
            node =>
              if (node.isInt) {
                val v = node.intValue()
                if (v >= min && v <= max) Integer.valueOf(v) else asRead(node)
              } else
                range.read(node) match {
                  case i: java.lang.Integer => i
                  case null                 => null
                  case _                    => asRead(node)
                },
            None
          )
        case SQLTypes.BigInt =>
          val range = WholeRange.ForBigInt
          (
            node =>
              if (node.isInt || node.isLong) java.lang.Long.valueOf(node.longValue())
              else
                range.read(node) match {
                  case l: java.lang.Long => l
                  case null              => null
                  case _                 => asRead(node)
                },
            None
          )
        case SQLTypes.Double =>
          (
            node =>
              if (node.isNumber) java.lang.Double.valueOf(node.doubleValue())
              else if (node.isTextual)
                SetOperationValues.double(node.asText()) match {
                  case d: java.lang.Double => d
                  case null                => null
                  case _                   => asRead(node)
                }
              else asRead(node),
            None
          )
        case SQLTypes.Real =>
          (
            node =>
              if (node.isNumber) java.lang.Float.valueOf(node.floatValue())
              else if (node.isTextual)
                SetOperationValues.real(node.asText()) match {
                  case f: java.lang.Float => f
                  case null               => null
                  case _                  => asRead(node)
                }
              else asRead(node),
            None
          )
        case SQLTypes.Boolean =>
          (
            node =>
              if (node.isBoolean) java.lang.Boolean.valueOf(node.booleanValue())
              else if (node.isTextual)
                SetOperationValues.boolean(node.asText()) match {
                  case b: java.lang.Boolean => b
                  case _                    => asRead(node)
                }
              else asRead(node),
            None
          )
        case SQLTypes.Varchar | SQLTypes.Keyword | SQLTypes.Text | SQLTypes.Char =>
          (node => if (node.isValueNode) node.asText() else asRead(node), None)
        case SQLTypes.VarBinary =>
          (
            node =>
              if (node.isTextual)
                try java.util.Base64.getDecoder.decode(node.asText())
                catch { case _: IllegalArgumentException => asRead(node) }
              else asRead(node),
            None
          )
        case SQLTypes.Date | SQLTypes.Timestamp | SQLTypes.Time | SQLTypes.DateTime =>
          val kind = if (sqlType == SQLTypes.DateTime) SQLTypes.Timestamp else sqlType
          format match {
            case Some(spec) =>
              DateRoute.of(kind, format, dates) match {
                case DateRoute.AsStored(_) =>
                  (node => if (node.isValueNode) node.asText() else asRead(node), None)
                case DateRoute.Typed =>
                  val read = byFormat(kind, spec)
                  (
                    node => {
                      val value = read(node)
                      if (value != null) value else asRead(node)
                    },
                    Some(read)
                  )
              }
            case None => (computedDate(kind), None)
          }
        case _ => (asRead, None)
      }

    /** A date field read by its mapping format, as the Elasticsearch major indexed it (D5): a text
      * by `MappedDateFormat.read` -- Elasticsearch's default format through [[DefaultDates]], which
      * hands every shape it does not read to it --, a number by its digits; `null` for a value the
      * format does not read, which the column answers as read (D6) -- or NULL where the hit's
      * `_ignored` names it (D8).
      */
    private def byFormat(
      kind: app.softnetwork.elastic.sql.`type`.SQLType,
      spec: String
    ): JsonNode => Any = {
      // the statement's one format of `spec`, the one its route was decided on (`Typed`: the major
      // is known)
      val format = dates.format(spec).get
      val fast = spec == app.softnetwork.elastic.sql.query.TemporalLiterals.DefaultDateFormat
      val result: Instant => Any = kind match {
        case SQLTypes.Date => _.atZone(ZoneOffset.UTC).toLocalDate
        case SQLTypes.Time => _.atZone(ZoneOffset.UTC).toLocalTime
        case _             => _.atZone(ZoneOffset.UTC)
      }
      val fastResult: ZonedDateTime => Any = kind match {
        case SQLTypes.Date => _.toLocalDate
        case SQLTypes.Time => _.toLocalTime
        case _             => z => z
      }
      node => {
        val z = if (fast && node.isTextual) DefaultDates.read(node.asText()) else null
        if (z ne null) fastResult(z)
        else {
          val outcome =
            if (node.isTextual) format.read(node.asText())
            else if (node.isNumber) format.readValue(node.numberValue())
            else MappedDateFormat.Rejected
          outcome match {
            case MappedDateFormat.Read(instant) => result(instant)
            case _                              => null
          }
        }
      }
    }

    /** A computed date (no field, so no format): `SetOperationValues`' `timestamp` / `date` over
      * today's reading, a `TIMESTAMP` at UTC; a value they do not read as read.
      */
    private def computedDate(kind: app.softnetwork.elastic.sql.`type`.SQLType): JsonNode => Any =
      kind match {
        case SQLTypes.Date =>
          node =>
            SetOperationValues.date(asRead(node)) match {
              case d: LocalDate => d
              case _            => asRead(node)
            }
        case SQLTypes.Time =>
          node =>
            asRead(node) match {
              case t: LocalTime     => t
              case z: ZonedDateTime => z.withZoneSameInstant(ZoneOffset.UTC).toLocalTime
              case l: LocalDateTime => l.toLocalTime
              case other            => other
            }
        case _ =>
          node => {
            val read = asRead(node)
            SetOperationValues.timestamp(read) match {
              case z: ZonedDateTime =>
                if (z.getZone == ZoneOffset.UTC) z else z.withZoneSameInstant(ZoneOffset.UTC)
              case _ => read
            }
          }
      }
  }

  /** What [[WholeRange.read]] answers for a value that is no value of its type. */
  private object OutOfType

  /** SCHEMA-2 (D6) -- an integral type's values read as Elasticsearch reads a value into that type,
    * `coerce` on (T row 5), the same on 6.8.23, 7.17.29 and 8.18.3 (`NumberFieldMapper.value` /
    * `parse(XContentParser, coerce)`, `AbstractXContentParser`, Jackson's `getIntValue` /
    * `getShortValue` / `getLongValue`):
    *
    *   - an empty text is NULL (`textLength() == 0`);
    *   - `integer`, `short` and `byte` read a JSON number through `getIntValue` -- a whole number
    *     within `int`; a fractional one, as a `double`, within `int`, truncated toward zero --,
    *     then `short` and `byte` check their own range on that `int`; a text through
    *     `Double.parseDouble`, which trims and reads what it reads (`5f`, `NaN` as 0), `short`
    *     checking its range on that `double`, `integer` and `byte` the `int` range on it, then
    *     `byte` its own range on the truncated `int`;
    *   - `long` reads a JSON number through `getLongValue` -- a fractional one, as a `double`,
    *     within `long`, truncated --; a text through `Long.parseLong`, else `new BigDecimal`
    *     (neither trims), truncated toward zero and checked on the truncated value.
    *
    * A value Elasticsearch refuses is no value of the type -- it holds it in `_source` only
    * (`ignore_malformed`) -- so `read` answers [[OutOfType]] and the column keeps it as stored
    * (`Unreadable.KeepAsStored`): narrowing it would wrap it. So is a `long` text with more than 19
    * digits of scale either way where the majors disagree (7.17 and 8.18 refuse a scale below -19
    * and read 0 above 19; 6.8 reads the value). No digit count or scale is computed in `Int`
    * arithmetic, and no value is materialised beyond the digits its text holds.
    */
  private[client] sealed abstract class WholeRange {

    /** A `java.lang.Integer` (`TINYINT`, `SMALLINT`, `INT`) or a `java.lang.Long` (`BIGINT`),
      * `null` for an empty text, else [[OutOfType]].
      */
    def read(node: JsonNode): Any
  }

  private[client] object WholeRange {

    /** `byte`: `parser.intValue(coerce)`, then the byte range on that `int`. */
    val ForTinyInt: Ints =
      new Ints(
        Byte.MinValue.toInt,
        Byte.MaxValue.toInt,
        Int.MinValue.toDouble,
        Int.MaxValue.toDouble
      )

    /** `short`: a number through `getShortValue` (`getIntValue`, then the short range on that
      * `int`); a text checked on its `double`.
      */
    val ForSmallInt: Ints = new Ints(
      Short.MinValue.toInt,
      Short.MaxValue.toInt,
      Short.MinValue.toDouble,
      Short.MaxValue.toDouble
    )

    /** `integer`: the `int` range, on the `double` of a fractional number or a text. */
    val ForInt: Ints =
      new Ints(Int.MinValue, Int.MaxValue, Int.MinValue.toDouble, Int.MaxValue.toDouble)

    /** The `int`-family reader: values `[min, max]`; a text's `double` checked within `[textLow,
      * textHigh]` before it is truncated.
      */
    private[client] final class Ints(val min: Int, val max: Int, textLow: Double, textHigh: Double)
        extends WholeRange {

      def read(node: JsonNode): Any =
        if (node.isIntegralNumber) {
          if (node.canConvertToLong) {
            val v = node.longValue()
            if (v >= min && v <= max) java.lang.Integer.valueOf(v.toInt) else OutOfType
          } else OutOfType
        } else if (node.isNumber) truncated(node.doubleValue(), IntLow, IntHigh)
        else if (node.isTextual) {
          val text = node.asText()
          if (text.isEmpty) null
          else {
            val d =
              try java.lang.Double.valueOf(text)
              catch { case _: NumberFormatException => null }
            if (d eq null) OutOfType else truncated(d.doubleValue(), textLow, textHigh)
          }
        } else OutOfType

      /** `d` within `[low, high]`, truncated toward zero (`(int) d`: `NaN` is 0), then within the
        * type's range.
        */
      private def truncated(d: Double, low: Double, high: Double): Any =
        if (d < low || d > high) OutOfType
        else {
          val i = d.toInt
          if (i >= min && i <= max) java.lang.Integer.valueOf(i) else OutOfType
        }
    }

    // Jackson's MIN_INT_D / MAX_INT_D / MIN_LONG_D / MAX_LONG_D
    private val IntLow: Double = Int.MinValue.toDouble
    private val IntHigh: Double = Int.MaxValue.toDouble
    private val LongLow: Double = Long.MinValue.toDouble
    private val LongHigh: Double = Long.MaxValue.toDouble
    private val LongMin = java.math.BigDecimal.valueOf(Long.MinValue)
    private val LongMax = java.math.BigDecimal.valueOf(Long.MaxValue)

    /** `long`. */
    object ForBigInt extends WholeRange {

      def read(node: JsonNode): Any =
        if (node.isIntegralNumber) {
          if (node.canConvertToLong) java.lang.Long.valueOf(node.longValue()) else OutOfType
        } else if (node.isNumber) {
          val d = node.doubleValue()
          if (d < LongLow || d > LongHigh) OutOfType else java.lang.Long.valueOf(d.toLong)
        } else if (node.isTextual) text(node.asText())
        else OutOfType

      private def text(text: String): Any =
        if (text.isEmpty) null
        else {
          val whole =
            try java.lang.Long.valueOf(text)
            catch { case _: NumberFormatException => null }
          if (whole ne null) whole
          else {
            val d =
              try new java.math.BigDecimal(text)
              catch { case _: NumberFormatException => null }
            if (d eq null) OutOfType
            else {
              // the digits before the point, in Long arithmetic, read off the scale
              val integerDigits = d.precision().toLong - d.scale()
              if (d.scale() < -19) OutOfType
              else if (d.signum() == 0 || integerDigits <= 0) java.lang.Long.valueOf(0L)
              else if (d.scale() > 19 || integerDigits > 19) OutOfType
              else {
                val truncated = d.setScale(0, java.math.RoundingMode.DOWN)
                if (truncated.compareTo(LongMin) >= 0 && truncated.compareTo(LongMax) <= 0)
                  java.lang.Long.valueOf(truncated.longValue())
                else OutOfType
              }
            }
          }
        }
    }
  }

  /** A node as a plain value, with no temporal inference: what `geoPoint` reads. */
  private[client] def plain(node: JsonNode): Any =
    if (node == null || node.isNull) null
    else if (node.isTextual) node.asText()
    else if (node.isNumber) node.numberValue()
    else if (node.isBoolean) node.booleanValue()
    else if (node.isArray) {
      val values = ListBuffer.empty[Any]
      val it = node.elements()
      while (it.hasNext) values += plain(it.next())
      values.toList
    } else if (node.isObject) {
      val entries = ListMap.newBuilder[String, Any]
      val it = node.properties().iterator()
      while (it.hasNext) {
        val e = it.next()
        entries += e.getKey -> plain(e.getValue)
      }
      entries.result()
    } else node.asText()

  /** SCHEMA-2 (D5) -- Elasticsearch's default `strict_date_optional_time||epoch_millis` for the
    * shapes it is written in almost always -- `uuuu-MM-dd`, `uuuu-MM-ddTHH:mm:ss`, a fraction of 1
    * to 3 digits, an optional `Z` -- at UTC, by hand; `null` for any other shape, which the caller
    * reads with `MappedDateFormat.read` (R-5: no second date rule; `HitReadingSpec` proves the two
    * equal over the corpus).
    */
  private[client] object DefaultDates {

    private def digit(c: Char): Int = if (c >= '0' && c <= '9') c - '0' else -1

    private def number(s: String, from: Int, to: Int): Int = {
      var n = 0
      var i = from
      while (i < to) {
        val d = digit(s.charAt(i))
        if (d < 0) return -1
        n = n * 10 + d
        i += 1
      }
      n
    }

    def read(s: String): ZonedDateTime = {
      val len = s.length
      if (len < 10 || s.charAt(4) != '-' || s.charAt(7) != '-') return null
      val year = number(s, 0, 4)
      val month = number(s, 5, 7)
      val day = number(s, 8, 10)
      if (year < 0 || month < 1 || month > 12 || day < 1 || day > 31) return null
      var hour = 0
      var minute = 0
      var second = 0
      var nanos = 0
      var end = len
      if (len > 10) {
        if (s.charAt(len - 1) == 'Z') end = len - 1
        if (end < 19 || s.charAt(10) != 'T' || s.charAt(13) != ':' || s.charAt(16) != ':')
          return null
        hour = number(s, 11, 13)
        minute = number(s, 14, 16)
        second = number(s, 17, 19)
        if (hour < 0 || hour > 23 || minute < 0 || minute > 59 || second < 0 || second > 59)
          return null
        if (end > 19) {
          val digits = end - 20
          if (s.charAt(19) != '.' || digits < 1 || digits > 3) return null
          val fraction = number(s, 20, end)
          if (fraction < 0) return null
          nanos =
            fraction * (if (digits == 1) 100000000 else if (digits == 2) 10000000 else 1000000)
        }
      }
      try ZonedDateTime.of(year, month, day, hour, minute, second, nanos, ZoneOffset.UTC)
      catch { case _: java.time.DateTimeException => null }
    }
  }
}
