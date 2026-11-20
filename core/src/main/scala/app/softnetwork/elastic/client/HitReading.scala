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
  Bucket,
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
  *   - `ignoredReaders`: the top-level `_source` keys of a kind Elasticsearch may ignore a value of
  *     (`ignore_malformed`) -- a date read by its format, and every kind of
  *     [[HitReading.IgnoredKinds]] (SCHEMA-17 fix R-172) --, each with the reader of its cell where
  *     the hit's `_ignored` names it (D8): Elasticsearch's verdict is per VALUE, so a cell of one
  *     value is NULL and an array keeps the elements the kind's own parse accepts, the others NULL;
  *   - `strict` / `keep`: the projection drops every undeclared key, but `keep` (D9);
  *   - `star`: a bare `SELECT *` -- unmapped top-level keys dropped, a flat dotted key folded into
  *     its mapped parent (A-3);
  *   - `instants`: SCHEMA-2b -- on the base side of a window statement, each partition column
  *     paired by instant (`PartitionKeys.instantColumns`): its hit `fields` entry (the
  *     `epoch_millis` doc value the base query asks) by the row key it is read under
  *     (`PartitionKeys.instantKey`), beside the column's stored `_source` value.
  *
  * A key the plan does not hold is read as it always was (`ElasticConversion.jsonNodeToAny`). Built
  * ONCE per statement: per row, only the readers run.
  */
private[client] final class HitReading(
  val schema: Option[ResultSchema],
  private[client] val source: java.util.HashMap[String, JsonNode => Any],
  private[client] val scripted: java.util.HashMap[String, JsonNode => Any],
  private[client] val inner: Map[String, java.util.HashMap[String, JsonNode => Any]],
  private[client] val ignoredReaders: java.util.HashMap[String, JsonNode => Any],
  val strict: Boolean,
  val keep: Set[String],
  private[client] val star: Boolean,
  private[client] val flatReaders: String => JsonNode => Any,
  private[client] val instants: java.util.HashMap[String, String],
  private[client] val listPaths: java.util.HashMap[String, Array[HitReading.ListPath]] =
    new java.util.HashMap[String, Array[HitReading.ListPath]](),
  private[client] val asReadKeys: java.util.HashMap[String, String] =
    new java.util.HashMap[String, String]()
) {

  /** Whether a hit's `_ignored` must be read (D8): the plan reads some top-level `_source` key of a
    * kind Elasticsearch may ignore a value of. One lookup per hit, which allocates nothing when the
    * hit has no `_ignored` (`ElasticConversion.ignoredOf`).
    */
  private[client] val readsIgnored: Boolean = !ignoredReaders.isEmpty

  /** Whether some selected path is collected whatever its parent's value is (SCHEMA-17 A-181,
    * [[HitReading.ListPath.always]]): decided once per statement, so a row of a statement with none
    * keeps reading only its list values.
    */
  private[client] val collectsAlways: Boolean = {
    var found = false
    val it = listPaths.values().iterator()
    while (!found && it.hasNext) found = it.next().exists(_.always)
    found
  }

  /** The reader of `key`'s cell when the hit's `_ignored` names it (D8): THE lookup (#292) of every
    * hit-reading path (search hits, scroll / PIT / search_after pages, `*`), built with the plan.
    * `null` for a key of no such kind, which keeps its own reader: a keyword or a wildcard over
    * `ignore_above` keeps its stored text, an object its own reader (a top-level key only).
    */
  private[client] def onIgnored(key: String): JsonNode => Any = ignoredReaders.get(key)

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

  /** SCHEMA-6 (D6, lead Q1): several values in a column not declared `ARRAY<X>` fail the statement,
    * naming the `ALTER` that declares it a list -- the remedy ships in the same release.
    */
  private[client] val Default: MultiValued = MultiValued.Fail

  /** SCHEMA-7 (A-41) -- what a several-element cell of a scalar column answers in `single`: as read
    * (a `List` of its elements, each read by the column's type) when its schema was typed from
    * `_field_caps` on a cluster whose fields take no `meta` -- Elasticsearch 6.8, or a version not
    * known (R-55, kept by the lead's LEAD-Q2 (a) of 2026-11-16: the one value-typed residue) --,
    * else [[Default]]. Read at every argument [[Default]] was given.
    *
    * SCHEMA-17 (A-173): from 7.6 a field core declared carries its declaration in its mapping
    * `meta`, which `_field_caps` returns -- a column declared `ARRAY<X>` reads `ARRAY<X>` there --,
    * and an undeclared several-valued cell fails as a mapping reader's does, naming the `ALTER`: a
    * user who may alter the table runs it, and the read-only user then sees the field declared.
    * `version` is the cluster's (`VersionApi`), read by `ElasticsearchVersion.supportsFieldMeta`
    * -- asked only for a schema typed from `_field_caps` (by name: every other statement never asks
    * it here).
    *
    * SCHEMA-17 fix R-150 (A-183, A-184): this is the STATEMENT's rule, which objects keep; a LEAF
    * no index declares to a read-only user ([[storedLeaves]]) reads its own several values as
    * stored where the statement's rule fails them (`Readers.leafRule`).
    */
  private[client] def multiValuedFor(
    single: SingleSearch,
    version: => Option[String]
  ): MultiValued =
    if (
      single.schema.exists(
        _.tableType == app.softnetwork.elastic.sql.schema.TableType.FieldCapabilities
      ) && !version.exists(ElasticsearchVersion.supportsFieldMeta)
    )
      MultiValued.AsRead
    else Default

  /** SCHEMA-17 fix R-150 (A-184) -- the LEAVES of `schema` whose own several values are read as
    * stored, as Elasticsearch 6.8's read-only users and 0.24.0 read them: those of a table typed
    * from `_field_caps` that no index declares (`schema.FieldCaps.undeclared`, the one answer);
    * none for any other table. Read once per plan, by [[Readers]].
    */
  private[client] def storedLeaves(
    schema: Option[app.softnetwork.elastic.sql.schema.Table]
  ): Set[String] =
    schema.fold(Set.empty[String])(app.softnetwork.elastic.schema.FieldCaps.undeclared)

  /** SCHEMA-7 (D3) -- whether a field's dates are read to the nanosecond, as Elasticsearch indexes
    * them: `Column.dateNanos`, the one `date_nanos` predicate (A-72, #292), which the date format
    * and a day shift read too. Every reader of a date field reads it: the hit reader
    * ([[nanosPaths]]), the set operations' (`SetOperationValues`), an UNNEST element's conversion
    * (`ElementConversions`).
    */
  private[client] def readsNanos(column: Column): Boolean = column.dateNanos

  /** SCHEMA-7 (D3) -- the paths of `columns` (at every depth) whose values are read to the
    * nanosecond ([[readsNanos]]).
    */
  private[client] def nanosPaths(columns: Seq[Column], prefix: String = ""): Set[String] =
    columns.flatMap { c =>
      val path = s"$prefix${c.name}"
      (if (readsNanos(c)) Set(path) else Set.empty[String]) ++
      nanosPaths(c.multiFields, s"$path.")
    }.toSet

  /** SCHEMA-17 fix R-172 (A-201) -- the Elasticsearch kinds, beside the dates (read by their
    * format), that take `ignore_malformed`: a value of them the hit's `_ignored` names was refused
    * by Elasticsearch, which holds it in `_source` only. `boolean` takes it only on the majors that
    * map the option (not 6.8 nor 7.x): elsewhere `_ignored` never names it. A refused
    * `scaled_float` is named only from 8.6: before, its value keeps its reading as stored, as any
    * value `_ignored` does not name. The strings (`keyword`, `wildcard`, ...) are not here:
    * `_ignored` names them over `ignore_above`, a value Elasticsearch keeps.
    */
  private[client] val IgnoredKinds: Set[String] = Set(
    "byte",
    "short",
    "integer",
    "long",
    "unsigned_long",
    "float",
    "half_float",
    "double",
    "scaled_float",
    "ip",
    "geo_point",
    "boolean"
  )

  /** SCHEMA-17 fix R-172 (A-201) -- each top-level column of `columns` of a kind of
    * [[IgnoredKinds]], with that kind: its `Column.mappingType` (#292: a table typed from its
    * mappings and one typed from `_field_caps` both carry the field's own type). Built once per
    * plan, so a reader knows its kind when it is built, never per row.
    */
  private[client] def ignoredKinds(columns: Seq[Column]): Map[String, String] =
    columns.flatMap { c =>
      val kind = c.mappingType
      if (IgnoredKinds.contains(kind)) Some(c.name -> kind) else None
    }.toMap

  /** The smallest magnitude a `half_float` refuses: Lucene's `HalfFloatPoint` rounds it (ties to
    * even) to infinity, which Elasticsearch refuses (the largest half float is 65504).
    */
  private val HalfFloatLimit: Float = 65520f

  /** SCHEMA-17 fix R-172 (A-201) -- whether `text` may be an address an `ip` field indexes: an IPv4
    * literal (four decimal parts of at most three digits, each at most 255), or a text holding a
    * `:` made only of hex digits, `:` and `.` -- an IPv6 literal, an embedded IPv4 one included --,
    * optionally followed by `%` and a zone. Written by hand, never `InetAddress.getByName`, which
    * resolves host names; lenient where Elasticsearch's own parse is stricter (a leading zero, an
    * IPv6 group count), so an address Elasticsearch indexed is never NULL.
    */
  private[client] def ipLiteral(text: String): Boolean =
    if (text.indexOf(':') < 0) ipv4(text)
    else {
      val zone = text.indexOf('%')
      val end = if (zone < 0) text.length else zone
      if (end == 0 || zone == text.length - 1) false
      else {
        var ok = true
        var i = 0
        while (ok && i < end) {
          val c = text.charAt(i)
          ok = c == ':' || c == '.' || Character.digit(c, 16) >= 0
          i += 1
        }
        ok
      }
    }

  private def ipv4(text: String): Boolean = {
    var parts = 0
    var digits = 0
    var value = 0
    var ok = true
    var i = 0
    while (ok && i < text.length) {
      val c = text.charAt(i)
      if (c == '.') {
        ok = digits > 0
        parts += 1
        digits = 0
        value = 0
      } else if (c >= '0' && c <= '9') {
        digits += 1
        value = value * 10 + (c - '0')
        ok = digits <= 3 && value <= 255
      } else ok = false
      i += 1
    }
    ok && digits > 0 && parts == 3
  }

  /** SCHEMA-6 (A-26) -- a selected dotted column under a parent the plan reads WHOLE (`SELECT obj,
    * obj.a`, `SELECT *, obj.a`): where the parent's `_source` value is a list of objects, the row
    * key `key` holds the values the path `segments` collects there ([[Readers.collected]]), read by
    * `reader`, the path's own reader -- as the parent read for its paths only answers them.
    *
    * SCHEMA-17 (A-181): `always` for a path under a list of objects (`MultiSearch.listedPath`),
    * typed a list: collected whatever the parent's value is -- one object holds a list of one
    * value, as the parent declared a list reads it a list of one object. Also `always` for a path
    * under a list of objects whose prefix is itself a selected path under a parent NOT read whole
    * (`obj.in.q` beside `obj.in`): the parent's reader reads the prefix, a list, and never the
    * path.
    *
    * `points`: the path is a `GEO_POINT` leaf ([[readsPoints]]), collected point by point.
    *
    * SCHEMA-17 fix R-150 (A-183): `asStored`, set for a leaf a read-only user reads as stored
    * (`Readers.collectedReaders`), where `reader` reads it under the statement's rule.
    */
  private[client] final class ListPath(
    val key: String,
    val segments: Array[String],
    val reader: JsonNode => Any,
    val always: Boolean = false,
    val points: Boolean = false,
    val asStored: JsonNode => Any = null
  ) {

    /** The values `values` the path collects under its parent's value `parent`, by [[reader]];
      * where it fails a several-valued cell and [[asStored]] is set, the CAUSE picks the rule
      * (A-183): one object holding them reads them as stored, several fail as before.
      */
    private[client] def read(parent: JsonNode, values: JsonNode): Any =
      if (asStored eq null) reader(values)
      else
        try reader(values)
        catch {
          case cell: MultiValuedCell =>
            if (severalHold(parent, segments)) throw cell else asStored(values)
        }
  }

  /** SCHEMA-6 (A-26) -- the values the sub-path `segments` holds under `node` (a list of objects),
    * in order, every list met on the way -- an element's own list included -- flattened, as
    * Elasticsearch indexes a path under a list of objects; `null` when it holds none.
    *
    * SCHEMA-17 (A-181): `points` for a `GEO_POINT` leaf ([[readsPoints]]): a coordinates array met
    * at the leaf (`[lon, lat]` or `[lon, lat, z]`, [[coordinates]]) is ONE value, never flattened
    * into its numbers; a list of them (one object holding several points) is one value each.
    */
  private[client] def collected(
    node: JsonNode,
    segments: Array[String],
    points: Boolean = false
  ): JsonNode = {
    val out = com.fasterxml.jackson.databind.node.JsonNodeFactory.instance.arrayNode()
    def walk(n: JsonNode, i: Int): Unit =
      if (n != null && !n.isNull) {
        if (n.isArray) {
          if (points && i == segments.length && coordinates(n)) out.add(n)
          else {
            val it = n.elements()
            while (it.hasNext) walk(it.next(), i)
          }
        } else if (i == segments.length) out.add(n)
        else if (n.isObject) walk(n.get(segments(i)), i + 1)
      }
    walk(node, 0)
    if (out.size() == 0) null else out
  }

  /** `[lon, lat]` or `[lon, lat, z]`: one point, not a list (R-34). The one test of the readers'
    * `geo` and `list` and of [[collected]].
    */
  private[client] def coordinates(node: JsonNode): Boolean = {
    val size = node.size()
    (size == 2 || size == 3) && {
      var all = true
      val it = node.elements()
      while (all && it.hasNext) all = it.next().isNumber
      all
    }
  }

  /** SCHEMA-17 (A-181) -- whether a path of type `t` holds points: `GEO_POINT`, or a list of them
    * (at any depth), whose `[lon, lat]` values [[collected]] keeps whole.
    */
  private[client] def readsPoints(t: SetOperationType): Boolean =
    t.sqlType match {
      case SQLTypes.GeoPoint => true
      case SQLTypes.Array(_) => t.element.exists(readsPoints)
      case _                 => false
    }

  /** SCHEMA-17 (A-181) -- whether several objects of the list of objects `node` hold a value at the
    * sub-path `segments` (every list met flattened, as [[collected]] reads them): the values of a
    * several-valued cell of that path then come from the LIST, not from one object's own field.
    * Read on the error path only.
    */
  private[client] def severalHold(node: JsonNode, segments: Array[String]): Boolean = {
    var holders = 0
    def walk(n: JsonNode): Unit =
      if (holders < 2 && n != null && !n.isNull) {
        if (n.isArray) {
          val it = n.elements()
          while (holders < 2 && it.hasNext) walk(it.next())
        } else if (collected(n, segments) ne null) holders += 1
      }
    walk(node)
    holders > 1
  }

  /** A bare `*` item. */
  private[client] def isStar(field: Field): Boolean =
    field.identifier.identifierName == "*" && field.identifier.functions.isEmpty

  /** A computed SELECT item: read from the hit's `fields` by its output name (`scripted`). */
  private[client] def isComputed(field: Field): Boolean =
    field.identifier.functions.nonEmpty || field.isScriptField

  /** Thrown by a scalar reader under [[MultiValued.Fail]]; the hit loop names the document and its
    * index. `alter` is the `ALTER COLUMN …` clause that declares the column a list ([[remedyOf]]),
    * empty for a name no schema column has (a computed item); `table` the statement's source, the
    * index named when the hit names none.
    */
  private[client] final class MultiValuedCell(
    val column: String,
    val alter: String,
    val table: String
  ) extends RuntimeException(column, null, false, false)

  /** The message a several-element cell fails with under [[MultiValued.Fail]] (SCHEMA-6 D6): the
    * column, the document `_id` and the index the hit came from (`_index`, else the statement's
    * source), and the `ALTER TABLE` statement -- one that parses and runs -- declaring it a list.
    */
  private[client] def multiValuedMessage(
    cell: MultiValuedCell,
    id: Option[String],
    index: Option[String]
  ): String = {
    val in = index.filter(_.nonEmpty).getOrElse(cell.table)
    val where =
      id.fold("")(i => s" in document $i") + (if (in.nonEmpty) s" of index $in" else "")
    val remedy =
      if (cell.alter.isEmpty || in.isEmpty) ""
      else
        s": declare it a list with ALTER TABLE ${app.softnetwork.elastic.sql.renderColumnName(in)} " +
        cell.alter
    s"Column ${cell.column} holds several values$where, but is declared as one value$remedy"
  }

  /** SCHEMA-17 (LEAD-Q1 b) -- the formats a `_field_caps` date is read by when its mapping `meta`
    * names none (A-40): ISO 8601 only, the one spelling read as an instant without knowing the
    * field's own format.
    */
  private val IsoOnlyFormats: Set[String] = Set(
    app.softnetwork.elastic.sql.query.TemporalLiterals.FieldCapsDateFormat,
    app.softnetwork.elastic.sql.query.TemporalLiterals.FieldCapsDateNanosFormat
  )

  /** Whether `single`'s dates of no declared format are read as ISO 8601 only, failing any other
    * spelling (SCHEMA-17, the lead's LEAD-Q1 (b) of 2026-11-16): its schema was typed from
    * `_field_caps`, which has no `format`.
    */
  private[client] def fieldCapsDates(single: SingleSearch): Boolean =
    single.schema.exists(
      _.tableType == app.softnetwork.elastic.sql.schema.TableType.FieldCapabilities
    )

  /** Thrown by a date reader of a `_field_caps` schema ([[fieldCapsDates]]) for a value that is not
    * ISO 8601; the hit loop names the document and its index ([[unreadableDateMessage]]).
    */
  private[client] final class UnreadableDateCell(
    val column: String,
    val value: String,
    val table: String
  ) extends RuntimeException(column, null, false, false)

  /** The message an [[UnreadableDateCell]] fails with: the column, the value, the document and its
    * index, and the remedies -- a grant to read the mapping, which runs on every version and over
    * several indices (accepted by the lead by name), or, on Elasticsearch 7.6 or later and one
    * index, an in-place `ALTER TABLE` by the table's owner, which writes the field's declaration
    * into its mapping `meta`.
    */
  private[client] def unreadableDateMessage(
    cell: UnreadableDateCell,
    id: Option[String],
    index: Option[String]
  ): String = {
    val in = index.filter(_.nonEmpty).getOrElse(cell.table)
    val where =
      id.fold("")(i => s" in document $i") + (if (in.nonEmpty) s" of index $in" else "")
    s"Column ${cell.column} holds ${cell.value}$where, which is not an ISO 8601 date: its mapping " +
    "format cannot be read by a user who may not read the index's mapping. Grant this user the " +
    "privilege to read the mapping (view_index_metadata), or, on Elasticsearch 7.6 or later, have " +
    "the table's owner alter the table in place, which declares the field's format to every user"
  }

  /** The `ALTER COLUMN …` clause declaring the column at `path` of `schema` a list of its own type
    * (SCHEMA-6 D6), read on the error path only: `ALTER COLUMN c SET DATA TYPE ARRAY<X>` for a
    * column, X its declared type; for a sub-field `p.f`, `ALTER COLUMN p SET FIELD <f>`, `<f>` the
    * sub-field as its schema declares it -- options, DEFAULT, COMMENT, NOT NULL and multi-fields
    * kept, `SET FIELD` replacing the whole sub-field -- with the type `ARRAY<X>`. `None` for a name
    * the schema does not have.
    */
  private[client] def remedyOf(
    schema: Option[app.softnetwork.elastic.sql.schema.Table]
  ): String => Option[String] =
    path =>
      schema.flatMap(_.find(path)).map { c =>
        val list = c.dataType match {
          case declared: app.softnetwork.elastic.sql.`type`.SQLArray => declared
          case declared                                              => SQLTypes.Array(declared)
        }
        val dot = path.lastIndexOf('.')
        if (dot < 0)
          s"ALTER COLUMN ${app.softnetwork.elastic.sql.renderColumnName(path)} SET DATA TYPE " +
          list.typeId
        else {
          val parent = path
            .substring(0, dot)
            .split("\\.")
            .map(app.softnetwork.elastic.sql.renderColumnName)
            .mkString(".")
          // SCHEMA-17 (A-166): the list keeps the field's Elasticsearch type -- `type = 'object'`
          // for a STRUCT, which `ARRAY<STRUCT>` alone would make `nested` -- so the sub-field is
          // declared a list in place, as `SET DATA TYPE` declares a column
          val kept =
            if (
              !c.options.contains("type") &&
              app.softnetwork.elastic.sql.`type`.SQLTypeUtils.elasticType(list) != c.mappingType
            )
              c.options + ("type" -> (app.softnetwork.elastic.sql.StringValue(
                c.mappingType
              ): app.softnetwork.elastic.sql.Value[_]))
            else c.options
          val field = c.copy(dataType = list, struct = None, options = kept).update()
          s"ALTER COLUMN $parent SET FIELD ${field.sql.replaceAll("\\n\\t*", " ")}"
        }
      }

  /** A plan that reads nothing and projects STRICTLY, keeping `keep` (D9): the final projection of
    * a window-enriched statement (SCHEMA-2b, B7 d), whose rows were read by their base query's
    * plan.
    */
  private[client] def projection(keep: Set[String]): HitReading =
    new HitReading(
      None,
      new java.util.HashMap[String, JsonNode => Any](),
      new java.util.HashMap[String, JsonNode => Any](),
      Map.empty,
      new java.util.HashMap[String, JsonNode => Any](),
      strict = true,
      keep,
      star = false,
      _ => null,
      new java.util.HashMap[String, String]()
    )

  /** @param carried
    *   SCHEMA-2b (B7 a) -- the PARTITION BY levels of the window-enriched statement whose base
    *   query `single` is: each plain column of them is read by its type -- a `DATE` / `TIME` one
    *   only where it is exactly a day / a time of day ([[Readers.exact]], A-23) -- and kept by the
    *   strict projection, so the base row carries the partition key the window values are joined
    *   on.
    * @param instants
    *   the carried columns paired by instant (`PartitionKeys.instantColumns`, the statement's one
    *   decision, taken as it is): each one's hit `fields` entry -- its `epoch_millis` doc value --
    *   read under `PartitionKeys.instantKey` and kept by the strict projection; its `_source` value
    *   stays as stored.
    */
  def of(
    single: SingleSearch,
    dates: SetOperationValues.DateReading,
    multiValued: MultiValued = Default,
    elements: Set[String] = Set.empty,
    carried: Seq[Bucket] = Nil,
    instants: Set[String] = Set.empty
  ): HitReading = {
    // each item's and each mapped column's type, computed ONCE per statement: the schema and the
    // readers read the same value; so do the date formats and routes, built once by `dates`
    val itemTypes = new java.util.IdentityHashMap[Field, SetOperationType]()
    val typeOf: Field => SetOperationType = f => {
      val known = itemTypes.get(f)
      if (known ne null) known
      else {
        val t = MultiSearch.fieldType(f, single)
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
    val readers = new Readers(
      aliases,
      dates,
      multiValued,
      table,
      remedyOf(single.schema),
      single.schema.fold(Set.empty[String])(t => nanosPaths(t.columns)),
      fieldCapsDates(single),
      storedLeaves(single.schema),
      single.schema.fold(Map.empty[String, String])(t => ignoredKinds(t.columns))
    )
    val fields = single.select.fieldsWithComputedAliases
    val star = fields.exists(isStar)
    // SCHEMA-7 (D5): the columns read from their doc value, never from `_source`
    val indexed = single.indexedColumns.toSet
    val source = new java.util.HashMap[String, JsonNode => Any]()
    val scripted = new java.util.HashMap[String, JsonNode => Any]()
    val ignoredReaders = new java.util.HashMap[String, JsonNode => Any]()
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
      onIgnored.foreach(ignoredReaders.put(key, _))
    }
    single.schema.foreach { table =>
      if (star)
        table.columns.foreach { c =>
          if (!source.containsKey(c.name) && !indexed.contains(c.name))
            put(c.name, columnTypeOf(c), keepUnder(c.name))
        }
      // SCHEMA-7 (D5): a column read from its doc value, in the hit's `fields`, by its type
      table.columns.foreach { c =>
        if (indexed.contains(c.name) && !scripted.containsKey(c.name))
          scripted.put(c.name, readers.column(c.name, columnTypeOf(c), Set.empty)._1)
      }
    }
    // explicit columns: a bare column (or a dotted column's parent and its flat spelling) by its
    // type; a computed column's `fields` entry by output name
    val paths =
      scala.collection.mutable.LinkedHashMap.empty[String, ListBuffer[(String, SetOperationType)]]
    // SCHEMA-17 (A-181): the selected paths under a list of objects, typed a list
    val listedPaths = scala.collection.mutable.Set.empty[String]
    fields.filterNot(isStar).foreach { f =>
      val id = f.identifier
      if (isComputed(f)) {
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
          if (!source.containsKey(name) && !indexed.contains(name)) put(name, t, keepUnder(name))
        } else {
          val parent = name.substring(0, dot)
          paths.getOrElseUpdate(parent, ListBuffer.empty) += (name.substring(dot + 1) -> t)
          if (MultiSearch.listedPath(id)) listedPaths += name
          // SCHEMA-12 A-3's flat spelling, read first by `flatValue`
          if (!source.containsKey(name)) {
            val (reader, _) = readers.column(name, t, Set.empty)
            source.put(name, reader)
          }
        }
      }
    }
    // a dotted column's parent not read whole: its selected paths by their types, every other key
    // as read; a parent read whole: its selected paths collected where it holds a list of objects
    // (SCHEMA-6 A-26)
    val listPaths = new java.util.HashMap[String, Array[ListPath]]()
    def listPath(parent: String, p: String, t: SetOperationType, always: Boolean): ListPath = {
      val path = s"$parent.$p"
      // SCHEMA-17 fix R-150 (A-183): a leaf read as stored is read under the statement's rule
      // first, the cause picking the rule ([[ListPath.read]])
      val (reader, asStored) = readers.collectedReaders(path, t)
      new ListPath(
        aliases.getOrElse(path, path),
        p.split("\\."),
        reader,
        always,
        readsPoints(t),
        asStored
      )
    }
    paths.foreach { case (parent, under) =>
      if (!source.containsKey(parent)) {
        source.put(parent, readers.paths(parent, under.toList))
        // SCHEMA-17 (A-181): a path under a list of objects whose prefix is itself selected
        // (`obj.in.q` beside `obj.in`) is never read by the parent's reader, which reads the
        // prefix -- a list the row path cannot walk through --: collected from the parent's value
        // as its own value, as under a parent read whole
        val shadowed = under.collect {
          case (p, t)
              if listedPaths.contains(s"$parent.$p") &&
                under.exists { case (q, _) => p.startsWith(q + ".") } =>
            listPath(parent, p, t, always = true)
        }
        if (shadowed.nonEmpty) listPaths.put(parent, shadowed.toArray)
      } else
        listPaths.put(
          parent,
          under.map { case (p, t) =>
            listPath(parent, p, t, listedPaths.contains(s"$parent.$p"))
          }.toArray
        )
    }
    // SCHEMA-2b (B7 a) -- each plain PARTITION BY column of the window statement: read by the type
    // its key is read with (`MultiSearch.bucketType`), a DATE / TIME one exactly (A-23), and kept
    val asReadKeys = new java.util.HashMap[String, String]()
    val carriedKeys: Seq[String] =
      if (single.schema.isEmpty) Nil
      else
        carried.flatMap { b =>
          val id = b.identifier
          val name = id.name
          if (id.nested || id.functions.nonEmpty || name.isEmpty || name.indexOf('.') >= 0) None
          else {
            val t = MultiSearch.partitionType(b)
            t.sqlType match {
              case SQLTypes.Date | SQLTypes.Time => source.put(name, readers.exact(name, t))
              // SCHEMA-6 (A-28) -- an `ARRAY` partition column is read as read, as 0.24.0 read it;
              // selected, it keeps its own reader and its partition value is read as read under
              // its own row key, the one the join reads (`PartitionKeys.of`)
              case _ if MultiSearch.arrayLevel(b) && source.containsKey(name) =>
                asReadKeys.put(name, PartitionKeys.asReadKey(name))
              case _ => if (!source.containsKey(name)) put(name, t, keepUnder(name))
            }
            Some(name)
          }
        }
    // SCHEMA-2b -- each carried column paired by instant: its `epoch_millis` doc value, under its
    // own row key (never over the stored value). `instants` is the statement's ONE decision
    // (`PartitionKeys.instantColumns`), the window aggregation's too: taken as it is
    val instantKeys = new java.util.HashMap[String, String]()
    if (single.schema.isDefined)
      instants.foreach(name => instantKeys.put(name, PartitionKeys.instantKey(name)))
    // UNNEST element fields, by inner hit; a column with an element conversion keeps its raw value
    val inner: Map[String, java.util.HashMap[String, JsonNode => Any]] =
      if (single.schema.isEmpty) Map.empty
      else
        (single.nestedHitsMappings ++ UnnestScope.rowExplosion(single)).flatMap {
          case (hit, mappings) =>
            val byField = new java.util.HashMap[String, JsonNode => Any]()
            // SCHEMA-17 fix R-150 (A-183): an element's field is a leaf of the unnested path, its
            // rule read there; the statement's where the path is not known
            val unnested = single.from.unnests.find(_.innerHitsName == hit).map(_.path)
            mappings.foreach { case (subField, output) =>
              if (subField.indexOf('.') < 0 && !elements.contains(output))
                fields.find(_.outputName == output).foreach { f =>
                  val t = typeOf(f)
                  if (readers.typed(t))
                    byField.put(
                      subField,
                      readers.column(output, t, Set.empty, unnested.map(p => s"$p.$subField"))._1
                    )
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
      single.nestedHitsMappings.keySet ++ UnnestScope.rowExplosion(single).keySet ++ carriedKeys ++
      instantKeys.values().toArray(new Array[String](0)) ++
      asReadKeys.values().toArray(new Array[String](0))
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
      ignoredReaders,
      strict,
      keep,
      star && schema.isDefined && fields.forall(isStar),
      flatReaders,
      instantKeys,
      listPaths,
      asReadKeys
    )
  }

  /** The readers of one statement (D5), built once: `JsonNode => Any`, a node never `null` (the
    * caller hands `null` / a JSON `null` to none of them but the scalar wrapper).
    *
    * `kinds`: SCHEMA-17 fix R-172 (A-201) -- each top-level column of a kind of [[IgnoredKinds]],
    * with that kind ([[ignoredKinds]]): its cell gets a reader for the hit's `_ignored` (D8).
    */
  private[client] final class Readers(
    aliases: ListMap[String, String],
    dates: SetOperationValues.DateReading,
    multiValued: MultiValued,
    table: String,
    remedy: String => Option[String] = _ => None,
    nanos: Set[String] = Set.empty,
    isoOnlyDates: Boolean = false,
    stored: Set[String] = Set.empty,
    kinds: Map[String, String] = Map.empty
  ) {

    /** The failure of a several-element cell of `name` (SCHEMA-6 D6). */
    private def several(name: String): MultiValuedCell =
      new MultiValuedCell(name, remedy(name).getOrElse(""), table)

    /** SCHEMA-17 fix R-150 (A-183, A-184) -- the rule of a LEAF's own several values at `path`: as
      * stored where the statement's rule fails them ([[multiValuedFor]]) and no index declares the
      * leaf to this user (`stored`, [[storedLeaves]]); else the statement's. An object takes no
      * `meta` and is never one: a STRUCT column holding several objects (A-168), and a path several
      * objects of a list hold (A-181), keep the statement's rule. Decided when a reader is built,
      * never per row.
      */
    private def leafRule(path: String): MultiValued =
      if (multiValued == MultiValued.Fail && stored.contains(path)) MultiValued.AsRead
      else multiValued

    /** SCHEMA-17 (A-181) -- the failure `cell` of a path read under `parent`, whose `_source` value
      * `node` is a list of objects the schema does not declare one (a declared list types its paths
      * lists, which never fail): where several of its objects hold the path ([[severalHold]]), the
      * LIST is the cause, and the failure names `parent` with the in-place `ALTER` declaring it
      * `ARRAY<STRUCT>` -- after which the path reads its objects' values as a list --; where one
      * object's own field holds several values, the path's own `SET FIELD` (`cell`). An enclosing
      * parent read the same way names itself in turn when several of its objects hold the path: the
      * outermost list is the cause.
      */
    private def fromList(
      parent: String,
      node: JsonNode,
      segments: Array[String],
      cell: MultiValuedCell
    ): MultiValuedCell =
      if (!severalHold(node, segments)) cell
      else listCause(parent, cell)

    /** The failure naming the list `parent` as the cause of `cell` ([[fromList]]). */
    private def listCause(parent: String, cell: MultiValuedCell): MultiValuedCell =
      remedy(parent).fold(cell)(alter => new MultiValuedCell(parent, alter, table))

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

    /** A `_source` column of type `t`, and -- for a date read by its format, and a top-level column
      * of a kind of [[IgnoredKinds]] (SCHEMA-17 fix R-172, A-201) -- the reader of its cell when
      * the hit's `_ignored` names it (D8): Elasticsearch refused some value of it, and its verdict
      * is per VALUE. A cell of one value is NULL (the value refused); an array keeps each element
      * the kind's own Elasticsearch parse accepts -- a value Elasticsearch indexed, read by the
      * column's reader -- and answers the others NULL, under the scalar column's rule of several
      * values. A column declared `ARRAY<X>` reads its elements the same way (A-202), a scalar cell
      * a list of its one value, NULL.
      */
    def column(
      name: String,
      t: SetOperationType,
      keepKeys: Set[String]
    ): (JsonNode => Any, Option[JsonNode => Any]) =
      columnUnder(name, t, keepKeys, leafRule(name))

    /** [[column]], its leaf's rule ([[leafRule]]) read at `leaf` -- the field's path where `name`
      * is the key its failure names (an UNNEST element field's output) --; the statement's rule
      * where the path is not known.
      */
    def column(
      name: String,
      t: SetOperationType,
      keepKeys: Set[String],
      leaf: Option[String]
    ): (JsonNode => Any, Option[JsonNode => Any]) =
      columnUnder(name, t, keepKeys, leaf.fold(multiValued)(leafRule))

    /** [[column]] with `mode` the rule of a scalar or `GEO_POINT` cell's several values. */
    private def columnUnder(
      name: String,
      t: SetOperationType,
      keepKeys: Set[String],
      mode: MultiValued
    ): (JsonNode => Any, Option[JsonNode => Any]) =
      t.sqlType match {
        case SQLTypes.Array(_) =>
          val element = t.element.getOrElse(SetOperationType(SQLTypes.Any))
          (list(name, element), ignoredList(name, element))
        // SCHEMA-17 (A-168): a list of objects in a column declared STRUCT takes the default rule
        // (`multiValued`, [[multiValuedFor]]) -- the in-place ALTER that declares an `object` field
        // `ARRAY<STRUCT>` ships in the same release (A-166), so the failure names a remedy that
        // runs; a column declared `ARRAY<STRUCT>` is read by the `Array` arm above, one object a
        // list of one. An object is no leaf: the statement's rule, whatever `mode` (R-150, A-183)
        case SQLTypes.Struct if t.fields.nonEmpty =>
          (scalar(name, struct(name, t, keepKeys), multiValued), None)
        case SQLTypes.GeoPoint =>
          (geo(name, mode), if (pointsOf(name)) Some(ignoredGeo(name, mode)) else None)
        case _ if !typed(t) => (asRead, None)
        case scalarType =>
          val (one, ignoredOne) = scalarOne(name, scalarType, t.format)
          (
            scalar(name, one, mode),
            ignoredOne.map(element =>
              (node: JsonNode) =>
                if (node != null && node.isArray) scalarValue(name, element, node, mode) else null
            )
          )
      }

    /** SCHEMA-17 fix R-150 (A-183) -- the readers of the values a selected path `path` of type `t`
      * collects under a list of objects ([[collected]]): its reader under the statement's rule, and
      * -- where it is a leaf whose own rule differs ([[leafRule]]) -- its reader under that rule,
      * else `null`. The first reads; where it fails a several-valued cell, the CAUSE picks the rule
      * ([[severalHold]], on the failure path only): several objects hold the path -- the list's
      * failure, the statement's rule --, else one object's own field holds the values -- the leaf's
      * own rule. Never the leaf's rule over values several objects hold: that would answer a list
      * no object holds.
      */
    def collectedReaders(path: String, t: SetOperationType): (JsonNode => Any, JsonNode => Any) =
      if (leafRule(path) == multiValued) (column(path, t, Set.empty)._1, null)
      else (columnUnder(path, t, Set.empty, multiValued)._1, column(path, t, Set.empty)._1)

    /** SCHEMA-2b (A-23) -- a `DATE` / `TIME` column read EXACTLY, as the key of its aggregation
      * level is (`AggregateReading`): a value its format reads to an instant that is exactly a UTC
      * day (`DATE`) or a time of day of 1970-01-01 (`TIME`) is that day / time; any other value as
      * read (D6, `Unreadable.KeepAsStored`), so it pairs with a key read as it always was. Any
      * other type -- and a date read as stored, or with no format -- is [[column]] 's reader.
      */
    def exact(name: String, t: SetOperationType): JsonNode => Any =
      (t.sqlType, t.format) match {
        case (kind @ (SQLTypes.Date | SQLTypes.Time), Some(spec))
            if DateRoute.of(kind, t.format, dates) == DateRoute.Typed =>
          val instantOf = instants(spec)
          scalar(
            name,
            node => {
              val instant = instantOf(node)
              val value = if (instant eq null) null else AggregateReading.exactly(kind, instant)
              if (value != null) value else asRead(node)
            },
            leafRule(name)
          )
        case _ => column(name, t, Set.empty)._1
      }

    /** The instant a date field's value is under its mapping format `spec` -- the format
      * [[byFormat]] reads it with --, `null` where the format does not read it.
      */
    private def instants(spec: String): JsonNode => Instant = {
      val format = dates.format(spec).get
      val fast = spec == app.softnetwork.elastic.sql.query.TemporalLiterals.DefaultDateFormat
      node => {
        val z = if (fast && node.isTextual) DefaultDates.read(node.asText()) else null
        if (z ne null) z.toInstant
        else {
          val outcome =
            if (node.isTextual) format.read(node.asText())
            else if (node.isNumber) format.readValue(node.numberValue())
            else MappedDateFormat.Rejected
          outcome match {
            case MappedDateFormat.Read(instant) => instant
            case _                              => null
          }
        }
      }
    }

    /** A computed column's `fields` entry: Elasticsearch's array unwrapped (#418 part 1). A
      * computed value is no field: the statement's rule, whatever leaf its name spells (R-150).
      */
    def scripted(name: String, t: SetOperationType): JsonNode => Any =
      t.sqlType match {
        case SQLTypes.Array(_) => column(name, t, Set.empty)._1
        case _ if !typed(t)    => scalar(name, asRead)
        case _                 => columnUnder(name, t, Set.empty, multiValued)._1
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
      // SCHEMA-6 (A-26): the selected children, in selection order, read under a list of objects;
      // a GEO_POINT leaf collected point by point (SCHEMA-17 A-181, [[collected]])
      val points: Set[String] =
        under.collect { case (p, t) if p.indexOf('.') < 0 && readsPoints(t) => p }.toSet
      // SCHEMA-17 fix R-150 (A-183): a leaf read as stored is read under the statement's rule over
      // a list of objects, its own reader (`leaves`) kept for the cause that is its own
      // ([[collectedReaders]])
      val leafTypes: Map[String, SetOperationType] =
        under.collect { case (p, t) if p.indexOf('.') < 0 => p -> t }.toMap
      val children: Array[(String, Array[String], JsonNode => Any, Boolean, JsonNode => Any)] =
        (under.map(_._1).filter(_.indexOf('.') < 0) ++ nested.keys).distinct.map { k =>
          val strict: JsonNode => Any = leafTypes.get(k) match {
            case Some(t) if leafRule(s"$name.$k") != multiValued =>
              columnUnder(s"$name.$k", t, Set.empty, multiValued)._1
            case _ => null
          }
          if (strict eq null) (k, Array(k), leaves.get(k), points.contains(k), null)
          else (k, Array(k), strict, points.contains(k), leaves.get(k))
        }.toArray
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
        } else if (node.isArray && children.nonEmpty) {
          // a list of objects: each selected path holds the values its elements hold, collected in
          // order -- as Elasticsearch indexes `obj.a` = [5, 50] for `obj: [{a: 5}, {a: 50}]` --,
          // read by the path's reader: one value is it, several are an `ARRAY` path's list (a
          // path under a declared list of objects is one, SCHEMA-17 A-181) or fail a scalar path,
          // naming the list's in-place ALTER when several objects hold it ([[fromList]]), none is
          // NULL. R-150 (A-183): one object's own several values of a leaf read as stored are
          // its list -- the cause is the leaf's, never the list's
          val entries = ListMap.newBuilder[String, Any]
          var i = 0
          while (i < children.length) {
            val (k, segments, reader, leafPoints, asStored) = children(i)
            val values = collected(node, segments, leafPoints)
            val value =
              if (values eq null) null
              else
                try reader(values)
                catch {
                  case cell: MultiValuedCell =>
                    if (asStored eq null) throw fromList(name, node, segments, cell)
                    else if (severalHold(node, segments)) throw listCause(name, cell)
                    else asStored(values)
                }
            entries += aliases.getOrElse(k, k) -> value
            i += 1
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
      node => scalarValue(name, one, node, multiValued)

    /** [[scalar]] under the given [[MultiValued]] rather than the statement's. */
    def scalar(name: String, one: JsonNode => Any, mode: MultiValued): JsonNode => Any =
      node => scalarValue(name, one, node, mode)

    private def scalarValue(
      name: String,
      one: JsonNode => Any,
      node: JsonNode,
      mode: MultiValued
    ): Any =
      if (node == null || node.isNull) null
      else if (node.isArray) {
        val size = node.size()
        if (size == 0) null
        else if (size == 1) {
          val only = node.get(0)
          if (only != null && only.isArray) {
            if (only.size() == 1) scalarValue(name, one, only, mode)
            else if (only.size() > 1 && mode == MultiValued.Fail)
              throw several(name)
            else asRead(node)
          } else element(one)(only)
        } else
          mode match {
            case MultiValued.AsRead =>
              val values = ListBuffer.empty[Any]
              val it = node.elements()
              while (it.hasNext) values += element(one)(it.next())
              values.toList
            case MultiValued.Fail => throw several(name)
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
      val points = element.sqlType == SQLTypes.GeoPoint
      node =>
        if (node == null || node.isNull) null
        // SCHEMA-6: `[lon, lat]` in an `ARRAY<GEO_POINT>` is ONE point, a list of one (R-34)
        else if (node.isArray && !(points && coordinates(node))) {
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
      * `[lon, lat]` is ONE point, several points per `mode` (the leaf's rule, [[leafRule]]); a
      * value that is not a point as read (D6).
      */
    def geo(name: String, mode: MultiValued): JsonNode => Any = {
      val one = geoOne(name)
      node =>
        if (node == null || node.isNull) null
        else if (node.isArray && !coordinates(node)) {
          val size = node.size()
          if (size == 0) null
          else if (size == 1) element(one)(node.get(0))
          else
            mode match {
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
              case MultiValued.Fail => throw several(name)
            }
        } else one(node)
    }

    private def geoOne(name: String): JsonNode => Any =
      node =>
        try point(name, node)
        catch { case _: IllegalArgumentException => asRead(node) }

    private def point(name: String, node: JsonNode): Any =
      SetOperationValues.geoPoint(HitReading.plain(node), name)

    /** SCHEMA-17 fix R-172 (A-201) -- whether `name` is a top-level `geo_point` field. */
    private def pointsOf(name: String): Boolean = kinds.get(name).contains("geo_point")

    /** SCHEMA-17 fix R-172 (A-201) -- ONE element of a `geo_point` cell the hit's `_ignored` names:
      * the point Elasticsearch indexed, or NULL for a value that is no point, which it refused.
      */
    private def ignoredPoint(name: String): JsonNode => Any =
      node =>
        try point(name, node)
        catch { case _: IllegalArgumentException => null }

    /** SCHEMA-17 fix R-172 (A-201) -- a `GEO_POINT` cell the hit's `_ignored` names (D8): one value
      * -- `[lon, lat]` is one point -- is NULL, the value Elasticsearch refused; several are each a
      * point or NULL ([[ignoredPoint]]), per `mode`.
      */
    private def ignoredGeo(name: String, mode: MultiValued): JsonNode => Any = {
      val one = ignoredPoint(name)
      node =>
        if (node == null || !node.isArray || coordinates(node)) null
        else {
          val size = node.size()
          if (size == 0) null
          else if (size == 1) element(one)(node.get(0))
          else
            mode match {
              case MultiValued.AsRead =>
                val values = ListBuffer.empty[Any]
                val it = node.elements()
                while (it.hasNext) values += element(one)(it.next())
                values.toList
              case MultiValued.Fail => throw several(name)
            }
        }
    }

    /** SCHEMA-17 fix R-172 (A-202) -- the cell of a column declared `ARRAY<X>` the hit's `_ignored`
      * names (D8), for an element read by its format (a date) or of a kind of [[IgnoredKinds]]: a
      * `List` of its elements, each the kind's own Elasticsearch parse accepts read by the column's
      * element reader, the others NULL; a scalar cell -- one value, the one Elasticsearch refused
      * -- a list of one NULL. `None` for any other element: a string keeps its text, a `STRUCT` or
      * a nested list its own reader.
      */
    private def ignoredList(
      name: String,
      elementType: SetOperationType
    ): Option[JsonNode => Any] = {
      val points = elementType.sqlType == SQLTypes.GeoPoint
      val read: Option[JsonNode => Any] = elementType.sqlType match {
        case SQLTypes.GeoPoint => if (pointsOf(name)) Some(ignoredPoint(name)) else None
        case SQLTypes.Struct | SQLTypes.Array(_) => None
        case _ if !typed(elementType)            => None
        case scalarType => scalarOne(name, scalarType, elementType.format)._2
      }
      read.map { one => (node: JsonNode) =>
        if (node == null || node.isNull) null
        // SCHEMA-6: `[lon, lat]` in an `ARRAY<GEO_POINT>` is ONE point (R-34)
        else if (node.isArray && !(points && coordinates(node))) {
          val values = ListBuffer.empty[Any]
          val it = node.elements()
          while (it.hasNext) values += element(one)(it.next())
          values.toList
        } else List(null)
      }
    }

    /** ONE non-null, non-array value of a scalar type, and -- for a date read by its format, and a
      * top-level column of a kind of [[IgnoredKinds]] (SCHEMA-17 fix R-172, A-201) -- the same read
      * answering NULL where the kind's own Elasticsearch parse refuses the value (D8): the format
      * for a date, [[acceptance]] for the others.
      */
    def scalarOne(
      name: String,
      sqlType: app.softnetwork.elastic.sql.`type`.SQLType,
      format: Option[String]
    ): (JsonNode => Any, Option[JsonNode => Any]) = {
      val read = scalarRead(name, sqlType, format)
      if (read._2.isDefined) read
      else
        kinds.get(name) match {
          case Some(kind) =>
            val one = read._1
            val accepts = acceptance(name, kind)
            (one, Some((node: JsonNode) => if (accepts(node)) one(node) else null))
          case None => read
        }
    }

    /** SCHEMA-17 fix R-172 (A-201) -- whether Elasticsearch's own parse of the field kind `kind`
      * (`ignore_malformed` on, `coerce` on) accepts a node, ONE element of a cell the hit's
      * `_ignored` names: an element it accepts is a value it indexed, kept and read by the column's
      * reader; one it refuses is NULL. Chosen once per reader, never per row. Where core is not
      * sure of the verdict (an object or a nested list, an `ip` spelling beyond the plain literals)
      * the element is KEPT: never NULL a value Elasticsearch indexed. An empty text is no value
      * (Elasticsearch indexes nothing for it, and names nothing): kept, read as the column reads
      * it.
      *
      *   - `byte` / `short` / `integer` / `long`: [[WholeRange]] reads it, as Elasticsearch does;
      *   - `unsigned_long`: an integral number within `[0, 2^64 - 1]`; a whole floating number `>=
      *     0`, whatever its size (Elasticsearch saturates one `>= 2^63` to `2^63 - 1` and indexes
      *     it); a text (not trimmed) holding a whole number within `[0, 2^64 - 1]`
      *     ([[unsignedLong]]);
      *   - `float` / `half_float` / `double` / `scaled_float`: a number, or a text `parseFloat` /
      *     `parseDouble` reads, that is finite in the kind (`half_float`: below 65520 in magnitude,
      *     which Lucene's half-float rounding, ties to even, keeps finite);
      *   - `boolean`: a JSON boolean, or the text `true`, `false` or empty;
      *   - `ip`: a text that is an IPv4 literal, or an IPv6 one ([[ipLiteral]]): never resolved;
      *   - `geo_point`: a point core reads.
      */
    private def acceptance(name: String, kind: String): JsonNode => Boolean = {
      val accepts: JsonNode => Boolean = kind match {
        case "byte"          => node => wholeAccepted(WholeRange.ForTinyInt.read(node))
        case "short"         => node => wholeAccepted(WholeRange.ForSmallInt.read(node))
        case "integer"       => node => wholeAccepted(WholeRange.ForInt.read(node))
        case "long"          => node => wholeAccepted(WholeRange.ForBigInt.read(node))
        case "unsigned_long" => node => unsignedLong(node)
        case "float"         => node => finiteFloat(node, Float.PositiveInfinity)
        case "half_float"    => node => finiteFloat(node, HalfFloatLimit)
        case "double" | "scaled_float" =>
          node =>
            if (node.isNumber) java.lang.Double.isFinite(node.doubleValue())
            else if (node.isTextual) {
              val text = node.asText()
              text.isEmpty || {
                try java.lang.Double.isFinite(java.lang.Double.parseDouble(text))
                catch { case _: NumberFormatException => false }
              }
            } else !node.isBoolean
        case "boolean" =>
          node =>
            if (node.isBoolean) true
            else if (node.isTextual) {
              val text = node.asText()
              text.isEmpty || text == "true" || text == "false"
            } else !node.isNumber
        case "ip" =>
          node =>
            if (node.isTextual) ipLiteral(node.asText())
            else !(node.isNumber || node.isBoolean)
        case "geo_point" =>
          node =>
            try {
              point(name, node)
              true
            } catch { case _: IllegalArgumentException => false }
        case _ => _ => true
      }
      if (kind == "geo_point") accepts
      else node => node.isContainerNode || accepts(node)
    }

    private def wholeAccepted(read: Any): Boolean =
      read match {
        case null | _: java.lang.Integer | _: java.lang.Long => true
        case _                                               => false
      }

    /** `float` (`limit` the float infinity) and `half_float` (`limit` 65520): Elasticsearch's
      * `parser.floatValue(coerce)` -- `Float.parseFloat` on a text, which trims --, then finite.
      */
    private def finiteFloat(node: JsonNode, limit: Float): Boolean =
      if (node.isNumber) Math.abs(node.floatValue()) < limit
      else if (node.isTextual) {
        val text = node.asText()
        text.isEmpty || {
          try Math.abs(java.lang.Float.parseFloat(text)) < limit
          catch { case _: NumberFormatException => false }
        }
      } else !node.isBoolean

    /** `unsigned_long`: Elasticsearch's `parseUnsignedLong` (7.17 / 8.18), which refuses a decimal
      * part ("has a decimal part") and a negative value, never truncating one:
      *
      *   - an integral number within `[0, 2^64 - 1]`;
      *   - a floating number -- a double, a float or a big decimal: Elasticsearch's parser reads
      *     every JSON float token as a `Double` -- equal to its floor (`Double.compare`) and not
      *     negative, whatever its size: Elasticsearch saturates a whole floating number `>= 2^63`
      *     (an infinity included) to `2^63 - 1` and indexes it;
      *   - a text (not trimmed, read by `parseUnsignedLong`, else `new BigDecimal`) that is exact
      *     ([[unsignedDecimal]]). No value is materialised beyond the digits it holds: a text of a
      *     large exponent is judged by its digit count.
      */
    private def unsignedLong(node: JsonNode): Boolean =
      if (node.isIntegralNumber) {
        val b = node.bigIntegerValue()
        b.signum() >= 0 && b.bitLength() <= 64
      } else if (node.isNumber) {
        // Elasticsearch: `Double.compare(v, Math.floor(v)) != 0` refuses, then `longValue() < 0`
        val d = node.doubleValue()
        java.lang.Double.compare(d, Math.floor(d)) == 0 && !(d < 0.0d)
      } else if (node.isTextual) {
        val text = node.asText()
        text.isEmpty || {
          val unsigned =
            try {
              java.lang.Long.parseUnsignedLong(text)
              true
            } catch { case _: NumberFormatException => false }
          unsigned || {
            val d =
              try new java.math.BigDecimal(text)
              catch { case _: NumberFormatException => null }
            (d ne null) && unsignedDecimal(d)
          }
        }
      } else !node.isBoolean

    /** A big decimal an `unsigned_long` takes: zero, or a positive whole number -- no non-zero
      * digit after the point (`toBigIntegerExact`) -- below 2^64. The digits before the point are
      * counted in Long arithmetic, off the scale (trailing zeros change none of them), before any
      * value is built.
      */
    private def unsignedDecimal(d: java.math.BigDecimal): Boolean =
      d.signum() == 0 || {
        val integerDigits = d.precision().toLong - d.scale()
        d.signum() > 0 && integerDigits > 0 && integerDigits <= 20 && {
          val whole = if (d.scale() <= 0) d else d.stripTrailingZeros()
          whole.scale() <= 0 && whole.toBigInteger.bitLength() <= 64
        }
      }

    /** The scalar read of [[scalarOne]], before any D8 reader of a kind of [[IgnoredKinds]]. */
    private def scalarRead(
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
                  val read = byFormat(kind, spec, nanos.contains(name))
                  // SCHEMA-17 (LEAD-Q1 b): a `_field_caps` date of no declared format is read as
                  // ISO 8601 only -- any other spelling fails loudly, never kept as stored
                  val loud = isoOnlyDates && IsoOnlyFormats.contains(spec)
                  (
                    node => {
                      val value = read(node)
                      if (value != null) value
                      else if (loud) throw new UnreadableDateCell(name, node.toString, table)
                      else asRead(node)
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
      spec: String,
      nanos: Boolean
    ): JsonNode => Any = {
      // the statement's one format of `spec`, the one its route was decided on (`Typed`: the major
      // is known)
      val format = dates.format(spec).get
      // Elasticsearch's default format and the ISO one a `_field_caps` date is read by (SCHEMA-7
      // A-40) take the ISO fast path, which reads up to milliseconds and hands any other shape on
      val fast = DefaultDates.fastFormats.contains(spec)
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
          // SCHEMA-7 (D3): a `date_nanos` field's value to the nanosecond
          val outcome =
            if (node.isTextual) format.read(node.asText(), nanos)
            else if (node.isNumber) format.readValue(node.numberValue(), nanos)
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
    * (`Unreadable.KeepAsStored`): narrowing it would wrap it; where the hit's `_ignored` names the
    * field, the value is NULL (D8, SCHEMA-17 fix R-172). So is a `long` text where the majors
    * disagree: 7.17 and 8.18 refuse a scale below -19 and read 0 above 19, where 6.8 reads the
    * truncated value -- except a non-zero value with a scale of 2^29 + 8 (536,870,920) or more,
    * which 6.8 refuses: its `toBigInteger` divides by `BigInteger.TEN.pow(scale)`, which reports an
    * overflow from that scale on (`bitLength(10) * scale / 32 > 2^26` on its JDK 15.0.1), and the
    * `ArithmeticException` is a refusal. A value below 1 with a smaller scale, and a zero at any
    * scale, are 0 on every major. No digit count or scale is computed in `Int` arithmetic, and no
    * value is materialised beyond the digits its text holds.
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

    /** The smallest scale 6.8.23 refuses in a non-zero `long` text: `BigInteger.TEN.pow(scale)`
      * overflows once `4L * scale / 32 > (1 << 26)` (JDK 15.0.1's `pow`).
      */
    private val Es6RefusedScale: Int = (1 << 29) + 8

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
              else if (d.signum() == 0) java.lang.Long.valueOf(0L)
              // 6.8 refuses it, 7.17 / 8.18 read 0: the majors disagree
              else if (d.scale() >= Es6RefusedScale) OutOfType
              else if (integerDigits <= 0) java.lang.Long.valueOf(0L)
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

    /** The formats whose ISO text this fast path reads as their own reader does: Elasticsearch's
      * default and the ISO format a `_field_caps` date is read by (SCHEMA-7 A-40).
      */
    val fastFormats: Set[String] = Set(
      app.softnetwork.elastic.sql.query.TemporalLiterals.DefaultDateFormat,
      app.softnetwork.elastic.sql.query.TemporalLiterals.FieldCapsDateFormat
    )

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
