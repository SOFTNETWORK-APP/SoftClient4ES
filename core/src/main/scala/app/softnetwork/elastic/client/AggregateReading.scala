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
import app.softnetwork.elastic.sql.query.{Bucket, MultiSearch, SingleSearch, TemporalLiterals}
import app.softnetwork.elastic.sql.query.MultiSearch.SetOperationType
import app.softnetwork.elastic.sql.serialization.JacksonConfig
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode}

import java.time.{Instant, LocalDate, LocalDateTime, LocalTime, ZoneOffset, ZonedDateTime}
import java.time.temporal.ChronoUnit
import scala.collection.immutable.ListMap

/** SCHEMA-2b (B1-B6) -- how ONE statement reads the values of its aggregation response by its
  * columns' declared types, where each is first read (a Jackson node):
  *
  *   - `keys`: a level's bucket key, by the level's name (`Bucket.name`), read by the type of its
  *     key ([[MultiSearch.bucketType]], B2);
  *   - `metrics`: a metric's `value`, by aggregation name, read by the type of the SELECT item that
  *     owns it ([[MultiSearch.fieldType]], B3 / B4) -- exactly: a number becomes the class only
  *     where the class holds it unchanged;
  *   - `tops`: a `FIRST_VALUE` / `LAST_VALUE` / `ARRAY_AGG` hit's `_source`, by top_hits name, its
  *     aggregated column read by the column's reader (`HitReading.Readers`, B5).
  *
  * A value a reader cannot read answers [[AggregateReading.Unread]], and the caller reads it as it
  * always did (A-12). Built ONCE per statement, by `SchemaColumns.of`; [[AggregateReading.Empty]]
  * reads everything as 0.24.0 did.
  */
private[client] final class AggregateReading(
  keys: java.util.HashMap[String, AggregateReading.KeyReader],
  metrics: java.util.HashMap[String, JsonNode => Any],
  tops: java.util.HashMap[String, java.util.HashMap[String, JsonNode => Any]]
) {

  /** The reader of the bucket keys of the level `name`, `null` for today's reading. */
  def key(name: String): AggregateReading.KeyReader = if (keys.isEmpty) null else keys.get(name)

  /** The reader of the `value` of the aggregation `name`, `null` for today's reading. */
  def metric(name: String): JsonNode => Any = if (metrics.isEmpty) null else metrics.get(name)

  /** The readers of the `_source` keys of the top_hits `name`, `null` for today's reading. */
  def top(name: String): java.util.HashMap[String, JsonNode => Any] =
    if (tops.isEmpty) null else tops.get(name)

  /** Whether it reads nothing: every value as 0.24.0 did. */
  def isEmpty: Boolean = keys.isEmpty && metrics.isEmpty && tops.isEmpty
}

private[client] object AggregateReading {

  /** What a reader answers for a value it cannot read: the caller reads it as it always did. */
  case object Unread

  /** Reads a bucket node; `doubleLevel` as `ElasticConversion.extractBucketKey` takes it. */
  trait KeyReader {
    def read(bucket: JsonNode, doubleLevel: Boolean): Any
  }

  /** Today's reading everywhere. */
  val Empty: AggregateReading =
    new AggregateReading(
      new java.util.HashMap[String, KeyReader](),
      new java.util.HashMap[String, JsonNode => Any](),
      new java.util.HashMap[String, java.util.HashMap[String, JsonNode => Any]]()
    )

  private val DayMillis = 86400000L

  /** The readers of `single`'s aggregation response; [[Empty]] where it has no schema (D10: no
    * declared type) or no aggregation.
    *
    * @param instants
    *   the levels whose key is read as the bucket's INSTANT ([[InstantKey]]): the partition columns
    *   of a window statement paired by instant ([[PartitionKeys.instantColumns]]), when `single` is
    *   its window aggregation request; none otherwise.
    */
  def of(
    single: SingleSearch,
    dates: SetOperationValues.DateReading,
    instants: Set[String] = Set.empty
  ): AggregateReading =
    if (single.schema.isEmpty || (single.buckets.isEmpty && single.sqlAggregations.isEmpty)) Empty
    else {
      val readers =
        new HitReading.Readers(
          single.fieldAliases,
          dates,
          HitReading.multiValuedFor(single),
          single.sources.headOption.getOrElse(""),
          HitReading.remedyOf(single.schema)
        )
      val keys = new java.util.HashMap[String, KeyReader]()
      single.buckets.foreach { b =>
        val name = b.name
        if (name.nonEmpty && !keys.containsKey(name)) {
          val reader =
            if (instants.contains(name)) InstantKey
            // a GROUP BY level's key by its type, a window level's by its partition type (A-28)
            else keyReader(name, MultiSearch.levelType(single, b), dates, readers)
          if (reader ne null) keys.put(name, reader)
        }
      }
      // the SELECT item that owns each aggregation: its output name is the aggregation's name
      val owners: Map[String, app.softnetwork.elastic.sql.query.Field] =
        single.select.fieldsWithComputedAliases
          .filter(f => f.isAggregation || f.isBucketScript || f.identifier.hasWindow)
          .foldLeft(Map.empty[String, app.softnetwork.elastic.sql.query.Field]) { (acc, f) =>
            if (acc.contains(f.outputName)) acc else acc + (f.outputName -> f)
          }
      val metrics = new java.util.HashMap[String, JsonNode => Any]()
      val tops = new java.util.HashMap[String, java.util.HashMap[String, JsonNode => Any]]()
      single.sqlAggregations.foreach { case (name, aggregation) =>
        // an aggregation the client plan cannot hold is the search's own failure, not a reader's
        val client: Option[ClientAggregation] =
          if (aggregation.auxiliary) None
          else scala.util.Try(sqlAggregationToClientAggregation(aggregation)).toOption
        client.foreach { client =>
          client.aggType match {
            case AggregationType.Count | AggregationType.Sum | AggregationType.Avg |
                AggregationType.Min | AggregationType.Max | AggregationType.BucketScript =>
              owners.get(name).foreach { field =>
                val reader = metricReader(MultiSearch.fieldType(field, single))
                if (reader ne null) metrics.put(name, reader)
              }
            case AggregationType.FirstValue | AggregationType.LastValue |
                AggregationType.ArrayAgg =>
              // the response names a top_hits by its last segment, a nested one included
              val leaf = name.substring(name.lastIndexOf('.') + 1)
              val source = aggregation.sourceField
              single.schema.flatMap(_.find(source)).foreach { column =>
                val t = MultiSearch.columnType(column)
                val key = aggregation.nestedElement match {
                  case Some(n) if source.startsWith(n.path + ".") =>
                    source.substring(n.path.length + 1)
                  case Some(_) => source.substring(source.lastIndexOf('.') + 1)
                  case None    => source
                }
                if (key.nonEmpty && !tops.containsKey(leaf)) {
                  val byField = new java.util.HashMap[String, JsonNode => Any]()
                  byField.put(key, readers.column(source, t, Set.empty)._1)
                  tops.put(leaf, byField)
                }
              }
            case _ => // statistics, percentiles, rankings: as today
          }
        }
      }
      if (keys.isEmpty && metrics.isEmpty && tops.isEmpty) Empty
      else new AggregateReading(keys, metrics, tops)
    }

  // ---------------------------------------------------------------------------------------------
  // Keys (B2)
  // ---------------------------------------------------------------------------------------------

  /** The reader of a level's keys of type `t`; `null` for a type read as it comes (NUMERIC,
    * TEMPORAL, ANY, nested types), whose keys keep today's reading.
    */
  private def keyReader(
    name: String,
    t: SetOperationType,
    dates: SetOperationValues.DateReading,
    readers: HitReading.Readers
  ): KeyReader = {
    val route = DateRoute.of(t.sqlType, t.format, dates)
    route match {
      case DateRoute.AsStored(_) => TextKey
      case DateRoute.Typed =>
        t.sqlType match {
          case SQLTypes.Boolean => BooleanKey
          case SQLTypes.TinyInt =>
            new WholeKey(Byte.MinValue.toLong, Byte.MaxValue.toLong, int = true)
          case SQLTypes.SmallInt =>
            new WholeKey(Short.MinValue.toLong, Short.MaxValue.toLong, int = true)
          case SQLTypes.Int    => new WholeKey(Int.MinValue.toLong, Int.MaxValue.toLong, int = true)
          case SQLTypes.BigInt => new WholeKey(Long.MinValue, Long.MaxValue, int = false)
          case SQLTypes.Double => DoubleKey
          case SQLTypes.Real   => RealKey
          case kind @ (SQLTypes.Date | SQLTypes.Timestamp | SQLTypes.Time) =>
            // a scripted key's text: D5's reader of Elasticsearch's default format (no second
            // date rule), or of a computed date where the major is not known
            val default = Some(TemporalLiterals.DefaultDateFormat)
            val text: JsonNode => Any =
              if (DateRoute.of(kind, default, dates) == DateRoute.Typed)
                readers.column(name, SetOperationType(kind, format = default), Set.empty)._1
              else readers.column(name, SetOperationType(kind), Set.empty)._1
            new TemporalKey(kind, text)
          case SQLTypes.Varchar | SQLTypes.Keyword | SQLTypes.Text | SQLTypes.Char => TextKey
          case _                                                                   => null
        }
    }
  }

  /** `key_as_string` where the response has one (Elasticsearch's own text), else the key's text --
    * never re-read as a temporal.
    */
  private object TextKey extends KeyReader {
    def read(bucket: JsonNode, doubleLevel: Boolean): Any = {
      val text = bucket.get("key_as_string")
      if (text != null && text.isTextual) text.asText()
      else {
        val key = bucket.get("key")
        if (key != null && key.isValueNode && !key.isNull) key.asText() else Unread
      }
    }
  }

  /** A window partition level paired by instant ([[PartitionKeys.instantColumns]]): the bucket's
    * `key`, the epoch milliseconds of the instant Elasticsearch buckets the date by (aggregations
    * are millisecond-resolution), as its decimal text -- the spelling of the base side's
    * `epoch_millis` doc value ([[PartitionKeys.instantOf]]). Never `key_as_string`: the field's
    * format at UTC spells one instant otherwise than the stored text does (`2024-01-31 10:30 Z` vs
    * `... 11:30 CET`).
    */
  private object InstantKey extends KeyReader {
    def read(bucket: JsonNode, doubleLevel: Boolean): Any = {
      val key = bucket.get("key")
      val millis =
        if (key == null || !key.isNumber) null
        else wholeOf(key, Long.MinValue, Long.MaxValue, text = false)
      if (millis eq null) Unread else millis.toString
    }
  }

  /** A `boolean` level: the key `1` / `0` (`key_as_string` `"true"` / `"false"`). */
  private object BooleanKey extends KeyReader {
    def read(bucket: JsonNode, doubleLevel: Boolean): Any = {
      val key = bucket.get("key")
      if (key == null) Unread
      else if (key.isBoolean) java.lang.Boolean.valueOf(key.booleanValue())
      else if (key.isNumber) {
        val d = key.doubleValue()
        if (d == 1d) java.lang.Boolean.TRUE else if (d == 0d) java.lang.Boolean.FALSE else Unread
      } else if (key.isTextual)
        SetOperationValues.boolean(key.asText()) match {
          case b: java.lang.Boolean => b
          case _                    => Unread
        }
      else Unread
    }
  }

  /** A whole level: the key when it is a whole number within `[min, max]`, whatever its node kind
    * (B3's exact rule); an `Integer` (`int`) or a `Long`.
    */
  private final class WholeKey(min: Long, max: Long, int: Boolean) extends KeyReader {
    def read(bucket: JsonNode, doubleLevel: Boolean): Any = {
      val key = bucket.get("key")
      val whole = if (key == null) null else wholeOf(key, min, max, text = true)
      if (whole eq null) Unread
      else if (int) java.lang.Integer.valueOf(whole.intValue())
      else whole
    }
  }

  /** A `DOUBLE` level: the number; a non-finite key's text (`NonFiniteKeys`) is the Double. */
  private object DoubleKey extends KeyReader {
    def read(bucket: JsonNode, doubleLevel: Boolean): Any = {
      val key = bucket.get("key")
      if (key == null) Unread
      else if (key.isNumber) java.lang.Double.valueOf(key.doubleValue())
      else if (key.isTextual && ElasticConversion.NonFiniteKeys.contains(key.asText()))
        java.lang.Double.valueOf(key.asText())
      else Unread
    }
  }

  /** A `REAL` level: the number at `REAL`'s precision; a non-finite key's text is the Float. */
  private object RealKey extends KeyReader {
    def read(bucket: JsonNode, doubleLevel: Boolean): Any = {
      val key = bucket.get("key")
      if (key == null) Unread
      else if (key.isNumber) java.lang.Float.valueOf(key.floatValue())
      else if (key.isTextual && ElasticConversion.NonFiniteKeys.contains(key.asText()))
        java.lang.Float.valueOf(key.asText())
      else Unread
    }
  }

  /** A typed `DATE` / `TIMESTAMP` / `TIME` level: a whole key is epoch milliseconds at UTC (the
    * bridge sends no `time_zone`), read EXACTLY (A-23): a `DATE` only at a whole UTC day, a `TIME`
    * only on 1970-01-01, any other key as it always was; a text key (a scripted level) by `text`.
    */
  private final class TemporalKey(kind: SQLType, text: JsonNode => Any) extends KeyReader {
    def read(bucket: JsonNode, doubleLevel: Boolean): Any = {
      val key = bucket.get("key")
      if (key == null || key.isNull) Unread
      else if (key.isNumber) {
        val millis = wholeOf(key, Long.MinValue, Long.MaxValue, text = false)
        if (millis eq null) Unread
        else {
          val value = exactly(kind, millis.longValue())
          if (value == null) Unread else value
        }
      } else if (key.isTextual) text(key)
      else Unread
    }
  }

  /** `instant` as a value of `kind` when it is EXACTLY one (A-23): a `DATE` at a whole UTC day, a
    * `TIME` on 1970-01-01, a `TIMESTAMP` always (at UTC); `null` otherwise.
    */
  private[client] def exactly(kind: SQLType, instant: Instant): Any =
    kind match {
      case SQLTypes.Date =>
        if (instant.getNano == 0 && Math.floorMod(instant.getEpochSecond, 86400L) == 0L)
          LocalDate.ofEpochDay(Math.floorDiv(instant.getEpochSecond, 86400L))
        else null
      case SQLTypes.Time =>
        if (instant.getEpochSecond >= 0L && instant.getEpochSecond < 86400L)
          LocalTime.ofNanoOfDay(instant.getEpochSecond * 1000000000L + instant.getNano)
        else null
      case _ => instant.atZone(ZoneOffset.UTC)
    }

  private def exactly(kind: SQLType, millis: Long): Any =
    kind match {
      case SQLTypes.Date =>
        if (Math.floorMod(millis, DayMillis) == 0L)
          LocalDate.ofEpochDay(Math.floorDiv(millis, DayMillis))
        else null
      case SQLTypes.Time =>
        if (millis >= 0L && millis < DayMillis) LocalTime.ofNanoOfDay(millis * 1000000L) else null
      case _ => Instant.ofEpochMilli(millis).atZone(ZoneOffset.UTC)
    }

  // ---------------------------------------------------------------------------------------------
  // Metrics (B3, B4)
  // ---------------------------------------------------------------------------------------------

  /** The reader of a metric's `value` of type `t` -- EXACT: a number becomes the class only where
    * the class holds it unchanged, else [[Unread]] (kept as stored, A-12); `null` for a type read
    * as it comes.
    */
  private def metricReader(t: SetOperationType): JsonNode => Any =
    t.sqlType match {
      case SQLTypes.TinyInt => wholeMetric(Byte.MinValue.toLong, Byte.MaxValue.toLong, int = true)
      case SQLTypes.SmallInt =>
        wholeMetric(Short.MinValue.toLong, Short.MaxValue.toLong, int = true)
      case SQLTypes.Int    => wholeMetric(Int.MinValue.toLong, Int.MaxValue.toLong, int = true)
      case SQLTypes.BigInt => wholeMetric(Long.MinValue, Long.MaxValue, int = false)
      case SQLTypes.Double =>
        node =>
          if (node.isFloatingPointNumber) {
            val d = node.doubleValue()
            if (finite(d)) java.lang.Double.valueOf(d) else Unread
          } else if (node.isIntegralNumber && node.canConvertToLong) {
            val l = node.longValue()
            val d = l.toDouble
            if (d.toLong == l && d != 9.223372036854775807e18) java.lang.Double.valueOf(d)
            else Unread
          } else Unread
      case SQLTypes.Real =>
        node =>
          if (node.isNumber && finite(node.doubleValue()))
            java.lang.Float.valueOf(node.floatValue())
          else Unread
      case SQLTypes.Boolean =>
        node =>
          if (node.isBoolean) java.lang.Boolean.valueOf(node.booleanValue())
          else if (node.isNumber) {
            val d = node.doubleValue()
            if (d == 1d) java.lang.Boolean.TRUE
            else if (d == 0d) java.lang.Boolean.FALSE
            else Unread
          } else Unread
      case kind @ (SQLTypes.Date | SQLTypes.Timestamp | SQLTypes.Time) =>
        // epoch milliseconds, never `value_as_string`: a metric truncates to its class
        node =>
          if (node.isNumber && finite(node.doubleValue())) {
            val d = node.doubleValue()
            if (d < -9.223372036854775808e18 || d >= 9.223372036854775808e18) Unread
            else {
              val instant =
                Instant.ofEpochMilli(if (node.isIntegralNumber) node.longValue() else d.toLong)
              kind match {
                case SQLTypes.Date => instant.atZone(ZoneOffset.UTC).toLocalDate
                case SQLTypes.Time => instant.atZone(ZoneOffset.UTC).toLocalTime
                case _             => instant.atZone(ZoneOffset.UTC)
              }
            }
          } else Unread
      case SQLTypes.Varchar | SQLTypes.Keyword | SQLTypes.Text | SQLTypes.Char =>
        node => if (node.isTextual) node.asText() else Unread
      case _ => null
    }

  private def wholeMetric(min: Long, max: Long, int: Boolean): JsonNode => Any =
    node => {
      val whole = wholeOf(node, min, max, text = false)
      if (whole eq null) Unread
      else if (int) java.lang.Integer.valueOf(whole.intValue())
      else whole
    }

  private def finite(d: Double): Boolean = !d.isNaN && !d.isInfinite

  /** `node` as a whole number within `[min, max]` -- an integral number, a finite floating-point
    * number with no fraction, or (`text`) a text spelling one --, `null` otherwise: never rounded,
    * never wrapped.
    */
  private[client] def wholeOf(node: JsonNode, min: Long, max: Long, text: Boolean): java.lang.Long =
    if (node.isIntegralNumber) {
      if (node.canConvertToLong) {
        val v = node.longValue()
        if (v >= min && v <= max) java.lang.Long.valueOf(v) else null
      } else null
    } else if (node.isFloatingPointNumber) {
      val d = node.doubleValue()
      if (
        !finite(d) || d != Math.rint(
          d
        ) || d < -9.223372036854775808e18 || d >= 9.223372036854775808e18
      )
        null
      else {
        val v = d.toLong
        if (v >= min && v <= max) java.lang.Long.valueOf(v) else null
      }
    } else if (text && node.isTextual) {
      val parsed =
        try new java.math.BigDecimal(node.asText())
        catch { case _: NumberFormatException => null }
      if (parsed eq null) null
      else {
        val whole =
          try parsed.toBigIntegerExact
          catch { case _: ArithmeticException => null }
        if ((whole eq null) || whole.bitLength() > 63) null
        else {
          val v = whole.longValue()
          if (v >= min && v <= max) java.lang.Long.valueOf(v) else null
        }
      }
    } else null
}

/** SCHEMA-2b (B8) -- the partition key of ONE side of a window statement's join: ONE function per
  * statement and side, the names of today's derivation hoisted, each value through its key function
  * -- a `TIMESTAMP` / `TIME` value truncated to the millisecond under [[SchemaColumns]] (doc
  * values, a bucket's `key` and a `date_nanos` aggregation are milliseconds, a `_source` may hold
  * more), every other value as it is. Each name is read under its row key: the name itself, but on
  * the base side of a partition column paired by instant ([[PartitionKeys.instantColumns]]), the
  * key its `epoch_millis` doc value is read under ([[PartitionKeys.instantKey]]).
  */
private[client] final class PartitionKeys(
  names: Array[String],
  rowKeys: Array[String],
  keyOf: Array[Any => Any]
) {

  /** The partition key of `row`: `__global__` without PARTITION BY; a name `row` lacks left out. */
  def of(row: ListMap[String, Any]): ListMap[String, Any] =
    if (names.isEmpty) PartitionKeys.Global
    else {
      val builder = ListMap.newBuilder[String, Any]
      var i = 0
      while (i < names.length) {
        row.get(rowKeys(i)) match {
          case Some(v) =>
            val keyFunction = keyOf(i)
            builder += names(i) -> (if (keyFunction eq null) v else keyFunction(v))
          case None =>
        }
        i += 1
      }
      builder.result()
    }
}

private[client] object PartitionKeys {

  /** The one partition of a window statement with no PARTITION BY. */
  val Global: ListMap[String, Any] = ListMap("__global__" -> true)

  private val toMillis: Any => Any = {
    case z: ZonedDateTime => z.truncatedTo(ChronoUnit.MILLIS)
    case t: LocalTime     => t.truncatedTo(ChronoUnit.MILLIS)
    case l: LocalDateTime => l.truncatedTo(ChronoUnit.MILLIS)
    case other            => other
  }

  /** The partition keys of `request`'s rows read in `context`: the window aggregation's (cache
    * side) or the base query's (base side).
    */
  def of(request: SingleSearch, context: ConversionContext): PartitionKeys = {
    val partitions = request.windowFunctions.flatMap(wf => wf.partitionBy.zip(wf.buckets))
    val names = partitions.map(_._1.aliasOrName).distinct.toArray
    // SCHEMA-6 (A-28) -- the base side of a selected ARRAY partition column: its value as read
    val asRead: java.util.HashMap[String, String] = context match {
      case s: SchemaColumns => s.reading.asReadKeys
      case _                => null
    }
    val instants: java.util.HashMap[String, String] = context match {
      case s: SchemaColumns => s.reading.instants
      case _                => null
    }
    // the base side of an instant column reads its doc value's key; the cache side, whose reading
    // holds no instant, reads the name, under which its key reader put the bucket's instant
    val rowKeys: Array[String] =
      if (((instants eq null) || instants.isEmpty) && ((asRead eq null) || asRead.isEmpty)) names
      else
        names.map { name =>
          val key = if (instants eq null) null else instants.get(name)
          val asReadKey = if ((key ne null) || (asRead eq null)) null else asRead.get(name)
          if (key ne null) key else if (asReadKey ne null) asReadKey else name
        }
    val keyOf = new Array[Any => Any](names.length)
    if (context.isInstanceOf[SchemaColumns]) {
      var i = 0
      while (i < names.length) {
        val name = names(i)
        if (rowKeys(i) eq name)
          partitions.find(_._1.aliasOrName == name).foreach { case (_, bucket) =>
            MultiSearch.partitionType(bucket).sqlType match {
              case SQLTypes.Timestamp | SQLTypes.Time => keyOf(i) = toMillis
              case _                                  =>
            }
          }
        i += 1
      }
    }
    new PartitionKeys(names, rowKeys, keyOf)
  }

  // ---------------------------------------------------------------------------------------------
  // Partition columns paired by instant
  // ---------------------------------------------------------------------------------------------

  /** The PARTITION BY columns of the window statement `request` paired by INSTANT: each plain
    * column whose key is a date read AS STORED (`DateRoute.AsStored`: core does not read every
    * value of its format as the Elasticsearch major does). Its base row holds the stored text and
    * its bucket's `key_as_string` is the field's format applied to the bucket's instant at UTC: two
    * spellings of one instant (`2024-01-31 11:30 CET` stored, `2024-01-31 10:30 Z` from
    * Elasticsearch 8.18.3), which never paired. Both sides pair instead by the instant
    * Elasticsearch supplies -- the bucket's millisecond `key` (cache side,
    * `AggregateReading.of(instants)`) and the column's `epoch_millis` doc value (base side,
    * [[withInstantDocValues]], `HitReading.of(instants)`) --, as Elasticsearch partitions a date;
    * core never parses the text. The displayed value stays the stored text.
    *
    * A column the base statement COMPUTES under the same name (`CONCAT(pz, '') AS pz`) is not
    * paired by instant: its reader reads the hit's `fields` entry of that name, so a doc value
    * asked there would be read as the item's value. Such an item keeps the window's partition
    * bucket, so no script field is sent for it either (Elasticsearch 8.18.3): both sides read it as
    * 0.24.0 did, and the column and its window values read NULL, as on 0.24.0.
    *
    * ONE function for both sides, its set taken as it is by each (`SchemaColumns.ofWindowBase`: the
    * base reading and the base request's `docvalue_fields`; `SchemaColumns.ofWindowAggregation`:
    * the bucket keys), decided per statement from the declared types, the mapping formats, the
    * major (`dates`) and the base statement's projection.
    */
  def instantColumns(request: SingleSearch, dates: SetOperationValues.DateReading): Set[String] =
    if (request.schema.isEmpty) Set.empty
    else {
      val candidates = request.windowFunctions
        .flatMap(wf => wf.partitionBy.zip(wf.buckets))
        .collect {
          case (id, bucket)
              if id.aliasOrName == bucket.identifier.name && byInstant(bucket, dates) =>
            id.aliasOrName
        }
        .toSet
      if (candidates.isEmpty) candidates
      else {
        // the output names the base statement computes: `SingleSearch.withoutWindows`'s SELECT --
        // this statement's (updated) items but its windows, `__cN` numbered without them -- the
        // projection its reading reads computed columns from (`HitReading.isComputed`)
        val computed = request.select
          .copy(fields = request.select.fields.filterNot(_.identifier.hasWindow))
          .fieldsWithComputedAliases
          .filter(HitReading.isComputed)
          .map(_.outputName)
          .toSet
        candidates.filterNot(computed.contains)
      }
    }

  /** A plain date column whose key is read as stored (see [[instantColumns]]). */
  private def byInstant(bucket: Bucket, dates: SetOperationValues.DateReading): Boolean = {
    val id = bucket.identifier
    val name = id.name
    val plain =
      !id.nested && id.functions.isEmpty && name.nonEmpty && name.indexOf('.') < 0 &&
      bucket.name == name
    plain && {
      val t = MultiSearch.partitionType(bucket)
      t.sqlType match {
        case SQLTypes.Date | SQLTypes.Timestamp | SQLTypes.Time =>
          DateRoute.of(t.sqlType, t.format, dates).isInstanceOf[DateRoute.AsStored]
        case _ => false
      }
    }
  }

  /** The base row key the doc value of the instant column `name` is read under: led by a NUL, so no
    * column's name, and dropped by the final projection.
    */
  private[client] def instantKey(name: String): String = "\u0000instant:" + name

  /** SCHEMA-6 (A-28) -- the base row key a selected `ARRAY` partition column's value is read under
    * AS READ (0.24.0's reading, the one its window aggregation key has), beside the column's typed
    * value: led by a NUL, so no column's name, and dropped by the final projection.
    */
  private[client] def asReadKey(name: String): String = "\u0000asread:" + name

  /** The instant of a base row's `epoch_millis` doc value (a one-element array of its text): the
    * epoch milliseconds' decimal text, the spelling of [[AggregateReading]] 's instant key -- any
    * sub-millisecond fraction (a `date_nanos` value) left out, as its aggregation buckets it. No
    * parse: Elasticsearch formats the number. A document holding several values keeps them all and
    * pairs with no bucket (NULL, as on main).
    */
  private[client] def instantOf(node: JsonNode): Any =
    if (node.isArray) {
      if (node.size() == 1) millisText(node.get(0))
      else {
        val values = List.newBuilder[String]
        val it = node.elements()
        while (it.hasNext) values += millisText(it.next())
        values.result()
      }
    } else millisText(node)

  private def millisText(node: JsonNode): String = {
    val text = node.asText()
    val dot = text.indexOf('.')
    if (dot < 0) text else text.substring(0, dot)
  }

  /** `query` (a search body) also asking each column of `fields` as its `epoch_millis` doc value
    * (`docvalue_fields`, accepted with a `format` from Elasticsearch 6.4 on, through 9): the base
    * query of a window statement whose `fields` pair by instant. Built once per statement; `query`
    * itself when `fields` is empty.
    */
  private[client] def withInstantDocValues(query: String, fields: Seq[String]): String =
    baseRequest(query, fields, Nil)

  /** The `_source` paths the base request of a COMPUTED-ONLY window statement asks (D4): the
    * distinct, non-empty name of every PARTITION BY column -- with or without a function, a dotted
    * path kept --, whether a schema is attached or not. Such a base fetches no `_source`
    * (`SingleSearch.sourceless`), yet its rows read their partition keys from it (B10): asked here,
    * the `_source` subtree each key reads is the one the whole document gave, so every key answers
    * as it did, with a narrower payload. Its caller asks it only for a sourceless base: a base with
    * a plain column keeps its own includes.
    */
  private[client] def sourceColumns(request: SingleSearch): Seq[String] =
    request.windowFunctions
      .flatMap(_.partitionBy)
      .map(_.name)
      .filter(_.nonEmpty)
      .distinct

  /** `query` (a window statement's base search body) asking each column of `instants` as its
    * `epoch_millis` doc value ([[withInstantDocValues]]) and, when `includes` is not empty, the
    * `_source` of those paths only (`{"includes": …}`, in place of the `false` a sourceless base is
    * sent with, [[sourceColumns]]): ONE parse for both. `query` itself when both are empty. Built
    * once per statement.
    */
  private[client] def baseRequest(
    query: String,
    instants: Seq[String],
    includes: Seq[String]
  ): String =
    if (instants.isEmpty && includes.isEmpty) query
    else
      JacksonConfig.objectMapper.readTree(query) match {
        case root: ObjectNode =>
          if (instants.nonEmpty) {
            val docValues = root.get("docvalue_fields") match {
              case existing: ArrayNode => existing
              case _                   => root.putArray("docvalue_fields")
            }
            instants.foreach { field =>
              docValues.addObject().put("field", field).put("format", "epoch_millis")
              ()
            }
          }
          if (includes.nonEmpty) {
            val paths = root.putObject("_source").putArray("includes")
            includes.foreach(path => paths.add(path))
          }
          JacksonConfig.objectMapper.writeValueAsString(root)
        case _ => query
      }
}
