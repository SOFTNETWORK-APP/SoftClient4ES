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

import app.softnetwork.elastic.schema.Index
import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query.SingleSearch
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.slf4j.{Logger, LoggerFactory}

import java.time.{LocalDate, LocalTime, ZoneOffset, ZonedDateTime}
import scala.collection.immutable.ListMap

/** SCHEMA-2b (B7 / B8, A-23) -- both sides of a window statement's join read a partition column by
  * ONE rule: the base row's value (`SchemaColumns.ofWindowBase`) and the window aggregation's key
  * (`SchemaColumns.ofWindowAggregation`) give equal partition keys, with equal hashes, for every
  * partition column type of fixture `s2b_w`; a DATE value that is not exactly a day pairs as on
  * main and shares no key with a day; a date read as stored pairs by instant. Network-free (canned
  * pages).
  */
class WindowPartitionKeySpec extends AnyFlatSpec with Matchers {

  private val version = "8.18.3"

  private val properties =
    """{"id": {"type": "keyword"}, "n": {"type": "integer"},
      | "pk1": {"type": "keyword"}, "pk2": {"type": "keyword"}, "pu": {"type": "keyword"},
      | "pt": {"type": "date"}, "pb": {"type": "boolean"},
      | "pd": {"type": "date", "format": "epoch_second"}, "pe": {"type": "date"},
      | "pz": {"type": "date", "format": "yyyy-MM-dd HH:mm z"}, "pn": {"type": "integer"},
      | "tm": {"type": "date"}}""".stripMargin

  private val schema =
    Index(
      "s2b_w",
      s"""{"s2b_w": {"aliases": {}, "mappings": {"_meta": {"columns": {
         |   "pd": {"data_type": "DATE"}, "pe": {"data_type": "DATE"}, "tm": {"data_type": "TIME"}}},
         | "properties": $properties}, "settings": {"index": {"number_of_shards": "1"}}}}""".stripMargin
    ).schema

  private def statement(sql: String): SingleSearch = Parser(sql) match {
    case Right(s: SingleSearch) => s.update(Some(schema))
    case other                  => fail(s"[$sql] expected a SingleSearch, got $other")
  }

  private def windowStatement(p: String): SingleSearch =
    statement(
      s"SELECT id, n, $p, ROW_NUMBER() OVER (PARTITION BY $p ORDER BY id) AS rn, " +
      s"SUM(n) OVER (PARTITION BY $p) AS s FROM s2b_w"
    )

  /** The window aggregation request, as `SearchApi.buildWindowAggregationRequest` derives it. */
  private def aggregationRequest(request: SingleSearch): SingleSearch =
    request
      .copy(
        select = request.select.copy(fields = request.windowFields.map(_.update(request))),
        groupBy = None,
        orderBy = None,
        limit = None
      )
      .update()

  private def clientAggregations(single: SingleSearch): ListMap[String, ClientAggregation] =
    ListMap(single.sqlAggregations.toSeq.map { case (k, v) =>
      k -> sqlAggregationToClientAggregation(v)
    }: _*)

  private def outputs(single: SingleSearch): Seq[String] =
    single.select.fieldsWithComputedAliases.map(_.outputName)

  /** The base row `_source` (and the hit's `fields`, when given) reads as, in `context`. */
  private def baseRow(
    request: SingleSearch,
    source: String,
    context: ConversionContext = NativeContext,
    fields: Option[String] = None
  ): ListMap[String, Any] = {
    val base = request.withoutWindows
    val ctx = SchemaColumns.ofWindowBase(request, base, context, Some(version))
    ElasticConversion
      .parseResponse(
        s"""{"hits": {"total": 1, "hits": [{"_id": "1", "_source": $source""" +
        fields.fold("")(f => s""", "fields": $f""") + "}]}}",
        base.fieldAliases,
        ListMap.empty,
        outputs(base),
        retainDocumentId = true
      )(ctx)
      .get
      .head
  }

  /** The base side: the partition key of the base row `_source` (and `fields`) reads as. */
  private def baseKey(
    request: SingleSearch,
    source: String,
    context: ConversionContext = NativeContext,
    fields: Option[String] = None
  ): ListMap[String, Any] = {
    val ctx = SchemaColumns.ofWindowBase(request, request.withoutWindows, context, Some(version))
    PartitionKeys.of(request, ctx).of(baseRow(request, source, context, fields))
  }

  /** The cache side: the partition key of the window aggregation's bucket `bucket` of level `p`. */
  private def cacheKey(
    request: SingleSearch,
    p: String,
    bucket: String,
    context: ConversionContext = NativeContext
  ): ListMap[String, Any] = {
    val aggRequest = aggregationRequest(request)
    val ctx = SchemaColumns.ofWindowAggregation(request, aggRequest, context, Some(version))
    val row = ElasticConversion
      .parseResponse(
        s"""{"hits": {"total": 1, "hits": []}, "aggregations": {"$p": {"buckets": [$bucket]}}}""",
        aggRequest.fieldAliases,
        clientAggregations(aggRequest),
        outputs(aggRequest)
      )(ctx)
      .get
      .head
    PartitionKeys.of(request, ctx).of(row)
  }

  private def samePartition(a: ListMap[String, Any], b: ListMap[String, Any]): Unit = {
    a shouldBe b
    a.hashCode shouldBe b.hashCode
  }

  private def utc(y: Int, mo: Int, d: Int, h: Int = 0, mi: Int = 0, ms: Int = 0): ZonedDateTime =
    ZonedDateTime.of(y, mo, d, h, mi, 0, ms * 1000000, ZoneOffset.UTC)

  behavior of "a window partition key"

  it should "pair a keyword spelled as a time or a date by its text (pk1, pk2)" in {
    val r1 = windowStatement("pk1")
    val k1 = baseKey(r1, """{"id": "1", "n": 1, "pk1": "10:30"}""")
    k1 shouldBe ListMap("pk1" -> "10:30")
    samePartition(
      k1,
      cacheKey(r1, "pk1", """{"key": "10:30", "doc_count": 1, "s": {"value": 1.0}}""")
    )
    val r2 = windowStatement("pk2")
    samePartition(
      baseKey(r2, """{"id": "1", "n": 1, "pk2": "2024-01-31"}"""),
      cacheKey(r2, "pk2", """{"key": "2024-01-31", "doc_count": 1, "s": {"value": 1.0}}""")
    )
  }

  it should "pair a BOOLEAN, an INT and a TIMESTAMP with sub-millisecond text (pb, pn, pt)" in {
    val rb = windowStatement("pb")
    val kb = baseKey(rb, """{"id": "1", "n": 1, "pb": true}""")
    kb shouldBe ListMap("pb" -> java.lang.Boolean.TRUE)
    samePartition(
      kb,
      cacheKey(
        rb,
        "pb",
        """{"key": 1, "key_as_string": "true", "doc_count": 1, "s": {"value": 1.0}}"""
      )
    )
    val ri = windowStatement("pn")
    samePartition(
      baseKey(ri, """{"id": "1", "n": 1, "pn": 7}"""),
      cacheKey(ri, "pn", """{"key": 7, "doc_count": 1, "s": {"value": 1.0}}""")
    )
    val rt = windowStatement("pt")
    val kt = baseKey(rt, """{"id": "1", "n": 1, "pt": "2024-01-31T10:30:00.123456Z"}""")
    kt shouldBe ListMap("pt" -> utc(2024, 1, 31, 10, 30, 123))
    samePartition(
      kt,
      cacheKey(
        rt,
        "pt",
        """{"key": 1706697000123, "key_as_string": "2024-01-31T10:30:00.123Z", "doc_count": 1,
          | "s": {"value": 1.0}}""".stripMargin
      )
    )
  }

  it should "pair a DATE exactly a day as a LocalDate, and a DATE not a day as main does (pd, pe)" in {
    val rd = windowStatement("pd")
    val day = baseKey(rd, """{"id": "1", "n": 1, "pd": 1706659200}""")
    day shouldBe ListMap("pd" -> LocalDate.of(2024, 1, 31))
    samePartition(
      day,
      cacheKey(
        rd,
        "pd",
        """{"key": 1706659200000, "key_as_string": "1706659200", "doc_count": 1, "s": {"value": 1.0}}"""
      )
    )
    // 10:30 and 14:00: none pairs (as on main), and neither shares the day's key
    Seq(1706697000L -> "1706697000", 1706709600L -> "1706709600").foreach { case (seconds, text) =>
      val b = baseKey(rd, s"""{"id": "1", "n": 1, "pd": $seconds}""")
      val c = cacheKey(
        rd,
        "pd",
        s"""{"key": ${seconds * 1000}, "key_as_string": "$text", "doc_count": 1, "s": {"value": 1.0}}"""
      )
      b should not be c
      b should not be day
      c should not be day
    }
    val re = windowStatement("pe")
    samePartition(
      baseKey(re, """{"id": "1", "n": 1, "pe": "2024-01-31T00:00:00Z"}"""),
      cacheKey(
        re,
        "pe",
        """{"key": 1706659200000, "key_as_string": "2024-01-31T00:00:00.000Z", "doc_count": 1,
          | "s": {"value": 1.0}}""".stripMargin
      )
    )
    // 10:30: main pairs it (equal ZonedDateTimes), and so does the branch
    val inexact = baseKey(re, """{"id": "1", "n": 1, "pe": "2024-01-31T10:30:00Z"}""")
    samePartition(
      inexact,
      cacheKey(
        re,
        "pe",
        """{"key": 1706697000000, "key_as_string": "2024-01-31T10:30:00.000Z", "doc_count": 1,
          | "s": {"value": 1.0}}""".stripMargin
      )
    )
    inexact should not be ListMap("pe" -> LocalDate.of(2024, 1, 31))
  }

  it should "pair a date read as stored (pz) by instant, as Elasticsearch buckets a date" in {
    // 2024-01-31 10:30 UTC = 11:30 CET (CET = UTC+1 in January) = 1706697000000 ms: one instant,
    // so one bucket, whose `key_as_string` Elasticsearch 8.18.3 spells `2024-01-31 10:30 Z` (its
    // own DateFormatter / DocValueFormat run on the mapping's format) -- neither stored spelling.
    // Both sides pair by the instant Elasticsearch supplies: the bucket's millisecond `key` and the
    // column's `epoch_millis` doc value (the same formatters print `1706697000000` for both docs).
    val rz = windowStatement("pz")
    val bucket =
      """{"key": 1706697000000, "key_as_string": "2024-01-31 10:30 Z", "doc_count": 2,
        | "s": {"value": 3.0}}""".stripMargin
    val instant = Some("""{"pz": ["1706697000000"]}""")
    val utcDoc =
      baseKey(rz, """{"id": "1", "n": 1, "pz": "2024-01-31 10:30 UTC"}""", fields = instant)
    val cetDoc =
      baseKey(rz, """{"id": "2", "n": 2, "pz": "2024-01-31 11:30 CET"}""", fields = instant)
    utcDoc shouldBe ListMap("pz" -> "1706697000000")
    samePartition(utcDoc, cacheKey(rz, "pz", bucket))
    samePartition(cetDoc, cacheKey(rz, "pz", bucket))
    // another instant is another partition
    val w3 = baseKey(
      rz,
      """{"id": "3", "n": 4, "pz": "2024-02-01 00:00 UTC"}""",
      fields = Some("""{"pz": ["1706745600000"]}""")
    )
    w3 should not be utcDoc
    samePartition(
      w3,
      cacheKey(
        rz,
        "pz",
        """{"key": 1706745600000, "key_as_string": "2024-02-01 00:00 Z", "doc_count": 1,
          | "s": {"value": 4.0}}""".stripMargin
      )
    )
    // the displayed value stays the stored text; the instant has its own row key
    val row = baseRow(rz, """{"id": "2", "n": 2, "pz": "2024-01-31 11:30 CET"}""", fields = instant)
    row("pz") shouldBe "2024-01-31 11:30 CET"
    row(PartitionKeys.instantKey("pz")) shouldBe "1706697000000"
    // a document without the doc value (no `pz`) pairs with no bucket, as on main
    baseKey(rz, """{"id": "4", "n": 8}""") shouldBe ListMap.empty
  }

  it should "pair a TIME of day (tm)" in {
    val rm = windowStatement("tm")
    val time = baseKey(rm, """{"id": "1", "n": 1, "tm": "1970-01-01T10:30:00Z"}""")
    time shouldBe ListMap("tm" -> LocalTime.of(10, 30))
    samePartition(
      time,
      cacheKey(
        rm,
        "tm",
        """{"key": 37800000, "key_as_string": "1970-01-01T10:30:00.000Z", "doc_count": 1,
          | "s": {"value": 1.0}}""".stripMargin
      )
    )
  }

  it should "keep today's reading on both sides under EntityContext (a mode-W subquery)" in {
    val rb = windowStatement("pb")
    val base = baseKey(rb, """{"id": "1", "n": 1, "pb": true}""", EntityContext)
    val cache = cacheKey(
      rb,
      "pb",
      """{"key": 1, "key_as_string": "true", "doc_count": 1, "s": {"value": 1.0}}""",
      EntityContext
    )
    base shouldBe ListMap("pb" -> true)
    // the key as 0.24.0 read it: `key_as_string` first
    cache shouldBe ListMap("pb" -> "true")
  }

  it should "carry an unselected partition column through the base projection when it is fetched" in {
    val request = statement(
      "SELECT UPPER(id) AS u, SUM(n) OVER (PARTITION BY pu) AS s FROM s2b_w"
    )
    baseKey(request, """{"id": "a", "n": 1, "pu": "x"}""") shouldBe ListMap("pu" -> "x")
  }

  behavior of "the final projection of a window statement (B7 d)"

  private val client = new NopeClientApi {
    override protected def logger: Logger = LoggerFactory.getLogger(classOf[WindowPartitionKeySpec])
  }

  it should "drop the carried partition columns and `_id` in its one rebuild" in {
    val request = statement("SELECT id, SUM(n) OVER (PARTITION BY pu) AS s FROM s2b_w")
    val base = request.withoutWindows
    val ctx = SchemaColumns.ofWindowBase(request, base, NativeContext, Some(version))
    val project = client.windowRowProjection(request, ctx)
    project(ListMap("id" -> "a", "_id" -> "1", "pu" -> "x", "s" -> 3L)) shouldBe
    ListMap("id" -> "a", "s" -> 3L)
    // under EntityContext: today's normalization, then `_id` stripped
    val legacy = client.windowRowProjection(request, EntityContext)
    legacy(ListMap("id" -> "a", "_id" -> "1", "pu" -> "x", "s" -> 3L)) shouldBe
    ListMap("id" -> "a", "s" -> 3L, "pu" -> "x")
  }

  it should "drop the doc value of a partition column paired by instant, strict or under `*`" in {
    val instant = PartitionKeys.instantKey("pz")
    val strict = statement("SELECT id, SUM(n) OVER (PARTITION BY pz) AS s FROM s2b_w")
    val strictCtx =
      SchemaColumns.ofWindowBase(strict, strict.withoutWindows, NativeContext, Some(version))
    client.windowRowProjection(strict, strictCtx)(
      ListMap("id" -> "a", "_id" -> "1", "pz" -> "2024-01-31 10:30 UTC", instant -> "1", "s" -> 3L)
    ) shouldBe ListMap("id" -> "a", "s" -> 3L)
    val star = statement("SELECT *, SUM(n) OVER (PARTITION BY pz) AS s FROM s2b_w")
    val starCtx =
      SchemaColumns.ofWindowBase(star, star.withoutWindows, NativeContext, Some(version))
    val projected = client.windowRowProjection(star, starCtx)(
      ListMap("id" -> "a", "_id" -> "1", "pz" -> "2024-01-31 10:30 UTC", instant -> "1", "s" -> 3L)
    )
    projected("pz") shouldBe "2024-01-31 10:30 UTC"
    projected.keySet should not contain instant
  }

  behavior of "the base query of a window statement"

  private val body = """{"query": {"match_all": {}}, "_source": {"includes": ["id"]}}"""

  private def baseQuery(request: SingleSearch, context: ConversionContext): String = {
    val base = request.withoutWindows
    val ctx = SchemaColumns.ofWindowBase(request, base, context, Some(version))
    client
      .windowBaseQuery(ElasticQuery(body, Seq("s2b_w")), ctx)
      .query
  }

  it should "ask the epoch_millis doc value of a partition column paired by instant only" in {
    val json = new com.fasterxml.jackson.databind.ObjectMapper()
    json.readTree(baseQuery(windowStatement("pz"), NativeContext)) shouldBe json.readTree(
      """{"query": {"match_all": {}}, "_source": {"includes": ["id"]},
        | "docvalue_fields": [{"field": "pz", "format": "epoch_millis"}]}""".stripMargin
    )
    // every other partition kind keeps its request, byte for byte
    Seq("pk1", "pt", "pb", "pd", "pe", "pn", "tm").foreach { p =>
      withClue(s"$p ") {
        baseQuery(windowStatement(p), NativeContext) shouldBe
        """{"query": {"match_all": {}}, "_source": {"includes": ["id"]}}"""
      }
    }
    // a mode-W subquery keeps today's reading on both sides, and its request
    baseQuery(windowStatement("pz"), EntityContext) shouldBe
    """{"query": {"match_all": {}}, "_source": {"includes": ["id"]}}"""
    // a body that already asks doc values keeps them
    PartitionKeys.withInstantDocValues(
      """{"docvalue_fields": [{"field": "x"}]}""",
      Seq("pz")
    ) shouldBe """{"docvalue_fields":[{"field":"x"},{"field":"pz","format":"epoch_millis"}]}"""
  }

  // D4 -- a computed-only base fetches no `_source` (`SingleSearch.sourceless`), yet its rows read
  // their partition keys from it (B10): its request asks the `_source` of those columns only, in
  // the parse that asks the instants; a base with a plain column keeps its includes (an unselected
  // partition column stays NULL there, B10, recorded), a global window keeps `_source: false`
  it should "ask the _source of its partition columns for a computed-only base, in one parse" in {
    val json = new com.fasterxml.jackson.databind.ObjectMapper()
    // the body both bridges send for a sourceless base (`fetchSource false`, es6 and es8 alike)
    val sourceless = """{"query":{"match_all":{}},"_source":false}"""
    def rewritten(sql: String): (Seq[String], String) = {
      val request = statement(sql)
      val base = request.withoutWindows
      val includes = client.windowBaseSourceIncludes(request, base)
      val ctx = SchemaColumns.ofWindowBase(request, base, NativeContext, Some(version))
      (
        includes,
        client.windowBaseQuery(ElasticQuery(sourceless, Seq("s2b_w")), ctx, includes).query
      )
    }
    // plain, unselected: its partition column
    val (pu, puBody) =
      rewritten("SELECT UPPER(id) AS u, SUM(n) OVER (PARTITION BY pu) AS s FROM s2b_w")
    pu shouldBe Seq("pu")
    json.readTree(puBody) shouldBe json.readTree(
      """{"query":{"match_all":{}},"_source":{"includes":["pu"]}}"""
    )
    // several windows, a dotted path, a function over a column: each name once
    rewritten(
      "SELECT 2 AS k, SUM(n) OVER (PARTITION BY pk1, pk2) AS s, " +
      "ROW_NUMBER() OVER (PARTITION BY pk1, obj.a ORDER BY id) AS rn, " +
      "COUNT(*) OVER (PARTITION BY UPPER(pk2)) AS c FROM s2b_w"
    )._1 shouldBe Seq("pk1", "pk2", "obj.a")
    // paired by instant: the doc value and the includes, one parse
    val (pz, pzBody) =
      rewritten("SELECT UPPER(id) AS u, SUM(n) OVER (PARTITION BY pz) AS s FROM s2b_w")
    pz shouldBe Seq("pz")
    json.readTree(pzBody) shouldBe json.readTree(
      """{"query":{"match_all":{}},"_source":{"includes":["pz"]},
        | "docvalue_fields":[{"field":"pz","format":"epoch_millis"}]}""".stripMargin
    )
    // a global window keeps `_source: false`
    val (global, globalBody) = rewritten("SELECT UPPER(id) AS u, COUNT(*) OVER () AS c FROM s2b_w")
    global shouldBe empty
    globalBody shouldBe sourceless
    // a base with a plain column, `*` or `_id` keeps its own includes
    Seq(
      "SELECT id, SUM(n) OVER (PARTITION BY pu) AS s FROM s2b_w",
      "SELECT *, SUM(n) OVER (PARTITION BY pu) AS s FROM s2b_w",
      "SELECT _id, SUM(n) OVER (PARTITION BY pu) AS s FROM s2b_w"
    ).foreach { sql =>
      withClue(s"[$sql] ") {
        val request = statement(sql)
        client.windowBaseSourceIncludes(request, request.withoutWindows) shouldBe empty
      }
    }
    // neither instants nor includes: the body itself
    PartitionKeys.baseRequest(sourceless, Nil, Nil) shouldBe sourceless
  }

  it should "pair by instant on both sides through ONE function (instantColumns)" in {
    val dates = SetOperationValues.DateReading(Some(version))
    PartitionKeys.instantColumns(windowStatement("pz"), dates) shouldBe Set("pz")
    Seq("pk1", "pk2", "pt", "pb", "pd", "pe", "pn", "tm").foreach { p =>
      withClue(s"$p ") { PartitionKeys.instantColumns(windowStatement(p), dates) shouldBe empty }
    }
  }

  it should "give both sides one rule when a computed column takes the partition column's name" in {
    val dates = SetOperationValues.DateReading(Some(version))
    val bucket =
      """{"key": 1706697000000, "key_as_string": "2024-01-31 10:30 Z", "doc_count": 2,
        | "s": {"value": 3.0}}""".stripMargin
    val source = """{"id": "1", "n": 1, "pk1": "a", "pz": "2024-01-31 10:30 UTC"}"""
    def instantsOf(request: SingleSearch): Set[String] = {
      val ctx =
        SchemaColumns.ofWindowBase(request, request.withoutWindows, NativeContext, Some(version))
      ctx match {
        case s: SchemaColumns => s.reading.instants.keySet().toArray(new Array[String](0)).toSet
        case other            => fail(s"expected SchemaColumns, got $other")
      }
    }
    // the item's reader reads the hit's `fields` entry `pz`, so a doc value asked there would be
    // read as the item's value: neither side pairs by instant, the bucket keeps its
    // `key_as_string`. The canned `fields` entry below stands for that reader; Elasticsearch
    // 8.18.3 sends none for this shape (the item keeps the window's bucket, no script field),
    // and the column reads NULL, as on 0.24.0
    Seq(
      "CONCAT(pz, '') AS pz" -> "2024-01-31T10:30Z",
      "UPPER(pk1) AS pz"     -> "A"
    ).foreach { case (item, computed) =>
      withClue(s"$item ") {
        val r = statement(s"SELECT id, n, $item, SUM(n) OVER (PARTITION BY pz) AS s FROM s2b_w")
        PartitionKeys.instantColumns(r, dates) shouldBe empty
        instantsOf(r) shouldBe empty
        baseQuery(r, NativeContext) shouldBe body
        baseKey(r, source, fields = Some(s"""{"pz": ["$computed"]}""")) shouldBe
        ListMap("pz" -> computed)
        cacheKey(r, "pz", bucket) shouldBe ListMap("pz" -> "2024-01-31 10:30 Z")
      }
    }
    // the plain column keeps pairing by instant, on both sides
    val plain = statement("SELECT id, n, pz, SUM(n) OVER (PARTITION BY pz) AS s FROM s2b_w")
    PartitionKeys.instantColumns(plain, dates) shouldBe Set("pz")
    instantsOf(plain) shouldBe Set("pz")
    val json = new com.fasterxml.jackson.databind.ObjectMapper()
    json.readTree(baseQuery(plain, NativeContext)) shouldBe json.readTree(
      """{"query": {"match_all": {}}, "_source": {"includes": ["id"]},
        | "docvalue_fields": [{"field": "pz", "format": "epoch_millis"}]}""".stripMargin
    )
    val key = baseKey(plain, source, fields = Some("""{"pz": ["1706697000000"]}"""))
    key shouldBe ListMap("pz" -> "1706697000000")
    samePartition(key, cacheKey(plain, "pz", bucket))
  }
}
