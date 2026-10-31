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
import com.fasterxml.jackson.databind.node.ObjectNode
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.{LocalDate, LocalTime, ZoneOffset, ZonedDateTime}
import scala.collection.immutable.ListMap

/** SCHEMA-2b (B1-B6) -- an aggregation response read by its statement's declared types: bucket keys
  * (B2), metrics exactly (B3 / B4), top_hits values (B5) and Case 4's global metrics (B6), on the
  * canned pages each client form answers (B13: Elasticsearch 6 / 7's raw JSON, the typed tree of
  * Elasticsearch 8 / 9 whose metrics are doubles). Network-free.
  */
class AggregateReadingSpec extends AnyFlatSpec with Matchers {

  private val version = "8.18.3"

  private val properties =
    """{"b": {"type": "boolean"}, "dc": {"type": "date", "format": "yyyy/MM/dd"},
      | "c_des": {"type": "date", "format": "epoch_second"}, "big": {"type": "long"},
      | "n": {"type": "integer"}, "k": {"type": "keyword"}, "x": {"type": "double"},
      | "r": {"type": "float"}, "pt": {"type": "date"}, "tm": {"type": "date"},
      | "id": {"type": "keyword"},
      | "children": {"type": "nested", "properties": {"name": {"type": "keyword"},
      |   "birthDate": {"type": "date"}}}}""".stripMargin

  private val schema =
    Index(
      "s2b_t",
      s"""{"s2b_t": {"aliases": {}, "mappings": {"_meta": {"columns": {
         |   "c_des": {"data_type": "DATE"}, "tm": {"data_type": "TIME"}}},
         | "properties": $properties}, "settings": {"index": {"number_of_shards": "1"}}}}""".stripMargin
    ).schema

  private def statement(sql: String): SingleSearch = Parser(sql) match {
    case Right(s: SingleSearch) => s.update(Some(schema))
    case other                  => fail(s"[$sql] expected a SingleSearch, got $other")
  }

  private def clientAggregations(single: SingleSearch): ListMap[String, ClientAggregation] =
    ListMap(single.sqlAggregations.toSeq.map { case (k, v) =>
      k -> sqlAggregationToClientAggregation(v)
    }: _*)

  private def contextOf(single: SingleSearch): ConversionContext =
    SchemaColumns.of(single, NativeContext, Some(version))

  /** The rows `json` answers for `sql`, read in the statement's schema context. */
  private def rows(sql: String, json: String): Seq[ListMap[String, Any]] = {
    val single = statement(sql)
    ElasticConversion
      .parseResponse(
        json,
        single.fieldAliases,
        clientAggregations(single),
        single.select.fieldsWithComputedAliases.map(_.outputName)
      )(contextOf(single))
      .get
  }

  private def grouped(sql: String, level: String, buckets: String): Seq[ListMap[String, Any]] =
    rows(
      sql,
      s"""{"hits": {"total": {"value": 9, "relation": "eq"}, "hits": []},
         | "aggregations": {"$level": {"buckets": [$buckets]}}}""".stripMargin
    )

  private def global(sql: String, aggregations: String, total: Int = 9): ListMap[String, Any] =
    rows(
      sql,
      s"""{"hits": {"total": {"value": $total, "relation": "eq"}, "hits": []},
         | "aggregations": {$aggregations}}""".stripMargin
    ) match {
      case Seq(one) => one
      case other    => fail(s"[$sql] answered ${other.size} rows: $other")
    }

  private def utc(y: Int, mo: Int, d: Int, h: Int = 0, mi: Int = 0): ZonedDateTime =
    ZonedDateTime.of(y, mo, d, h, mi, 0, 0, ZoneOffset.UTC)

  private def classOf(v: Any): String = if (v == null) "null" else v.getClass.getName

  // -----------------------------------------------------------------------------------------------
  // Keys (B2) -- Table A rows 20a-20f
  // -----------------------------------------------------------------------------------------------

  behavior of "a bucket key read by its level's declared type (B2)"

  it should "read a BOOLEAN key 1 / 0 as a Boolean (20a)" in {
    val rs = grouped(
      "SELECT b, COUNT(*) AS c FROM s2b_t GROUP BY b",
      "b",
      """{"key": 1, "key_as_string": "true", "doc_count": 2},
        |{"key": 0, "key_as_string": "false", "doc_count": 1}""".stripMargin
    )
    rs.map(_("b")) shouldBe Seq(java.lang.Boolean.TRUE, java.lang.Boolean.FALSE)
  }

  it should "read a custom-format date key from its epoch millis, at Z (20b)" in {
    val rs = grouped(
      "SELECT dc, COUNT(*) AS c FROM s2b_t GROUP BY dc",
      "dc",
      """{"key": 1706659200000, "key_as_string": "2024/01/31", "doc_count": 1}"""
    )
    rs.head("dc") shouldBe utc(2024, 1, 31)
    rs.head("dc").asInstanceOf[ZonedDateTime].getZone shouldBe ZoneOffset.UTC
  }

  it should "read a BIGINT key as a Long, never a 10^12 date guess (20c)" in {
    val rs = grouped(
      "SELECT big, COUNT(*) AS c FROM s2b_t GROUP BY big",
      "big",
      """{"key": 42, "doc_count": 1}, {"key": 1706697000000, "doc_count": 1}"""
    )
    rs.map(_("big")) shouldBe Seq(
      java.lang.Long.valueOf(42L),
      java.lang.Long.valueOf(1706697000000L)
    )
    rs.map(r => classOf(r("big"))).distinct shouldBe Seq("java.lang.Long")
  }

  it should "read a DATE key exactly: a whole day a LocalDate, 10:30 as main reads it (20d, A-25)" in {
    val rs = grouped(
      "SELECT c_des, COUNT(*) AS c FROM s2b_t GROUP BY c_des",
      "c_des",
      """{"key": 1706697000000, "key_as_string": "1706697000", "doc_count": 1},
        |{"key": 1706659200000, "key_as_string": "1706659200", "doc_count": 1}""".stripMargin
    )
    rs.head("c_des") shouldBe ElasticConversion.extractBucketKey(
      mapper.readTree("""{"key": 1706697000000, "key_as_string": "1706697000"}""")
    )
    rs(1)("c_des") shouldBe LocalDate.of(2024, 1, 31)
  }

  it should "read an INT key as an Integer (20e) and a keyword key as its text (20f)" in {
    grouped("SELECT n, COUNT(*) AS c FROM s2b_t GROUP BY n", "n", """{"key": 5, "doc_count": 1}""")
      .head("n") shouldBe java.lang.Integer.valueOf(5)
    grouped(
      "SELECT k, COUNT(*) AS c FROM s2b_t GROUP BY k",
      "k",
      """{"key": "10:30", "doc_count": 1}, {"key": "2024-01-31", "doc_count": 1}"""
    ).map(_("k")) shouldBe Seq("10:30", "2024-01-31")
  }

  it should "read a TIME key only on 1970-01-01 and a DOUBLE key's non-finite text as the Double" in {
    val times = grouped(
      "SELECT tm, COUNT(*) AS c FROM s2b_t GROUP BY tm",
      "tm",
      """{"key": 37800000, "key_as_string": "1970-01-01T10:30:00.000Z", "doc_count": 1},
        |{"key": 1706697000000, "key_as_string": "2024-01-31T10:30:00.000Z", "doc_count": 1}""".stripMargin
    )
    times.head("tm") shouldBe LocalTime.of(10, 30)
    // not a time of day: today's reading
    times(1)("tm") shouldBe ElasticConversion.tryParseAsDateTime("2024-01-31T10:30:00.000Z").get
    grouped(
      "SELECT x, COUNT(*) AS c FROM s2b_t GROUP BY x",
      "x",
      """{"key": 2.5, "doc_count": 1}, {"key": "Infinity", "doc_count": 1}"""
    ).map(_("x")) shouldBe Seq(java.lang.Double.valueOf(2.5), java.lang.Double.POSITIVE_INFINITY)
  }

  it should "keep a key no typed reader reads as main reads it (A-12)" in {
    grouped(
      "SELECT n, COUNT(*) AS c FROM s2b_t GROUP BY n",
      "n",
      """{"key": 3000000000, "doc_count": 1}"""
    ).head("n") shouldBe 3000000000L
  }

  // -----------------------------------------------------------------------------------------------
  // Metrics (B3 / B4) -- Table A rows 21a-21d
  // -----------------------------------------------------------------------------------------------

  behavior of "a metric read by its column's declared type, exactly (B3 / B4)"

  it should "read a temporal MAX from its epoch millis at Z, never value_as_string (21a)" in {
    val row = global(
      "SELECT MAX(dc) AS m FROM s2b_t",
      """"m": {"value": 1.7067456E12, "value_as_string": "2024/02/01"}"""
    )
    row("m") shouldBe utc(2024, 2, 1)
    row("m").asInstanceOf[ZonedDateTime].getZone shouldBe ZoneOffset.UTC
  }

  it should "read SUM over an INT as a Long (21b, A-20), MAX as an Integer (21c), COUNT as a Long (21d)" in {
    val row = global(
      "SELECT SUM(n) AS s, MAX(n) AS m, COUNT(*) AS c, COUNT(n) AS cn FROM s2b_t",
      """"s": {"value": 15.0}, "m": {"value": 10.0}, "c": {"value": 3.0}, "cn": {"value": 3}"""
    )
    row("s") shouldBe java.lang.Long.valueOf(15L)
    row("m") shouldBe java.lang.Integer.valueOf(10)
    row("c") shouldBe java.lang.Long.valueOf(3L)
    row("cn") shouldBe java.lang.Long.valueOf(3L)
    Seq("s", "m", "c", "cn").map(k => classOf(row(k))) shouldBe
    Seq("java.lang.Long", "java.lang.Integer", "java.lang.Long", "java.lang.Long")
  }

  it should "read a SUM beyond the INT range as a Long (T0-4)" in {
    global("SELECT SUM(n) AS s FROM s2b_t", """"s": {"value": 6.0E9}""")("s") shouldBe
    java.lang.Long.valueOf(6000000000L)
  }

  it should "keep a value its class does not hold unchanged as stored, never failing" in {
    // a fractional SUM (a scripted operand), a SUM beyond Long, a MAX beyond INT
    global("SELECT SUM(n) AS s FROM s2b_t", """"s": {"value": 2.5}""")("s") shouldBe 2.5
    global("SELECT SUM(n) AS s FROM s2b_t", """"s": {"value": 1.0E19}""")("s") shouldBe 1.0e19
    global("SELECT MAX(n) AS m FROM s2b_t", """"m": {"value": 3.0E9}""")("m") shouldBe 3.0e9
    // a non-finite value (the typed tree of Elasticsearch 8 holds it as a double)
    val single = statement("SELECT SUM(n) AS s FROM s2b_t")
    val json = mapper
      .readTree("""{"hits": {"total": 9, "hits": []}, "aggregations": {"s": {"value": 0.0}}}""")
    json.path("aggregations").path("s").asInstanceOf[ObjectNode].put("value", Double.NaN)
    val nan = ElasticConversion
      .parseResponseTree(json, single.fieldAliases, clientAggregations(single), Seq("s"))(
        contextOf(single)
      )
      .get
      .head("s")
    nan.asInstanceOf[Double].isNaN shouldBe true
  }

  it should "read DOUBLE / REAL / BOOLEAN metrics in their classes" in {
    val row = global(
      "SELECT MAX(x) AS mx, MIN(r) AS mr, MAX(b) AS mb, AVG(n) AS a FROM s2b_t",
      """"mx": {"value": 2.5}, "mr": {"value": 1.5}, "mb": {"value": 1.0, "value_as_string": "true"},
        | "a": {"value": 5.0}""".stripMargin
    )
    row("mx") shouldBe java.lang.Double.valueOf(2.5)
    row("mr") shouldBe java.lang.Float.valueOf(1.5f)
    row("mb") shouldBe java.lang.Boolean.TRUE
    row("a") shouldBe java.lang.Double.valueOf(5.0)
  }

  it should "answer an empty input before any reader: COUNT 0, SUM 0, MIN NULL (ES 8's 0.0 included)" in {
    val row = global(
      "SELECT COUNT(*) AS c, SUM(n) AS s, MIN(n) AS m FROM s2b_t",
      """"c": {"value": 0.0}, "s": {"value": 0.0}, "m": {"value": 0.0}""",
      total = 0
    )
    row("c") shouldBe java.lang.Long.valueOf(0L)
    row("s") shouldBe java.lang.Long.valueOf(0L)
    row("m") shouldBe (null: Any)
  }

  it should "read a temporal bucket_script from its epoch millis (MAX(dc) + 1)" in {
    val rs = grouped(
      "SELECT k, MAX(dc) + 1 AS d FROM s2b_t GROUP BY k",
      "k",
      """{"key": "a", "doc_count": 1, "max_dc": {"value": 1.7066592E12, "value_as_string": "2024/01/31"},
        | "d": {"value": 1.7067456E12}}""".stripMargin
    )
    rs.head("d") shouldBe utc(2024, 2, 1)
  }

  // -----------------------------------------------------------------------------------------------
  // top_hits (B5), Case 4 (B6)
  // -----------------------------------------------------------------------------------------------

  behavior of "a top_hits value read by its aggregated column's reader (B5)"

  it should "read FIRST / LAST and ARRAY_AGG (DISTINCT) values canonically" in {
    val row = global(
      "SELECT FIRST(pt) AS f, ARRAY_AGG(DISTINCT pt) AS a FROM s2b_t",
      """"f": {"hits": {"hits": [{"_source": {"pt": "2024-01-31"}}]}},
        |"a": {"hits": {"hits": [{"_source": {"pt": "2024-01-31"}},
        |  {"_source": {"pt": "2024-01-31T00:00:00.000Z"}}, {"_source": {"pt": "2024-02-01"}}]}}""".stripMargin
    )
    row("f") match {
      case m: scala.collection.Map[_, _] =>
        m.asInstanceOf[scala.collection.Map[String, Any]]("pt") shouldBe utc(2024, 1, 31)
      case other => fail(s"FIRST answered $other")
    }
    // the same instant spelled twice is ONE canonical value
    row("a") shouldBe List(utc(2024, 1, 31), utc(2024, 2, 1))
  }

  it should "read a qualified column's top_hits value (FIRST(p.pt) FROM t p)" in {
    val row = global(
      "SELECT FIRST(p.pt) AS f FROM s2b_t p",
      """"f": {"hits": {"hits": [{"_source": {"pt": "2024-01-31"}}]}}"""
    )
    row("f") match {
      case m: scala.collection.Map[_, _] =>
        m.asInstanceOf[scala.collection.Map[String, Any]]("pt") shouldBe utc(2024, 1, 31)
      case other => fail(s"FIRST answered $other")
    }
  }

  it should "read a nested (UNNEST) top_hits value relative to its nested object" in {
    val rs = rows(
      "SELECT ARRAY_AGG(DISTINCT child.birthDate) AS birthDates FROM s2b_t p JOIN UNNEST(p.children) AS child LIMIT 10",
      """{"hits": {"total": 9, "hits": []}, "aggregations": {"children": {"doc_count": 2,
        | "birthDates": {"hits": {"hits": [{"_source": {"birthDate": "1999-05-09"}},
        |  {"_source": {"birthDate": "2002-05-09"}}]}}}}}""".stripMargin
    )
    rs.flatMap(_.get("birthDates")).head shouldBe List(utc(1999, 5, 9), utc(2002, 5, 9))
  }

  behavior of "Case 4's global metrics (B6)"

  it should "read a global metric by type under today's key, a temporal one without value_as_string as today" in {
    val single =
      statement("SELECT id, COUNT(*) OVER () AS c, MAX(pt) OVER () AS m FROM s2b_t LIMIT 5")
    val ctx = contextOf(single)
    val aggs = clientAggregations(single)
    def read(m: String): ListMap[String, Any] =
      ElasticConversion
        .parseResponse(
          s"""{"hits": {"total": 1, "hits": [{"_id": "1", "_source": {"id": "1"}}]},
             | "aggregations": {"c": {"value": 1.0}, "m": $m}}""".stripMargin,
          single.fieldAliases,
          aggs,
          single.select.fieldsWithComputedAliases.map(_.outputName)
        )(ctx)
        .get
        .head
    val typed = read("""{"value": 1.7066592E12, "value_as_string": "2024-01-31T00:00:00.000Z"}""")
    typed("c") shouldBe java.lang.Long.valueOf(1L)
    typed("m") shouldBe utc(2024, 1, 31)
    // ES 8's missing 0.0 must not become the epoch
    read("""{"value": 0.0}""")("m") shouldBe 0.0
  }

  // -----------------------------------------------------------------------------------------------
  // The carrier (A-22)
  // -----------------------------------------------------------------------------------------------

  behavior of "the carrier (A-22)"

  it should "be built by SchemaColumns.of for a native statement with aggregations, Empty elsewhere" in {
    val single = statement("SELECT k, SUM(n) AS s FROM s2b_t GROUP BY k")
    SchemaColumns.of(single, NativeContext, Some(version)) match {
      case s: SchemaColumns =>
        s.aggregates.isEmpty shouldBe false
        s.aggregates.key("k") should not be null
        s.aggregates.metric("s") should not be null
      case other => fail(s"$other")
    }
    // a subquery's inner statement (B11), and a statement with no schema (D10)
    SchemaColumns.of(single, EntityContext, Some(version)) shouldBe EntityContext
    val unmapped = Parser("SELECT k, SUM(n) AS s FROM s2b_t GROUP BY k") match {
      case Right(s: SingleSearch) => s
      case other                  => fail(s"$other")
    }
    AggregateReading
      .of(unmapped, SetOperationValues.DateReading(Some(version)))
      .isEmpty shouldBe true
  }

  it should "leave the public parsers on 0.24.0's reading" in {
    val single = statement("SELECT b, MAX(n) AS m FROM s2b_t GROUP BY b")
    val node = mapper.readTree(
      """{"b": {"buckets": [{"key": 1, "key_as_string": "true", "doc_count": 1, "m": {"value": 10.0}}]}}"""
    )
    val parsed =
      ElasticConversion.parseAggregations(
        node,
        ListMap.empty,
        single.fieldAliases,
        clientAggregations(single)
      )
    parsed.head("b") shouldBe ElasticConversion.extractBucketKey(
      mapper.readTree("""{"key": 1, "key_as_string": "true"}""")
    )
    parsed.head("m") shouldBe 10.0
  }
}
