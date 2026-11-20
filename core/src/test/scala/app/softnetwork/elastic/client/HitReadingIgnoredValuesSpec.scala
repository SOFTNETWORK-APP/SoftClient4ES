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

import app.softnetwork.elastic.client.SetOperationValues.DateReading
import app.softnetwork.elastic.schema.{FieldCaps, Index}
import app.softnetwork.elastic.sql.`type`.{SQLTypes, ValueCoercion}
import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query.SingleSearch
import app.softnetwork.elastic.sql.schema.Schema
import app.softnetwork.elastic.sql.serialization.JacksonConfig
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.{ZoneOffset, ZonedDateTime}
import scala.collection.immutable.ListMap

/** SCHEMA-17 fix R-172 -- a value Elasticsearch ignored as malformed (the hit's `_ignored` names
  * its top-level field) reads as NULL for every kind that takes `ignore_malformed`, by the rule
  * dates already follow (SCHEMA-2 D8): one value is NULL; an array keeps each element the kind's
  * own Elasticsearch parse accepts, read by the column's reader, and answers the others NULL. A
  * keyword or a wildcard over `ignore_above`, which `_ignored` names too, keeps its text. A value
  * `_ignored` does not name keeps its reading (D6).
  */
class HitReadingIgnoredValuesSpec extends AnyFlatSpec with Matchers {

  private val mapping: String =
    """{"r172_t": {"aliases": {}, "mappings": {
      | "_meta": {"columns": {
      |  "la": {"data_type": "ARRAY<BIGINT>"}, "ia": {"data_type": "ARRAY<INT>"},
      |  "ra": {"data_type": "ARRAY<REAL>"}, "hfa": {"data_type": "ARRAY<REAL>"},
      |  "ua": {"data_type": "ARRAY<BIGINT>"}, "ipa": {"data_type": "ARRAY<KEYWORD>"},
      |  "ga": {"data_type": "ARRAY<GEO_POINT>"}, "da": {"data_type": "ARRAY<TIMESTAMP>"},
      |  "w": {"data_type": "INT"}}},
      | "properties": {
      |  "id": {"type": "keyword"}, "k": {"type": "keyword"}, "ok": {"type": "long"},
      |  "tb": {"type": "byte", "ignore_malformed": true},
      |  "sh": {"type": "short", "ignore_malformed": true},
      |  "i": {"type": "integer", "ignore_malformed": true},
      |  "l": {"type": "long", "ignore_malformed": true},
      |  "u": {"type": "unsigned_long", "ignore_malformed": true},
      |  "r": {"type": "float", "ignore_malformed": true},
      |  "hf": {"type": "half_float", "ignore_malformed": true},
      |  "x": {"type": "double", "ignore_malformed": true},
      |  "sf": {"type": "scaled_float", "scaling_factor": 100, "ignore_malformed": true},
      |  "ip": {"type": "ip", "ignore_malformed": true},
      |  "g": {"type": "geo_point", "ignore_malformed": true},
      |  "b": {"type": "boolean", "ignore_malformed": true},
      |  "dm": {"type": "date", "ignore_malformed": true},
      |  "kw": {"type": "keyword", "ignore_above": 3},
      |  "wc": {"type": "wildcard", "ignore_above": 3},
      |  "la": {"type": "long", "ignore_malformed": true},
      |  "ia": {"type": "integer", "ignore_malformed": true},
      |  "ra": {"type": "float", "ignore_malformed": true},
      |  "hfa": {"type": "half_float", "ignore_malformed": true},
      |  "ua": {"type": "unsigned_long", "ignore_malformed": true},
      |  "ipa": {"type": "ip", "ignore_malformed": true},
      |  "ga": {"type": "geo_point", "ignore_malformed": true},
      |  "da": {"type": "date", "ignore_malformed": true},
      |  "obj": {"properties": {"n": {"type": "long", "ignore_malformed": true},
      |   "i": {"type": "integer", "ignore_malformed": true}}},
      |  "w": {"type": "long"}
      | }}, "settings": {"index": {"number_of_shards": "1"}}}}""".stripMargin

  private val schema: Schema = Index("r172_t", mapping).schema

  private def statement(sql: String, table: Schema = schema): SingleSearch = Parser(sql) match {
    case Right(s: SingleSearch) => s.update(Some(table))
    case other                  => fail(s"[$sql] expected a SingleSearch, got $other")
  }

  private def outputs(single: SingleSearch): Seq[String] = {
    val fields = single.select.fieldsWithComputedAliases
    if (fields.size == 1 && fields.head.identifier.identifierName == "*") Seq.empty
    else fields.map(_.outputName)
  }

  private def contextOf(
    single: SingleSearch,
    version: String,
    multiValued: MultiValued
  ): ConversionContext = {
    val elements = ElementConversions.of(single, Some(version))
    SchemaColumns(
      HitReading.of(single, DateReading(Some(version)), multiValued, elements.keySet),
      single.doubleKeyLevels
    )(elements)
  }

  /** The rows `sql` answers for `hits` on both parse routes a client's page takes: the body text (a
    * one-shot search) and the tree (scroll, PIT and search_after pages).
    */
  private def onRoutes(
    sql: String,
    hits: String,
    version: String = "8.18.3",
    multiValued: MultiValued = HitReading.Default,
    table: Schema = schema
  ): Seq[(String, Seq[ListMap[String, Any]])] = {
    val single = statement(sql, table)
    implicit val context: ConversionContext = contextOf(single, version, multiValued)
    val response = s"""{"hits": {"total": {"value": 1, "relation": "eq"}, "hits": [$hits]}}"""
    Seq(
      "body" -> ElasticConversion
        .parseResponse(response, single.fieldAliases, ListMap.empty, outputs(single))
        .get,
      "tree" -> ElasticConversion
        .parseSingleSearchResponse(
          JacksonConfig.objectMapper.readTree(response),
          single.fieldAliases,
          ListMap.empty,
          outputs(single)
        )
        .get
    )
  }

  /** A document holding `value` in `field`, beside a keyword and a number Elasticsearch indexed,
    * its hit's `_ignored` naming `ignored`.
    */
  private def hit(field: String, value: String, ignored: Seq[String]): String = {
    val names =
      if (ignored.isEmpty) ""
      else ignored.map(n => s""""$n"""").mkString(""", "_ignored": [""", ", ", "]")
    s"""{"_id": "d1", "_index": "r172_t",
       | "_source": {"id": "d1", "k": "keep", "ok": 5, "$field": $value}$names}""".stripMargin
  }

  private def utc(y: Int, mo: Int, d: Int, h: Int = 0, mi: Int = 0): ZonedDateTime =
    ZonedDateTime.of(y, mo, d, h, mi, 0, 0, ZoneOffset.UTC)

  private val point: ListMap[String, Any] = ListMap("lat" -> 1.5, "lon" -> 2.5)

  /** One value of each kind Elasticsearch refuses -- a text it cannot parse, a number out of the
    * kind's range or not finite -- which it holds in `_source` only and names in `_ignored`.
    */
  private val refused: Seq[(String, String)] = Seq(
    "tb" -> "\"x\"",
    "tb" -> "200",
    "sh" -> "\"x\"",
    "sh" -> "40000",
    "i"  -> "\"x\"",
    "i"  -> "3000000000",
    "l"  -> "\"x\"",
    "l"  -> "100000000000000000000",
    "u"  -> "\"x\"",
    "u"  -> "-1",
    "r"  -> "\"x\"",
    "r"  -> "1e39",
    "hf" -> "\"x\"",
    "hf" -> "65520",
    "x"  -> "\"x\"",
    "x"  -> "\"1e309\"",
    "sf" -> "\"x\"",
    "sf" -> "\"1e309\"",
    "ip" -> "\"x\"",
    "g"  -> "\"not a point\"",
    "b"  -> "\"x\"",
    // the date rule, unchanged
    "dm" -> "\"not-a-date\""
  )

  "A value Elasticsearch ignored as malformed" should
  "read NULL in its column for every kind, on every venue and parse route (AC2)" in {
    refused.foreach { case (field, value) =>
      Seq(
        s"SELECT id, k, ok, $field FROM r172_t",
        "SELECT * FROM r172_t",
        s"SELECT id, k, ok, $field FROM r172_t WHERE ok > 0"
      ).foreach { sql =>
        onRoutes(sql, hit(field, value, Seq(field))).foreach { case (route, rows) =>
          withClue(s"[$route] $sql over $field = $value: ") {
            val row = rows.head
            row.getOrElse(field, null) shouldBe (null: Any)
            row.get("id") shouldBe Some("d1")
            row.get("k") shouldBe Some("keep")
            row.get("ok") shouldBe Some(5L)
          }
        }
      }
    }
  }

  it should "keep its reading where the hit's _ignored does not name it (D6)" in {
    refused.filterNot(_._1 == "dm").foreach { case (field, value) =>
      onRoutes(s"SELECT id, $field FROM r172_t", hit(field, value, Nil)).foreach {
        case (route, rows) =>
          withClue(s"[$route] $field = $value: ") {
            Option(rows.head.getOrElse(field, null)) shouldBe defined
          }
      }
    }
    // ...and a cell `_ignored` names leaves the document's other cells to their own readers
    onRoutes("SELECT id, i, ok FROM r172_t", hit("i", "\"x\"", Seq("i"))).foreach {
      case (route, rows) => withClue(s"[$route] ")(rows.head.get("ok") shouldBe Some(5L))
    }
  }

  "An ARRAY<X> cell Elasticsearch ignored some value of" should
  "keep each element the kind's own parse accepts and answer the others NULL (AC2, A-201, A-202)" in {
    val cases: Seq[(String, String, Seq[Any])] = Seq(
      ("la", """[1, "x", 3]""", Seq(1L, null, 3L)),
      ("ia", """[1, 3000000000, "y"]""", Seq(1, null, null)),
      ("ra", """[1.5, 1e39, "x"]""", Seq(1.5f, null, null)),
      ("hfa", """[65519, 65520, "x"]""", Seq(65519.0f, null, null)),
      (
        "ua",
        """[-1, 18446744073709551615, 5]""",
        Seq(null, new java.math.BigInteger("18446744073709551615"), 5L)
      ),
      (
        "ipa",
        """["10.0.0.1", "x", "::1", "2001:db8::ff00:42:8329"]""",
        Seq("10.0.0.1", null, "::1", "2001:db8::ff00:42:8329")
      ),
      ("ga", """[{"lat": 1.5, "lon": 2.5}, "not a point"]""", Seq(point, null)),
      ("da", """["2024-01-31T10:30:00Z", "bad"]""", Seq(utc(2024, 1, 31, 10, 30), null))
    )
    cases.foreach { case (field, values, expected) =>
      Seq(s"SELECT id, $field FROM r172_t", "SELECT * FROM r172_t").foreach { sql =>
        onRoutes(sql, hit(field, values, Seq(field))).foreach { case (route, rows) =>
          withClue(s"[$route] $sql over $field = $values: ") {
            rows.head.getOrElse(field, null) shouldBe expected
          }
        }
      }
    }
  }

  it should "read a scalar column's several values the same way where the statement keeps them" in {
    onRoutes(
      "SELECT id, i FROM r172_t",
      hit("i", """[1, 3000000000, "y"]""", Seq("i")),
      multiValued = MultiValued.AsRead
    ).foreach { case (route, rows) =>
      withClue(s"[$route] ")(rows.head.getOrElse("i", null) shouldBe List(1, null, null))
    }
    // one element: the value Elasticsearch ignored, NULL
    onRoutes("SELECT id, i FROM r172_t", hit("i", """["x"]""", Seq("i"))).foreach {
      case (route, rows) =>
        withClue(s"[$route] ")(rows.head.getOrElse("i", null) shouldBe (null: Any))
    }
    // several values in a scalar column still fail under the default rule, naming the ALTER (D6)
    val failure = intercept[Exception] {
      onRoutes("SELECT id, i FROM r172_t", hit("i", """[1, "x"]""", Seq("i")))
    }
    val message = Option(failure.getCause).fold(failure.getMessage)(_.getMessage)
    message should include("ALTER TABLE r172_t ALTER COLUMN i SET DATA TYPE ARRAY<INT>")
  }

  it should "judge each element by its kind's own Elasticsearch parse at the edges (A-201)" in {
    val cases: Seq[(String, String, Seq[Any])] = Seq(
      ("tb", """[127, 128, -129, "5", ""]""", Seq(127, null, null, 5, null)),
      ("sh", """[32767, 32768]""", Seq(32767, null)),
      ("i", """[2147483647, 2147483648, "7.9"]""", Seq(2147483647, null, 7)),
      ("l", """[9223372036854775807, 9223372036854775808, "1.9"]""", Seq(Long.MaxValue, null, 1L)),
      (
        "u",
        """[0, -1, 18446744073709551615, 18446744073709551616]""",
        Seq(0L, null, new java.math.BigInteger("18446744073709551615"), null)
      ),
      // a whole number only: Elasticsearch refuses a decimal part, and trims no text
      (
        "u",
        """[5.5, "5.5", 5.0, "5.0", -0.5, " 5 ", "0.0"]""",
        Seq(null, null, 5L, 5L, null, null, 0L)
      ),
      // a whole floating number >= 0 is indexed whatever its size (Elasticsearch saturates one
      // >= 2^63 to 2^63 - 1): kept, read as the BIGINT column reads it -- as stored beyond `long`
      ("u", """[1e20, "x"]""", Seq(1e20, null)),
      (
        "u",
        """[1e20, 18446744073709551616.0, -1.0, -0.0]""",
        Seq(1e20, 18446744073709551616.0, null, 0L)
      ),
      ("r", """[3.4028235e38, 3.5e38, " 2.5 "]""", Seq(Float.MaxValue, null, 2.5f)),
      ("hf", """[65504, 65519.99, 65520, -65520]""", Seq(65504.0f, 65519.99f, null, null)),
      ("x", """[2.5, "NaN", "1e309"]""", Seq(2.5, null, null)),
      ("sf", """[1.25, "Infinity"]""", Seq(1.25, null)),
      ("b", """[true, "false", "x", 1]""", Seq(true, false, null, null)),
      (
        "ip",
        """["10.0.0.1", "256.0.0.1", 123, "::ffff:10.0.0.1"]""",
        Seq("10.0.0.1", null, null, "::ffff:10.0.0.1")
      ),
      ("g", """[[2.5, 1.5], "1.5,2.5", "not a point"]""", Seq(point, point, null))
    )
    cases.foreach { case (field, values, expected) =>
      onRoutes(
        s"SELECT id, $field FROM r172_t",
        hit(field, values, Seq(field)),
        multiValued = MultiValued.AsRead
      ).foreach { case (route, rows) =>
        withClue(s"[$route] $field = $values: ")(rows.head.getOrElse(field, null) shouldBe expected)
      }
    }
  }

  "A keyword or a wildcard over ignore_above" should
  "keep its text, though the hit's _ignored names it (AC3)" in {
    Seq("kw", "wc").foreach { field =>
      Seq(s"SELECT id, $field FROM r172_t", "SELECT * FROM r172_t").foreach { sql =>
        onRoutes(sql, hit(field, "\"abcdefgh\"", Seq(field))).foreach { case (route, rows) =>
          withClue(s"[$route] $sql: ")(rows.head.getOrElse(field, null) shouldBe "abcdefgh")
        }
      }
    }
  }

  "An integer of 3000000000 and a float of 1e39 Elasticsearch ignored" should
  "read NULL, never a wrapped number through the coercions (AC4)" in {
    onRoutes("SELECT id, i FROM r172_t", hit("i", "3000000000", Seq("i"))).foreach {
      case (route, rows) =>
        val i = rows.head.getOrElse("i", null)
        withClue(s"[$route] i = $i: ") {
          ValueCoercion.coerceToInt(i) shouldBe (null: java.lang.Integer)
          i shouldBe (null: Any)
        }
    }
    onRoutes("SELECT id, r FROM r172_t", hit("r", "1e39", Seq("r"))).foreach { case (route, rows) =>
      val r = rows.head.getOrElse("r", null)
      withClue(s"[$route] r = $r: ") {
        ValueCoercion.coerceToFloat(r) shouldBe (null: java.lang.Float)
        r shouldBe (null: Any)
      }
    }
  }

  "A number outside its column's type that Elasticsearch indexed" should
  "read as stored and fail through the coercions naming it (AC7, R-177)" in {
    // `w` declared INT over a `long` field; `obj.i` an object's sub-field, which D8 does not
    // reach (A-202) though `_ignored` names it
    val w = schema.columns.find(_.name == "w").getOrElse(fail("no column w"))
    (w.dataType, w.mappingType) shouldBe (SQLTypes.Int, "long")
    Seq(
      ("SELECT id, w FROM r172_t", hit("w", "3000000000", Nil), "w"),
      (
        "SELECT id, obj.i FROM r172_t",
        hit("obj", """{"i": 3000000000}""", Seq("obj.i")),
        "obj.i"
      )
    ).foreach { case (sql, document, key) =>
      onRoutes(sql, document).foreach { case (route, rows) =>
        val v = rows.head.getOrElse(key, null)
        withClue(s"[$route] $sql: $v (${Option(v).map(_.getClass.getName).orNull}): ") {
          v shouldBe java.lang.Long.valueOf(3000000000L)
          val error = the[java.sql.SQLException] thrownBy ValueCoercion.coerceToInt(v)
          error.getMessage shouldBe "Value 3000000000 is out of range for INT"
        }
      }
    }
  }

  "Whether the hit's _ignored decides a cell" should
  "be ONE lookup, built with the plan, for every accepted kind (AC5, #292)" in {
    val dates = DateReading(Some("8.18.3"))
    val accepted = Seq("tb", "sh", "i", "l", "u", "r", "hf", "x", "sf", "ip", "g", "b", "dm") ++
      Seq("la", "ia", "ra", "hfa", "ua", "ipa", "ga", "da")
    val star = HitReading.of(statement("SELECT * FROM r172_t"), dates)
    star.readsIgnored shouldBe true
    accepted.foreach(k => withClue(s"* $k: ")(star.onIgnored(k) should not be null))
    // a string keeps its own reader (AC3), an object its own (A-202: a top-level field)
    Seq("id", "k", "kw", "wc", "obj").foreach(k =>
      withClue(s"* $k: ")(star.onIgnored(k) shouldBe null)
    )
    // a named column is decided by the same lookup
    accepted.foreach { k =>
      val named = HitReading.of(statement(s"SELECT id, $k FROM r172_t"), dates)
      withClue(s"named $k: ") {
        named.readsIgnored shouldBe true
        named.onIgnored(k) should not be null
      }
    }
    // a dotted column keeps today's reading (A-202)
    HitReading
      .of(statement("SELECT id, obj.n FROM r172_t"), dates)
      .onIgnored("obj.n") shouldBe null
  }

  /** `r172_c` as a read-only user is typed: from `_field_caps`, as the client types it. */
  private val caps: Schema = {
    val answer = FieldCaps.read(
      "r172_c",
      JacksonConfig.objectMapper.readTree(
        """{"indices": ["r172_c"], "fields": {
          | "id": {"keyword": {"type": "keyword", "metadata_field": false}},
          | "i": {"integer": {"type": "integer", "metadata_field": false}},
          | "ip": {"ip": {"type": "ip", "metadata_field": false}},
          | "kw": {"keyword": {"type": "keyword", "metadata_field": false}}
          |}}""".stripMargin
      )
    )
    FieldCaps.table(answer, Index("r172_c", answer.members.head._2).schema)
  }

  "A table typed from _field_caps" should "read a value Elasticsearch ignored NULL by its field's kind" in {
    Seq("7.17.29", "8.18.3").foreach { version =>
      onRoutes(
        "SELECT id, i, ip, kw FROM r172_c",
        """{"_id": "d1", "_index": "r172_c",
          | "_source": {"id": "d1", "i": "x", "ip": "x", "kw": "abcdefgh"},
          | "_ignored": ["i", "ip", "kw"]}""".stripMargin,
        version = version,
        table = caps
      ).foreach { case (route, rows) =>
        withClue(s"[$version $route] ") {
          val row = rows.head
          row.getOrElse("i", null) shouldBe (null: Any)
          row.getOrElse("ip", null) shouldBe (null: Any)
          row.getOrElse("kw", null) shouldBe "abcdefgh"
          row.get("id") shouldBe Some("d1")
        }
      }
    }
  }
}
