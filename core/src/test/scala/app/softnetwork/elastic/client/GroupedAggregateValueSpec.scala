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
import app.softnetwork.elastic.sql.query.{GroupedRowOrder, SingleSearch}
import app.softnetwork.elastic.sql.schema.Schema
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.{ZoneId, ZonedDateTime}
import scala.collection.immutable.ListMap

/** How the response parser reads the value of an aggregate of a group, from the response shapes
  * Elasticsearch answers -- on 6.8 / 7.17 the raw `null`, on 8 the `0.0` its typed client turns a
  * `null` into. The same statements are executed on every major by the shared testkit trait
  * `GroupedAggregateSemanticsSpec`.
  */
class GroupedAggregateValueSpec extends AnyFlatSpec with Matchers {

  implicit private val context: ConversionContext = NativeContext

  private val properties: String =
    """{"g1": {"type": "keyword"}, "k": {"type": "keyword"}, "n": {"type": "integer"}, "ts": {"type": "date"},
      | "df": {"type": "date", "format": "yyyy/MM/dd HH:mm:ss"}}""".stripMargin

  private val schema: Schema =
    Index(
      "t",
      s"""{"t": {"aliases": {}, "mappings": {"properties": $properties}, """ +
      s""""settings": {"index": {"number_of_shards": "1"}}}}"""
    ).schema

  private def statement(sql: String): SingleSearch = Parser(sql) match {
    case Right(s: SingleSearch) => s.update(Some(schema))
    case other                  => fail(s"[$sql] expected a SingleSearch, got $other")
  }

  /** The rows `response` answers for `sql`, read as the search path reads them. */
  private def rows(sql: String, buckets: String): Seq[ListMap[String, Any]] = {
    val single = statement(sql)
    val aggregations: ListMap[String, ClientAggregation] =
      single.sqlAggregations.map(kv => kv._1 -> implicitly[ClientAggregation](kv._2))
    val response =
      s"""{"hits": {"total": {"value": 9, "relation": "eq"}, "hits": []},
         | "aggregations": {"g1": {"buckets": [$buckets]}}}""".stripMargin
    ElasticConversion
      .parseResponse(response, single.fieldAliases, aggregations, Seq("g1", "a"))
      .get
  }

  private def valueOf(rs: Seq[ListMap[String, Any]], group: String): Any =
    rs.find(_("g1") == group).map(_("a")).getOrElse(fail(s"no group $group in $rs"))

  private val utc = ZoneId.of("UTC")

  behavior of "a MIN / MAX of a date column"

  it should "answer the timestamp, not its epoch milliseconds" in {
    val rs = rows(
      "SELECT g1, MAX(ts) AS a FROM t GROUP BY g1",
      """{"key": "x", "doc_count": 2,
        |  "a": {"value": 1.7434152E12, "value_as_string": "2025-03-31T10:00:00.000Z"}},
        |{"key": "y", "doc_count": 1, "a": {"value": 1.7200512E12,
        |  "value_as_string": "2024-07-04T00:00:00.000Z"}}""".stripMargin
    )
    valueOf(rs, "x").asInstanceOf[ZonedDateTime].toInstant shouldBe
    ZonedDateTime.of(2025, 3, 31, 10, 0, 0, 0, utc).toInstant
    valueOf(rs, "y").asInstanceOf[ZonedDateTime].toInstant shouldBe
    ZonedDateTime.of(2024, 7, 4, 0, 0, 0, 0, utc).toInstant
  }

  it should "answer the timestamp of a date column of a custom format" in {
    val rs = rows(
      "SELECT g1, MIN(df) AS a FROM t GROUP BY g1",
      """{"key": "x", "doc_count": 1,
        |  "a": {"value": 1.7434152E12, "value_as_string": "2025/03/31 10:00:00"}}""".stripMargin
    )
    valueOf(rs, "x").asInstanceOf[ZonedDateTime].toInstant shouldBe
    ZonedDateTime.of(2025, 3, 31, 10, 0, 0, 0, utc).toInstant
  }

  it should "keep a MIN / MAX of a number a number (the control)" in {
    val rs = rows(
      "SELECT g1, MAX(n) AS a FROM t GROUP BY g1",
      """{"key": "x", "doc_count": 2, "a": {"value": 7.0}}"""
    )
    valueOf(rs, "x") shouldBe 7.0
  }

  behavior of "an aggregate of a group with documents but no value of its field"

  /** `extended_stats` over a group, as Elasticsearch 8's typed client renders it (`0.0` for null)
    * and as 6.8 / 7.17 answer it (`null`).
    */
  private def extendedStats(count: Long, variance: String, sampling: String): String =
    s"""{"count": $count, "min": null, "max": null, "avg": null, "sum": 0.0,
       | "sum_of_squares": $variance, "variance": $variance, "variance_sampling": $sampling,
       | "std_deviation": $variance, "std_deviation_sampling": $sampling}""".stripMargin

  it should "answer NULL for a STDDEV / VARIANCE, read from the field's value count" in {
    Seq("STDDEV", "STDDEV_SAMP", "STDDEV_POP", "VARIANCE", "VAR_SAMP", "VAR_POP").foreach { fn =>
      withClue(s"[$fn] ") {
        val rs = rows(
          s"SELECT g1, $fn(n) AS a FROM t GROUP BY g1",
          s"""{"key": "es8", "doc_count": 2, "a": ${extendedStats(0, "0.0", "0.0")}},
             |{"key": "es7", "doc_count": 2, "a": ${extendedStats(0, "null", "null")}},
             |{"key": "two", "doc_count": 2, "a": ${extendedStats(2, "1.0", "2.0")}}""".stripMargin
        )
        valueOf(rs, "es8") shouldBe (null: Any)
        valueOf(rs, "es7") shouldBe (null: Any)
        // the control: a group with values keeps its statistic
        valueOf(rs, "two") should not be (null: Any)
      }
    }
  }

  it should "answer NULL for a SAMPLE statistic of one value, and 0 for a POPULATION one" in {
    def one(fn: String) =
      rows(
        s"SELECT g1, $fn(n) AS a FROM t GROUP BY g1",
        // 7.17 answers the sample statistic of one value as "NaN", 8 as 0.0
        s"""{"key": "es8", "doc_count": 1, "a": ${extendedStats(1, "0.0", "0.0")}},
           |{"key": "es7", "doc_count": 1, "a": ${extendedStats(1, "0.0", "\"NaN\"")}}""".stripMargin
      )
    Seq("STDDEV", "STDDEV_SAMP", "VARIANCE", "VAR_SAMP").foreach { fn =>
      withClue(s"[$fn] ") {
        val rs = one(fn)
        valueOf(rs, "es8") shouldBe (null: Any)
        valueOf(rs, "es7") shouldBe (null: Any)
      }
    }
    Seq("STDDEV_POP", "VAR_POP").foreach { fn =>
      withClue(s"[$fn] ") {
        valueOf(one(fn), "es8") shouldBe 0.0
      }
    }
  }

  it should "answer NULL for a MIN / MAX of a date column" in {
    val rs = rows(
      "SELECT g1, MAX(ts) AS a FROM t GROUP BY g1",
      // 8: the typed client's 0.0, and no value_as_string; 6.8 / 7.17: null
      """{"key": "es8", "doc_count": 2, "a": {"value": 0.0}},
        |{"key": "es7", "doc_count": 2, "a": {"value": null}}""".stripMargin
    )
    valueOf(rs, "es8") shouldBe (null: Any)
    valueOf(rs, "es7") shouldBe (null: Any)
  }

  behavior of "a percentile of a group with no value"

  it should "answer NULL, not Elasticsearch's map of percents" in {
    val sql =
      "SELECT g1, PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY n) AS a FROM t GROUP BY g1"
    val rs = rows(
      sql,
      """{"key": "null", "doc_count": 2, "a": {"values": {"50.0": null}}},
        |{"key": "nan", "doc_count": 2, "a": {"values": {"50.0": "NaN"}}},
        |{"key": "five", "doc_count": 2, "a": {"values": {"50.0": 5.0}}}""".stripMargin
    )
    valueOf(rs, "null") shouldBe (null: Any)
    valueOf(rs, "nan") shouldBe (null: Any)
    // the control: a group with values answers its percentile
    valueOf(rs, "five") shouldBe 5.0
  }

  behavior of "the rows of a GROUP BY of several levels"

  private def ordered(sql: String, in: Seq[(String, String, Any)]): Seq[(String, String, Any)] = {
    val rs = in.map { case (g, k, c) => ListMap[String, Any]("g1" -> g, "k" -> k, "c" -> c) }
    GroupedRowOrder.of(statement(sql)) match {
      case Some(order) =>
        ElasticConversion
          .orderGroupedRows(rs, order)(identity)
          .map(r => (r("g1").toString, r("k").toString, r("c")))
      case None => in
    }
  }

  /** The rows as the levels answer them: ordered inside each `g1` only. */
  private val asTheLevelsAnswer: Seq[(String, String, Any)] = Seq(
    ("x", "a", 2L),
    ("x", "b", 1L),
    ("y", "a", 1L),
    ("y", "c", null),
    ("z", "b", 1L),
    ("w", "c", 1L)
  )

  it should "be cut to the LIMIT" in {
    ordered("SELECT g1, k, COUNT(*) AS c FROM t GROUP BY g1, k LIMIT 2", asTheLevelsAnswer) shouldBe
    asTheLevelsAnswer.take(2)
  }

  it should "be sorted by an ORDER BY the levels cannot give, NULL last ascending" in {
    ordered(
      "SELECT g1, k, COUNT(*) AS c FROM t GROUP BY g1, k ORDER BY k DESC, g1 ASC",
      asTheLevelsAnswer
    ).map(r => (r._1, r._2)) shouldBe
    Seq(("w", "c"), ("y", "c"), ("x", "b"), ("z", "b"), ("x", "a"), ("y", "a"))
    ordered(
      "SELECT g1, k, COUNT(*) AS c FROM t GROUP BY g1, k ORDER BY c ASC, g1 ASC LIMIT 4",
      asTheLevelsAnswer
    ).map(r => (r._1, r._2)) shouldBe Seq(("w", "c"), ("x", "b"), ("y", "a"), ("z", "b"))
    // descending, NULL first
    ordered(
      "SELECT g1, k, COUNT(*) AS c FROM t GROUP BY g1, k ORDER BY c DESC, g1 ASC LIMIT 2",
      asTheLevelsAnswer
    ).map(r => (r._1, r._2)) shouldBe Seq(("y", "c"), ("x", "a"))
  }

  it should "be left as the levels answer them when they already give the order (the control)" in {
    GroupedRowOrder.of(
      statement("SELECT g1, k, COUNT(*) AS c FROM t GROUP BY g1, k ORDER BY g1, k")
    ) shouldBe None
    GroupedRowOrder.of(statement("SELECT g1, COUNT(*) AS c FROM t GROUP BY g1 LIMIT 2")) shouldBe
    None
  }
}
