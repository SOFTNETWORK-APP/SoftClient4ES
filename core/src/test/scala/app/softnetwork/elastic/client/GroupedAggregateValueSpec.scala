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
           |{"key": "es7", "doc_count": 1, "a": ${extendedStats(
          1,
          "0.0",
          "\"NaN\""
        )}}""".stripMargin
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

  /** `extended_stats` as Elasticsearch 6.8 answers it: no `_sampling` key. */
  private def extendedStats68(count: Long, sum: String, squares: String, variance: String): String =
    s"""{"count": $count, "min": null, "max": null, "avg": null, "sum": $sum,
       | "sum_of_squares": $squares, "variance": $variance, "std_deviation": $variance,
       | "std_deviation_bounds": {"upper": null, "lower": null}}""".stripMargin

  it should "derive a SAMPLE statistic from count, sum and sum of squares where 6.8 has no key" in {
    // the values of the shared trait's group x, n in (5, -900, -4): DuckDB answers var_samp
    // 270320.3333333333 and stddev_samp 519.9233917928037; a group of one value or none, NULL
    def of(fn: String) =
      rows(
        s"SELECT g1, $fn(n) AS a FROM t GROUP BY g1",
        s"""{"key": "x", "doc_count": 3, "a": ${extendedStats68(3, "-899.0", "810041.0", "180213.55555555553")}},
           |{"key": "w", "doc_count": 1, "a": ${extendedStats68(1, "7.0", "49.0", "0.0")}},
           |{"key": "z", "doc_count": 2, "a": ${extendedStats68(0, "0.0", "0.0", "null")}}""".stripMargin
      )
    Seq("VARIANCE", "VAR_SAMP").foreach { fn =>
      withClue(s"[$fn] ") {
        val rs = of(fn)
        valueOf(rs, "x").asInstanceOf[Double] shouldBe 270320.3333333333 +- 1e-6
        valueOf(rs, "w") shouldBe (null: Any)
        valueOf(rs, "z") shouldBe (null: Any)
      }
    }
    Seq("STDDEV", "STDDEV_SAMP").foreach { fn =>
      withClue(s"[$fn] ") {
        val rs = of(fn)
        valueOf(rs, "x").asInstanceOf[Double] shouldBe 519.9233917928037 +- 1e-9
        valueOf(rs, "w") shouldBe (null: Any)
        valueOf(rs, "z") shouldBe (null: Any)
      }
    }
    // the control: a POPULATION statistic reads its own key, present on 6.8
    valueOf(of("VAR_POP"), "x").asInstanceOf[Double] shouldBe 180213.55555555553 +- 1e-6
    // and a response that carries the sample key is read from it, not derived
    valueOf(
      rows(
        "SELECT g1, VAR_SAMP(n) AS a FROM t GROUP BY g1",
        s"""{"key": "x", "doc_count": 3, "a": ${extendedStats(3, "1.0", "2.0")}}"""
      ),
      "x"
    ) shouldBe 2.0
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

  it should "skip the OFFSET's groups, then be cut to the LIMIT" in {
    // DuckDB: LIMIT n OFFSET m answers the n groups after the first m
    ordered(
      "SELECT g1, k, COUNT(*) AS c FROM t GROUP BY g1, k LIMIT 2 OFFSET 1",
      asTheLevelsAnswer
    ) shouldBe asTheLevelsAnswer.slice(1, 3)
    ordered(
      "SELECT g1, k, COUNT(*) AS c FROM t GROUP BY g1, k ORDER BY k DESC, g1 ASC LIMIT 2 OFFSET 1",
      asTheLevelsAnswer
    ).map(r => (r._1, r._2)) shouldBe Seq(("y", "c"), ("x", "b"))
    // past the last group: none
    ordered(
      "SELECT g1, k, COUNT(*) AS c FROM t GROUP BY g1, k LIMIT 2 OFFSET 10",
      asTheLevelsAnswer
    ) shouldBe empty
    // one level: its buckets come in the statement's order, only the skip is left to the rows
    val single = asTheLevelsAnswer.map { case (g, _, c) => ListMap[String, Any]("g1" -> g, "c" -> c) }
    GroupedRowOrder.of(statement("SELECT g1, COUNT(*) AS c FROM t GROUP BY g1 LIMIT 2 OFFSET 3")) match {
      case Some(order) =>
        order shouldBe GroupedRowOrder(Nil, Some(2), Some(3))
        ElasticConversion.orderGroupedRows(single, order)(identity) shouldBe single.slice(3, 5)
      case None => fail("no cut for a one-level OFFSET")
    }
    // an OFFSET with no LIMIT skips its groups and keeps every other one
    ElasticConversion.orderGroupedRows(single, GroupedRowOrder(Nil, None, Some(4)))(identity) shouldBe
    single.drop(4)
  }

  it should "send every level with the LIMIT plus the OFFSET" in {
    statement("SELECT g1, k, COUNT(*) AS c FROM t GROUP BY g1, k LIMIT 2 OFFSET 3").groupBy
      .map(_.buckets.map(_.size))
      .getOrElse(Nil) shouldBe Seq(Some(5), Some(5))
    // the control: no OFFSET
    statement("SELECT g1, COUNT(*) AS c FROM t GROUP BY g1 LIMIT 2").groupBy
      .map(_.buckets.map(_.size))
      .getOrElse(Nil) shouldBe Seq(Some(2))
  }

  it should "refuse an OFFSET the rows cannot skip in the statement's order, and no other" in {
    // an ORDER BY over an aggregate the SELECT does not return: the rows do not carry it
    Parser(
      "SELECT g1, k, COUNT(*) AS c FROM t GROUP BY g1, k ORDER BY MAX(n) DESC LIMIT 2 OFFSET 1"
    ).swap.map(_.msg).getOrElse(fail("not refused")) should include("OFFSET is not supported")
    Seq(
      "SELECT g1, k, COUNT(*) AS c FROM t GROUP BY g1, k LIMIT 2 OFFSET 1",
      "SELECT g1, COUNT(*) AS c FROM t GROUP BY g1 ORDER BY MAX(n) DESC LIMIT 2 OFFSET 1",
      "SELECT g1, COUNT(*) AS c FROM t GROUP BY g1 LIMIT 2 OFFSET 0",
      "SELECT g1, COUNT(*) AS c FROM t GROUP BY g1 LIMIT 2",
      "SELECT n FROM t LIMIT 2 OFFSET 1"
    ).foreach { sql =>
      withClue(s"[$sql] ") { GroupedRowOrder.offsetRefusal(statement(sql)) shouldBe None }
    }
  }

  it should "be left as the levels answer them when they already give the order (the control)" in {
    GroupedRowOrder.of(
      statement("SELECT g1, k, COUNT(*) AS c FROM t GROUP BY g1, k ORDER BY g1, k")
    ) shouldBe None
    GroupedRowOrder.of(statement("SELECT g1, COUNT(*) AS c FROM t GROUP BY g1 LIMIT 2")) shouldBe
    None
  }
}
