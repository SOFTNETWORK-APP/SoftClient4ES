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

package app.softnetwork.elastic.sql

import app.softnetwork.elastic.schema.Index
import app.softnetwork.elastic.sql.bridge._
import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query._
import app.softnetwork.elastic.sql.schema.Schema
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.ZonedDateTime
import scala.collection.JavaConverters._

/** What a GROUP BY statement asks Elasticsearch for, level by level: which aggregations hang from
  * which level, and how each level is keyed. The same rows as the hand-maintained es6 bridge's
  * copy.
  *
  * The answers these requests give are asserted on every major by the shared testkit trait
  * `GroupedAggregateSemanticsSpec`.
  */
class GroupedAggregateEmissionSpec extends AnyFlatSpec with Matchers {

  implicit def timestamp: Long = ZonedDateTime.parse("2025-12-31T00:00:00Z").toInstant.toEpochMilli

  private val mapper = new ObjectMapper()

  private val properties: String =
    """{"id": {"type": "keyword"}, "g1": {"type": "keyword"}, "k": {"type": "keyword"},
      | "n": {"type": "integer"}, "x": {"type": "double"}, "d": {"type": "date"},
      | "ts": {"type": "date"}}""".stripMargin

  private val schema: Schema =
    Index(
      "t",
      s"""{"t": {"aliases": {}, "mappings": {"properties": $properties}, """ +
      s""""settings": {"index": {"number_of_shards": "1"}}}}"""
    ).schema

  /** The request of `sql`, with the mapping attached as core attaches it. */
  private def body(sql: String, over: Schema = schema): JsonNode = {
    val parsed: SingleSearch = Parser(sql) match {
      case Right(s: SingleSearch) => s
      case other                  => fail(s"[$sql] did not parse as a SingleSearch: $other")
    }
    val request: ElasticSearchRequest = parsed.update(Some(over))
    mapper.readTree(request.query)
  }

  /** The aggregation named `name`, at any depth of `node`. */
  private def aggNamed(node: JsonNode, name: String): Option[JsonNode] =
    Option(node.get("aggs")).flatMap { aggs =>
      Option(aggs.get(name)).orElse(
        aggs.fields().asScala.toSeq.view.flatMap(e => aggNamed(e.getValue, name)).headOption
      )
    }

  /** The names of the sub-aggregations of the aggregation named `name`. */
  private def childrenOf(node: JsonNode, name: String): Seq[String] =
    aggNamed(node, name)
      .flatMap(n => Option(n.get("aggs")))
      .map(_.fieldNames().asScala.toSeq.sorted)
      .getOrElse(Nil)

  behavior of "an aggregate under a GROUP BY whose first key is an expression"

  it should "hang from the innermost level, as it does under a column key" in {
    val caseKey = "CASE WHEN d > '2024-02-01' THEN k ELSE 'x' END"
    Seq(
      // the defect: the expression key first -- COUNT(*) hung from no level at all
      s"SELECT $caseKey AS kk, g1, COUNT(*) AS c FROM t GROUP BY kk, g1"                  -> "g1",
      s"SELECT $caseKey AS kk, g1, COUNT(*) AS c FROM t GROUP BY kk, g1 HAVING kk <> 'x'" -> "g1",
      s"SELECT n + 0 AS kk, g1, MAX(x) AS c FROM t GROUP BY n + 0, g1"                    -> "g1",
      // the controls: the expression key last, and two column keys
      s"SELECT g1, $caseKey AS kk, COUNT(*) AS c FROM t GROUP BY g1, kk" -> "kk",
      "SELECT g1, k, COUNT(*) AS c FROM t GROUP BY g1, k"                -> "k"
    ).foreach { case (sql, leaf) =>
      withClue(s"[$sql] ") {
        childrenOf(body(sql), leaf) should contain("c")
      }
    }
  }

  behavior of "a scripted GROUP BY key"

  /** The `value_type` the level `name` is sent with. */
  private def valueTypeOf(node: JsonNode, name: String): Option[String] =
    aggNamed(node, name).flatMap(n => Option(n.path("terms").get("value_type"))).map(_.asText())

  it should "be a numeric terms level when it reports a number" in {
    Seq(
      "SELECT n + 0 AS kk, COUNT(*) AS c FROM t GROUP BY n + 0 ORDER BY kk DESC" -> Some("long"),
      "SELECT n * 2 AS kk, COUNT(*) AS c FROM t GROUP BY n * 2"                  -> Some("long"),
      "SELECT x * 2 AS kk, COUNT(*) AS c FROM t GROUP BY x * 2"                  -> Some("double"),
      "SELECT g1, n + 0 AS kk, COUNT(*) AS c FROM t GROUP BY g1, n + 0"          -> Some("long"),
      // beside a key filter of exact numbers of its type
      "SELECT n + 0 AS kk, COUNT(*) AS c FROM t GROUP BY n + 0 HAVING kk <> 5"      -> Some("long"),
      "SELECT n + 0 AS kk, COUNT(*) AS c FROM t GROUP BY n + 0 HAVING kk IN (5, 7)" -> Some("long"),
      // the controls: a field key, and a key that reports a string
      "SELECT n AS kk, COUNT(*) AS c FROM t GROUP BY n"               -> None,
      "SELECT UPPER(k) AS kk, COUNT(*) AS c FROM t GROUP BY UPPER(k)" -> None
    ).foreach { case (sql, expected) =>
      withClue(s"[$sql] ") {
        valueTypeOf(body(sql), "kk") shouldBe expected
      }
    }
  }

  // A date part renders NUMERIC and reports BIGINT, the whole number its script returns: DuckDB
  // 1.5.5 answers BIGINT for every one (`typeof(year(d))`, ..., `typeof(extract(hour FROM ts))`).
  // Sent as a `double` level, `GROUP BY YEAR(d)` answered the key 2024.0.
  it should "be a whole-number level for a date part, the BIGINT it reports" in {
    val parts = Seq(
      "YEAR(%s)",
      "MONTH(%s)",
      "DAY(%s)",
      "QUARTER(%s)",
      "WEEK(%s)",
      "DAYOFWEEK(%s)",
      "DAYOFYEAR(%s)",
      "HOUR(%s)",
      "MINUTE(%s)",
      "SECOND(%s)",
      "EXTRACT(YEAR FROM %s)",
      "EXTRACT(HOUR FROM %s)"
    )
    for (part <- parts; c <- Seq("d", "ts")) {
      val key = part.format(c)
      val sql = s"SELECT $key AS kk, COUNT(*) AS c FROM t GROUP BY $key"
      withClue(s"[$sql] ") { valueTypeOf(body(sql), "kk") shouldBe Some("long") }
    }
    Seq(
      // beside a key filter of exact numbers of its type
      "SELECT YEAR(d) AS kk, COUNT(*) AS c FROM t GROUP BY YEAR(d) HAVING kk IN (2024, 2025)" ->
      Some("long"),
      // GREATEST / LEAST cast to the common type they report: BIGINT beside an INT, DOUBLE beside
      // a DOUBLE
      "SELECT GREATEST(n, YEAR(d)) AS kk, COUNT(*) AS c FROM t GROUP BY GREATEST(n, YEAR(d))" ->
      Some("long"),
      "SELECT LEAST(x, YEAR(d)) AS kk, COUNT(*) AS c FROM t GROUP BY LEAST(x, YEAR(d))" ->
      Some("double")
    ).foreach { case (sql, expected) =>
      withClue(s"[$sql] ") { valueTypeOf(body(sql), "kk") shouldBe expected }
    }
  }

  // `num` a scaled_float and `ip` an ip, which no SQL type names; `g` a geo_point, no number
  private val untyped: Schema =
    Index(
      "u",
      """{"u": {"aliases": {}, "mappings": {"properties": {"n": {"type": "integer"},
        | "b": {"type": "boolean"}, "num": {"type": "scaled_float", "scaling_factor": 100},
        | "ip": {"type": "ip"}, "g": {"type": "geo_point"}}},
        | "settings": {"index": {"number_of_shards": "1"}}}}""".stripMargin
    ).schema

  // A conditional REPORTS the common type of its typed arms, which any number wins: beside an
  // integer literal a scaled_float, an ip or a geo_point arm reported BIGINT. MEASURED on
  // Elasticsearch 8.18.3: sent as a `long` level, `COALESCE(num, 2)` grouped -3.75 with -3, and
  // `COALESCE(ip, 2)` failed every shard.
  it should "keep its string terms when an arm it answers is no number of its type" in {
    Seq(
      "COALESCE(num, 2)"            -> None,
      "GREATEST(num, 2)"            -> None,
      "LEAST(num, 2)"               -> None,
      "COALESCE(ip, 2)"             -> None,
      "NULLIF(ip, 2)"               -> None,
      "COALESCE(g, 2)"              -> None,
      "COALESCE(NULLIF(num, 0), 2)" -> None,
      // the controls: a cast to a number, a whole-number arm, a BOOLEAN arm (1 or 0, the INTEGER
      // DuckDB types it), and arithmetic over the scaled_float, a string terms already
      "COALESCE(CAST(num AS DOUBLE), 2)" -> Some("double"),
      "COALESCE(n, 2)"                   -> Some("long"),
      "COALESCE(n, b)"                   -> Some("long"),
      "num + 0"                          -> None
    ).foreach { case (key, expected) =>
      val sql = s"SELECT $key AS kk, COUNT(*) AS c FROM u GROUP BY $key"
      withClue(s"[$sql] ") { valueTypeOf(body(sql, untyped), "kk") shouldBe expected }
    }
  }

  behavior of "a GROUP BY of several levels"

  /** The `size` the level `name` is sent with. */
  private def sizeOf(node: JsonNode, name: String): Int =
    aggNamed(node, name)
      .map(_.path("terms").path("size").asInt())
      .getOrElse(fail(s"no level $name"))

  /** The `order` the level `name` is sent with, as its JSON text. */
  private def orderOf(node: JsonNode, name: String): String =
    aggNamed(node, name)
      .map(_.path("terms").path("order").toString)
      .getOrElse(fail(s"no level $name"))

  it should "send every bucket only on the levels above the first sort key's level" in {
    Seq(
      // sorted after they are read: the level of the first sort key, and the levels below it, keep
      // the LIMIT, in that key's order; the levels above it send every bucket
      "SELECT g1, k, COUNT(*) AS c FROM t GROUP BY g1, k ORDER BY k DESC LIMIT 2" -> ((65536, 2)),
      "SELECT g1, k, COUNT(*) AS c FROM t GROUP BY g1, k ORDER BY c DESC LIMIT 2" -> ((65536, 2)),
      "SELECT g1, k, COUNT(*) AS c FROM t GROUP BY g1, k ORDER BY k, g1 LIMIT 2"  -> ((65536, 2)),
      // the last level is ordered by the aggregate: a key sorted beside it is not, inside a parent
      "SELECT g1, k, COUNT(*) AS c FROM t GROUP BY g1, k ORDER BY c DESC, k DESC LIMIT 2" ->
      ((65536, 65536)),
      "SELECT g1, k, COUNT(*) AS c FROM t GROUP BY g1, k ORDER BY k DESC, c DESC LIMIT 2" ->
      ((65536, 65536)),
      // the controls: no ORDER BY, the levels' own order -- each level keeps the LIMIT
      "SELECT g1, k, COUNT(*) AS c FROM t GROUP BY g1, k LIMIT 2"                -> ((2, 2)),
      "SELECT g1, k, COUNT(*) AS c FROM t GROUP BY g1, k ORDER BY g1, k LIMIT 2" -> ((2, 2))
    ).foreach { case (sql, (g1, k)) =>
      withClue(s"[$sql] ") {
        val request = body(sql)
        sizeOf(request, "g1") shouldBe g1
        sizeOf(request, "k") shouldBe k
      }
    }
    orderOf(
      body("SELECT g1, k, COUNT(*) AS c FROM t GROUP BY g1, k ORDER BY k DESC LIMIT 2"),
      "k"
    ) should include(""""_key":"desc"""")
    orderOf(
      body("SELECT g1, k, COUNT(*) AS c FROM t GROUP BY g1, k ORDER BY c DESC LIMIT 2"),
      "k"
    ) should include(""""c":"desc"""")
    // the control: one level keeps the LIMIT
    sizeOf(
      body("SELECT g1, COUNT(*) AS c FROM t GROUP BY g1 ORDER BY c DESC LIMIT 2"),
      "g1"
    ) shouldBe
    2
  }

  it should "order its levels by the aggregate an ORDER BY spells, returned or not" in {
    orderOf(
      body("SELECT g1, k, COUNT(*) AS c FROM t GROUP BY g1, k ORDER BY COUNT(*) DESC LIMIT 2"),
      "k"
    ) should include(""""c":"desc"""")
    sizeOf(
      body("SELECT g1, k FROM t GROUP BY g1, k ORDER BY COUNT(*) DESC LIMIT 2"),
      "k"
    ) shouldBe 2
    sizeOf(
      body("SELECT g1, k, MAX(n) AS m FROM t GROUP BY g1, k ORDER BY MAX(n) DESC LIMIT 3"),
      "g1"
    ) shouldBe 65536
  }
}
