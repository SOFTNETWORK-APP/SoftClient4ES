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

/** The `value_count` a `MIN` / `MAX` of a number is sent with, beside it, where the client module
  * reads a metric's `null` as `0.0` (`PainlessTarget.readsNullMetricAsZero`: Elasticsearch 8): the
  * count tells a group with no value from a value of 0. The answers are asserted on every major by
  * the shared testkit trait `GroupedAggregateSemanticsSpec`.
  */
class MinMaxValueCountEmissionSpec extends AnyFlatSpec with Matchers {

  implicit def timestamp: Long = ZonedDateTime.parse("2025-12-31T00:00:00Z").toInstant.toEpochMilli

  private val mapper = new ObjectMapper()

  private val properties: String =
    """{"g1": {"type": "keyword"}, "n": {"type": "integer"}, "x": {"type": "double"},
      | "ts": {"type": "date"}}""".stripMargin

  private val schema: Schema =
    Index(
      "t",
      s"""{"t": {"aliases": {}, "mappings": {"properties": $properties}, """ +
      s""""settings": {"index": {"number_of_shards": "1"}}}}"""
    ).schema

  /** The request of `sql` for the client module of `target`, with the mapping attached. */
  private def body(sql: String, target: PainlessTarget): JsonNode = {
    val parsed: SingleSearch = Parser(sql) match {
      case Right(s: SingleSearch) => s.update(Some(schema))
      case other                  => fail(s"[$sql] did not parse as a SingleSearch: $other")
    }
    val request: ElasticSearchRequest = requestToElasticSearchRequest(parsed)(
      timestamp,
      PainlessContextType.Query,
      SearchBodySerializer.Default,
      target
    )
    mapper.readTree(request.query)
  }

  /** The aggregation named `name`, at any depth of `node`. */
  private def aggNamed(node: JsonNode, name: String): Option[JsonNode] =
    Option(node.get("aggs")).orElse(Option(node.get("aggregations"))).flatMap { aggs =>
      Option(aggs.get(name)).orElse(
        aggs.fields().asScala.toSeq.view.flatMap(e => aggNamed(e.getValue, name)).headOption
      )
    }

  /** The names of the aggregations beside the one named `name`. */
  private def siblingsOf(node: JsonNode, name: String): Seq[String] = {
    def walk(n: JsonNode): Option[Seq[String]] =
      Option(n.get("aggs")).orElse(Option(n.get("aggregations"))).flatMap { aggs =>
        if (aggs.has(name)) Some(aggs.fieldNames().asScala.toSeq.sorted)
        else aggs.fields().asScala.toSeq.view.flatMap(e => walk(e.getValue)).headOption
      }
    walk(node).getOrElse(fail(s"no aggregation [$name] in $node"))
  }

  private def valueCounts(node: JsonNode): Seq[String] =
    node.findParents("value_count").asScala.map(_.get("value_count").toString).toSeq

  private val es8 = PainlessTarget(8)

  behavior of "a MIN / MAX of a number, on Elasticsearch 8"

  it should "be sent with the value count of its own values beside it" in {
    val grouped =
      body("SELECT g1, MIN(n) AS lo, MAX(n) AS hi, MIN(x) AS xlo FROM t GROUP BY g1", es8)
    siblingsOf(grouped, "lo") shouldBe Seq(
      "hi",
      "hi__value_count",
      "lo",
      "lo__value_count",
      "xlo",
      "xlo__value_count"
    )
    aggNamed(grouped, "lo__value_count").map(_.toString) shouldBe
    Some("""{"value_count":{"field":"n"}}""")
    aggNamed(grouped, "xlo__value_count").map(_.toString) shouldBe
    Some("""{"value_count":{"field":"x"}}""")
    // over the whole table
    val whole = body("SELECT MIN(n) AS lo FROM t", es8)
    siblingsOf(whole, "lo") shouldBe Seq("lo", "lo__value_count")
  }

  it should "count the values of its script, when it is over an expression" in {
    val request = body("SELECT g1, MAX(n * 2) AS m FROM t GROUP BY g1", es8)
    val max = aggNamed(request, "m").map(_.get("max")).getOrElse(fail(s"no [m] in $request"))
    val count = aggNamed(request, "m__value_count")
      .map(_.get("value_count"))
      .getOrElse(fail(s"no value count in $request"))
    count.get("script") shouldBe max.get("script")
    count.get("field") shouldBe max.get("field")
  }

  it should "be sent alone for a date or a timestamp, which tells NULL by its value_as_string" in {
    valueCounts(body("SELECT g1, MAX(ts) AS m, MIN(ts) AS l FROM t GROUP BY g1", es8)) shouldBe
    empty
  }

  behavior of "a MIN / MAX of a number, on the other majors"

  it should "be sent alone: their clients read the null Elasticsearch answers" in {
    Seq(
      "SELECT g1, MIN(n) AS lo, MAX(n) AS hi FROM t GROUP BY g1",
      "SELECT MIN(n) AS lo FROM t",
      "SELECT g1, MAX(n * 2) AS m FROM t GROUP BY g1"
    ).foreach { sql =>
      withClue(s"[$sql] ") {
        val default = body(sql, PainlessTarget.Default)
        valueCounts(default) shouldBe empty
        Seq(6, 7, 9).foreach(major => body(sql, PainlessTarget(major)) shouldBe default)
      }
    }
  }
}
