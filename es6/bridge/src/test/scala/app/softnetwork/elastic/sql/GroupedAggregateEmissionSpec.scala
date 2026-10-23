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
  * which level, and how each level is keyed. The same rows as the hand-maintained es6 bridge's copy.
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
  private def body(sql: String): JsonNode = {
    val parsed: SingleSearch = Parser(sql) match {
      case Right(s: SingleSearch) => s
      case other                  => fail(s"[$sql] did not parse as a SingleSearch: $other")
    }
    val request: ElasticSearchRequest = parsed.update(Some(schema))
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
      s"SELECT n + 0 AS kk, g1, MAX(x) AS c FROM t GROUP BY n + 0, g1"                     -> "g1",
      // the controls: the expression key last, and two column keys
      s"SELECT g1, $caseKey AS kk, COUNT(*) AS c FROM t GROUP BY g1, kk" -> "kk",
      "SELECT g1, k, COUNT(*) AS c FROM t GROUP BY g1, k"                -> "k"
    ).foreach { case (sql, leaf) =>
      withClue(s"[$sql] ") {
        childrenOf(body(sql), leaf) should contain("c")
      }
    }
  }
}
