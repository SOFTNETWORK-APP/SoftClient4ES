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

import app.softnetwork.elastic.sql.bridge._
import app.softnetwork.elastic.sql.query._
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.ZonedDateTime
import scala.jdk.CollectionConverters._

/** Issue #413 -- a calculation over aggregates with NO `GROUP BY` is evaluated in ONE bucket, as
  * rendered by the HAND-MAINTAINED es6 bridge.
  *
  * Its `bucket_script` used to sit at the top level of `aggs`, which Elasticsearch refuses on every
  * major (6.8: `Only sibling pipeline aggregations are allowed at the top level`; 7.17 and 8.18:
  * `must be declared inside of another aggregation`). It now hangs, with the metrics it reads, from
  * the single-bucket `filters` aggregation a `HAVING` without `GROUP BY` already uses, and the
  * statement is an aggregation (`size` 0), not a row query. Each shape below was EXECUTED on
  * Elasticsearch 6.8.23, 7.17.29 and 8.18.3 and answered one row.
  */
class WholeTableCalculationSpec extends AnyFlatSpec with Matchers {

  import scala.language.implicitConversions

  implicit def timestamp: Long = ZonedDateTime.parse("2025-12-31T00:00:00Z").toInstant.toEpochMilli

  implicit def sqlQueryToRequest(sqlQuery: SelectStatement): ElasticSearchRequest =
    sqlQuery.statement match {
      case Some(value: SingleSearch) => value.copy(score = sqlQuery.score)
      case other => throw new IllegalArgumentException(s"Not a single search: $other")
    }

  private val mapper = new ObjectMapper()

  private def bodyOf(sql: String): JsonNode = {
    val select: ElasticSearchRequest = SelectStatement(sql)
    mapper.readTree(select.query)
  }

  private def names(node: JsonNode): Seq[String] =
    Option(node).map(_.fieldNames().asScala.toSeq.sorted).getOrElse(Nil)

  private val wrapper = SingleSearch.WholeTableHavingAgg

  "a calculation over aggregates without GROUP BY" should "hang from the whole-table bucket" in {
    Seq(
      "SELECT MAX(n) - MIN(n) AS v FROM t"              -> Seq("max_n", "min_n", "v"),
      "SELECT COUNT(*) * 2 AS v FROM t"                 -> Seq("count_all", "v"),
      "SELECT MAX(d) + 1 AS v FROM t"                   -> Seq("max_d", "v"),
      "SELECT MAX(n) AS m, MAX(n) - MIN(n) AS v FROM t" -> Seq("m", "min_n", "v")
    ).foreach { case (sql, inside) =>
      withClue(s"[$sql] ") {
        val body = bodyOf(sql)
        // an aggregation, not a row query: no document is fetched
        body.get("size").asInt() shouldBe 0
        names(body.get("aggs")) shouldBe Seq(wrapper)
        val bucket = body.get("aggs").get(wrapper)
        bucket
          .get("filters")
          .get("filters")
          .get(SingleSearch.WholeTableHavingBucket)
          .has(
            "match_all"
          ) shouldBe true
        names(bucket.get("aggs")) shouldBe inside
        bucket.get("aggs").get("v").has("bucket_script") shouldBe true
      }
    }
  }

  it should "keep the HAVING's selector beside it, in the same bucket" in {
    val bucket =
      bodyOf("SELECT COUNT(*) * 2 AS v FROM t HAVING COUNT(*) > 1").get("aggs").get(wrapper)
    names(bucket.get("aggs")) shouldBe Seq("count_all", "having_filter", "v")
  }

  it should "leave a statement it does not cover as it was" in {
    // a column beside the calculation has no value for the whole table: unchanged, still refused
    // by Elasticsearch -- not answered with a NULL calculation per document
    val mixed = bodyOf("SELECT id, MAX(n) - MIN(n) AS v FROM t")
    names(mixed.get("aggs")) shouldBe Seq("max_n", "min_n", "v")
    // per group: the GROUP BY bucket is the parent, as before
    names(bodyOf("SELECT g, MAX(n) - MIN(n) AS v FROM t GROUP BY g").get("aggs")) shouldBe Seq("g")
    // a plain aggregate needs no parent
    names(bodyOf("SELECT MAX(n) AS m FROM t").get("aggs")) shouldBe Seq("m")
  }

  private def calculation(sql: String): JsonNode =
    bodyOf(sql).get("aggs").get(wrapper).get("aggs").get("v").get("bucket_script")

  private def gapPolicyOf(sql: String): Option[String] =
    Option(calculation(sql).get("gap_policy")).map(_.asText())

  "over zero documents, the whole-table bucket" should "read COUNT(*) as its own _count" in {
    // Elasticsearch never takes `_count` for a gap: `COUNT(*) * 2` is 0 and `HAVING COUNT(*) = 0`
    // keeps the bucket over no document, on every major (MEASURED on 6.8.23, 7.17.29, 8.18.3)
    calculation("SELECT COUNT(*) * 2 AS v FROM t")
      .get("buckets_path")
      .get("count_all")
      .asText() shouldBe
    "_count"
    val selector = bodyOf("SELECT COUNT(*) AS c FROM t HAVING COUNT(*) = 0")
      .get("aggs")
      .get(wrapper)
      .get("aggs")
      .get("having_filter")
      .get("bucket_selector")
    selector.get("buckets_path").elements().asScala.map(_.asText()).toSeq shouldBe Seq("_count")
    // the COUNT(*) column itself is still the count it was
    names(
      bodyOf("SELECT COUNT(*) AS c FROM t HAVING COUNT(*) = 0").get("aggs").get(wrapper).get("aggs")
    ) shouldBe Seq("c", "having_filter")
    // a count of a column is not the bucket's document count
    calculation("SELECT COUNT(n) * 2 AS v FROM t")
      .get("buckets_path")
      .get("count_n")
      .asText() shouldBe
    "count_n"
  }

  it should "send no gap policy: Elasticsearch 6.8 refuses keep_values" in {
    // `Invalid gap policy: [keep_values], accepted values: [insert_zeros, skip]` (MEASURED on
    // 6.8.23), so COUNT(x) and COUNT(DISTINCT x) of a calculation are NULL over no document there
    Seq(
      "SELECT COUNT(n) * 2 AS v FROM t",
      "SELECT SUM(x) * 2 AS v FROM t",
      "SELECT COUNT(DISTINCT g) * 10 AS v FROM t",
      "SELECT MAX(n) - MIN(n) AS v FROM t"
    ).foreach(sql => withClue(s"[$sql] ")(gapPolicyOf(sql) shouldBe None))
  }
}
