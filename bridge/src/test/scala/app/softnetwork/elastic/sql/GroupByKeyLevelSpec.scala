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
import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query._
import app.softnetwork.elastic.sql.schema.{Column, Table}
import app.softnetwork.elastic.sql.`type`.SQLTypes
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.ZonedDateTime

/** The level a GROUP BY key is emitted as, chosen by `Bucket.isDateHistogram` alone.
  *
  * A level is a `date_histogram` only for a key that has an interval: one `DATE_TRUNC`. Every other
  * key is a `terms` level -- a bare column on its field, a function of a column on its script. Each
  * arm is asked at both resolutions: with the mapping attached (a concrete index, and since an
  * index pattern resolves its merged mapping, a pattern too) and without it (the statement as
  * parsed, which a remembered schema miss sends).
  *
  * Before, the level followed `out`, the COLUMN's type: with the mapping attached every key over a
  * date column was a `date_histogram`, with no interval unless the key was one `DATE_TRUNC`, which
  * Elasticsearch refuses (HTTP 400). And a function whose own `shouldBeScripted` is false
  * (`ISNULL`, `DATE_FORMAT`, `WEEKDAY`) left the level a `terms` on the bare FIELD: one group per
  * raw value, with HTTP 200.
  */
class GroupByKeyLevelSpec extends AnyFlatSpec with Matchers {

  implicit def timestamp: Long = ZonedDateTime.parse("2025-12-31T00:00:00Z").toInstant.toEpochMilli

  private val schema: Table = Table(
    "t",
    columns = List(
      Column("d", SQLTypes.Date),
      Column("ts", SQLTypes.Timestamp),
      Column("s", SQLTypes.Keyword),
      Column("n", SQLTypes.Int)
    )
  )

  private val mapper = new ObjectMapper()

  /** The emitted query, at both resolutions: "R2" with the mapping attached, "R1" as parsed. */
  private def queries(sql: String): Seq[(String, String)] =
    Parser(sql) match {
      case Right(ss: SingleSearch) =>
        val r1: ElasticSearchRequest = ss
        val r2: ElasticSearchRequest = ss.update(Some(schema))
        Seq("R2" -> r2.query, "R1" -> r1.query)
      case other => fail(s"[$sql] expected a SingleSearch, got $other")
    }

  /** The level named `name` at the top of the request's aggregations. */
  private def level(query: String, name: String): JsonNode = {
    val node = mapper.readTree(query).path("aggs").path(name)
    withClue(query) { node.isMissingNode shouldBe false }
    node
  }

  private def isTermsOnField(level: JsonNode, field: String): Boolean =
    level.has("terms") && level.path("terms").path("field").asText() == field &&
    !level.path("terms").has("script")

  private def isTermsOnScript(level: JsonNode): Boolean =
    level.has("terms") && level.path("terms").has("script") &&
    !level.path("terms").has("field")

  "a DATE_TRUNC key" should "be a date_histogram with its calendar interval, at both resolutions" in {
    Seq("MONTH" -> "1M", "HOUR" -> "1h", "QUARTER" -> "1q", "WEEK" -> "1w").foreach {
      case (unit, interval) =>
        queries(
          s"SELECT DATE_TRUNC(d, $unit) AS k, COUNT(*) AS c FROM t GROUP BY DATE_TRUNC(d, $unit)"
        ).foreach { case (resolution, q) =>
          withClue(s"$resolution $unit: $q") {
            val l = level(q, "k")
            l.has("date_histogram") shouldBe true
            l.path("date_histogram").path("calendar_interval").asText() shouldBe interval
          }
        }
    }
  }

  "a bare DATE or TIMESTAMP column" should "be a terms level on its field, at both resolutions" in {
    Seq("d", "ts").foreach { column =>
      queries(s"SELECT $column, COUNT(*) AS c FROM t GROUP BY $column").foreach {
        case (resolution, q) =>
          withClue(s"$resolution: $q") {
            isTermsOnField(level(q, column), column) shouldBe true
            q should not include "date_histogram"
          }
      }
    }
  }

  it should "be a terms level beside a keyword key, in both orders" in {
    Seq("s, d", "d, s").foreach { keys =>
      queries(s"SELECT s, d, COUNT(*) AS c FROM t GROUP BY $keys").foreach { case (resolution, q) =>
        withClue(s"$resolution [$keys]: $q") {
          q should not include "date_histogram"
          q should include("\"field\":\"d\"")
        }
      }
    }
  }

  "a function of a date column" should "be a terms level on the key expression's script" in {
    Seq("YEAR(d)", "MONTH(d)", "DAY(d)", "YEAR(ts)", "LAST_DAY(d)", "DATE_FORMAT(d, '%Y-%m')")
      .foreach { key =>
        queries(s"SELECT $key AS k, COUNT(*) AS c FROM t GROUP BY $key").foreach {
          case (resolution, q) =>
            withClue(s"$resolution $key: $q") {
              isTermsOnScript(level(q, "k")) shouldBe true
              q should not include "date_histogram"
            }
        }
      }
  }

  "a function whose own script flag is off" should "still be scripted, never the bare field" in {
    Seq("ISNULL(s)", "ISNULL(n)", "WEEKDAY(d)", "DAYOFWEEK(d)", "DATE_FORMAT(d, '%Y-%m')").foreach {
      key =>
        queries(s"SELECT $key AS k, COUNT(*) AS c FROM t GROUP BY $key").foreach {
          case (resolution, q) =>
            withClue(s"$resolution $key: $q") {
              isTermsOnScript(level(q, "k")) shouldBe true
            }
        }
    }
  }

  "a bare keyword or number key" should "be the terms level on its field it always was" in {
    // byte-pinned: the class this change does not touch
    val expected = Map(
      "s" -> """{"query":{"match_all":{}},"size":0,"_source":false,"aggs":{"s":{"terms":{"field":"s","size":65536,"min_doc_count":1},"aggs":{"c":{"value_count":{"field":"_index"}}}}}}""",
      "n" -> """{"query":{"match_all":{}},"size":0,"_source":false,"aggs":{"n":{"terms":{"field":"n","size":65536,"min_doc_count":1},"aggs":{"c":{"value_count":{"field":"_index"}}}}}}"""
    )
    expected.foreach { case (column, bytes) =>
      queries(s"SELECT $column, COUNT(*) AS c FROM t GROUP BY $column").foreach {
        case (resolution, q) =>
          withClue(s"$resolution: $q") { q.replaceAll("\\s+", "") shouldBe bytes }
      }
    }
  }

  "Bucket.isDateHistogram" should "hold for one DATE_TRUNC key only" in {
    def buckets(sql: String): Seq[Bucket] =
      Parser(sql) match {
        case Right(ss: SingleSearch) =>
          ss.update(Some(schema)).groupBy.map(_.buckets).getOrElse(Nil)
        case other => fail(s"[$sql] expected a SingleSearch, got $other")
      }
    buckets("SELECT COUNT(*) AS c FROM t GROUP BY DATE_TRUNC(d, MONTH)")
      .map(_.isDateHistogram) shouldBe Seq(true)
    Seq("d", "ts", "YEAR(d)", "LAST_DAY(d)", "s", "n", "UPPER(s)").foreach { key =>
      withClue(key) {
        buckets(s"SELECT COUNT(*) AS c FROM t GROUP BY $key").map(_.isDateHistogram) shouldBe
        Seq(false)
      }
    }
  }
}
