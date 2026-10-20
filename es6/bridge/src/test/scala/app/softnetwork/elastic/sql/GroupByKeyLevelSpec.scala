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
            l.path("date_histogram").path("interval").asText() shouldBe interval
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

  // ---- Elasticsearch 6 only: a key that formats a date column's raw doc value ------------------

  /** The body the conversion builds for a client module talking to `target`'s major. */
  private def bodyFor(sql: String, target: PainlessTarget): Either[String, String] =
    Parser(sql) match {
      case Right(ss: SingleSearch) =>
        scala.util
          .Try(
            requestToElasticSearchRequest(ss.update(Some(schema)))(
              timestamp,
              PainlessContextType.Query,
              SearchBodySerializer.Default,
              target
            ).query
          )
          .toEither
          .left
          .map(_.getMessage)
      case other => fail(s"[$sql] expected a SingleSearch, got $other")
    }

  private def grouped(key: String): String =
    s"SELECT $key AS k, COUNT(*) AS c FROM t GROUP BY $key"

  "a key formatting a date column as stored" should "be refused by name on Elasticsearch 6" in {
    Seq(
      ("DATE_FORMAT(d, '%Y-%m')", "DATE_FORMAT", "d", "DAY"),
      ("DATE_FORMAT(ts, '%Y-%m')", "DATE_FORMAT", "ts", "DAY"),
      ("DATETIME_FORMAT(ts, '%Y-%m')", "DATETIME_FORMAT", "ts", "SECOND")
    ).foreach { case (key, function, column, unit) =>
      withClue(key) {
        bodyFor(grouped(key), PainlessTarget(6)) shouldBe Left(
          ElasticAggregation.dateFormatOfDocValueUnsupportedOn(6, function, column, unit)
        )
      }
    }
    val message = ElasticAggregation.dateFormatOfDocValueUnsupportedOn(6, "DATE_FORMAT", "d", "DAY")
    message should include("Elasticsearch 6")
    message should include("DATE_FORMAT(DATE_TRUNC(d, DAY), ...)")
    message should not include "#"
  }

  it should "be sent as a script on every other major, and with no major given" in {
    Seq(PainlessTarget.Default, PainlessTarget(7), PainlessTarget(8)).foreach { target =>
      withClue(target) {
        bodyFor(grouped("DATETIME_FORMAT(ts, '%Y-%m')"), target).map(
          _.contains("\"script\"")
        ) shouldBe
        Right(true)
        bodyFor(grouped("DATE_FORMAT(d, '%Y-%m')"), target).map(_.contains("\"script\"")) shouldBe
        Right(true)
      }
    }
  }

  it should "not be refused when a function reads the column first, or without a mapping" in {
    Seq(
      "DATE_FORMAT(CAST(d AS DATE), '%Y-%m')",
      "DATE_FORMAT(DATE_TRUNC(d, DAY), '%Y-%m')",
      "DATETIME_FORMAT(DATE_TRUNC(ts, SECOND), '%Y-%m')",
      "DATE_TRUNC(d, MONTH)",
      "YEAR(d)",
      "d"
    ).foreach { key =>
      withClue(key) { bodyFor(grouped(key), PainlessTarget(6)).isRight shouldBe true }
    }
    // without a mapping the column's type is unknown: no refusal, as the statement is parsed
    Parser(grouped("DATE_FORMAT(d, '%Y-%m')")) match {
      case Right(ss: SingleSearch) =>
        ss.groupBy.map(_.buckets.map(_.rawDateDocValueFormat.isDefined)) shouldBe Some(Seq(false))
      case other => fail(s"$other")
    }
  }

  /** The request the conversion builds for `target`, its body not yet serialized. */
  private def requestFor(sql: String, target: PainlessTarget): ElasticSearchRequest =
    Parser(sql) match {
      case Right(ss: SingleSearch) =>
        requestToElasticSearchRequest(ss.update(Some(schema)))(
          timestamp,
          PainlessContextType.Query,
          SearchBodySerializer.Default,
          target
        )
      case other => fail(s"[$sql] expected a SingleSearch, got $other")
    }

  it should "be refused at an INNER level too: the refusal is found anywhere in the tree" in {
    val message = ElasticAggregation.dateFormatOfDocValueUnsupportedOn(6, "DATE_FORMAT", "d", "DAY")
    Seq(
      "SELECT s, DATE_FORMAT(d, '%Y-%m') AS k, COUNT(*) AS c FROM t GROUP BY s, DATE_FORMAT(d, '%Y-%m')",
      "SELECT DATE_FORMAT(d, '%Y-%m') AS k, s, COUNT(*) AS c FROM t GROUP BY DATE_FORMAT(d, '%Y-%m'), s"
    ).foreach { sql =>
      withClue(sql) {
        SearchBodySerializer.refusedLevel(requestFor(sql, PainlessTarget(6)).search) shouldBe
        Some(message)
        bodyFor(sql, PainlessTarget(6)) shouldBe Left(message)
        SearchBodySerializer.refusedLevel(requestFor(sql, PainlessTarget(8)).search) shouldBe None
      }
    }
  }

  it should "leave every body it does not refuse untagged, and byte-identical to the builder's" in {
    for {
      key <- Seq(
        "DATE_FORMAT(DATE_TRUNC(d, DAY), '%Y-%m')",
        "DATE_FORMAT(d, '%Y-%m')",
        "YEAR(d)",
        "d",
        "s"
      )
      target <- Seq(PainlessTarget.Default, PainlessTarget(6), PainlessTarget(8))
      if !(target == PainlessTarget(6) && key == "DATE_FORMAT(d, '%Y-%m')")
    } withClue(s"$key / $target") {
      val search = requestFor(grouped(key), target).search
      SearchBodySerializer.refusedLevel(search) shouldBe None
      val body = SearchBodySerializer.Default.serialize(search)
      body should not include SearchBodySerializer.Refusal
      body shouldBe com.sksamuel.elastic4s.http.search.SearchBodyBuilderFn(search).string()
    }
  }
}
