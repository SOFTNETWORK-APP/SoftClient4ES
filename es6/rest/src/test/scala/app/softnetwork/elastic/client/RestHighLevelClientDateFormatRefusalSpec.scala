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

import app.softnetwork.elastic.client.rest.{
  RestHighLevelClientApi,
  RestHighLevelClientSearchBodySerializer
}
import app.softnetwork.elastic.client.result.ElasticError
import app.softnetwork.elastic.sql.bridge._
import app.softnetwork.elastic.sql.query.{SelectStatement, SingleSearch}
import app.softnetwork.elastic.sql.schema.{Column, Table}
import app.softnetwork.elastic.sql.`type`.SQLTypes
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

/** The ES 6 REST-client half of the refusal of a GROUP BY key that formats a date column as stored:
  * the bridge tags the level, and this client's serializer raises the refusal as a named
  * `ElasticError` (status 400) before any JSON exists -- whether the level is the outer one or an
  * inner one. Every other key is written exactly as the default serializer writes it, and no body
  * carries the tag. No Docker: the underlying client is built lazily and never used.
  */
class RestHighLevelClientDateFormatRefusalSpec extends AnyWordSpec with Matchers {

  private val client: RestHighLevelClientApi = new RestHighLevelClientApi {
    override def config: Config = ConfigFactory.load()
  }

  implicit private val timestamp: Long = 1767139200000L // 2025-12-31T00:00:00Z

  private val schema: Table = Table(
    "t",
    columns = List(
      Column("d", SQLTypes.Date),
      Column("ts", SQLTypes.Timestamp),
      Column("s", SQLTypes.Keyword)
    )
  )

  /** The statement as a concrete index sends it: resolved against its mapping. */
  private def resolved(sql: String): SingleSearch =
    SelectStatement(sql).statement match {
      case Some(s: SingleSearch) => s.update(Some(schema))
      case other                 => fail(s"Not a single search: $other")
    }

  "the ES 6 REST client" should {

    "refuse a GROUP BY level formatting a date column as stored, outer or inner, before any JSON exists" in {
      Seq(
        "SELECT DATE_FORMAT(d, '%Y-%m') AS k, COUNT(*) AS c FROM t GROUP BY DATE_FORMAT(d, '%Y-%m')" ->
        ElasticAggregation.dateFormatOfDocValueUnsupportedOn(6, "DATE_FORMAT", "d", "DAY"),
        "SELECT s, DATE_FORMAT(d, '%Y-%m') AS k, COUNT(*) AS c FROM t " +
        "GROUP BY s, DATE_FORMAT(d, '%Y-%m')" ->
        ElasticAggregation.dateFormatOfDocValueUnsupportedOn(6, "DATE_FORMAT", "d", "DAY"),
        "SELECT DATETIME_FORMAT(ts, '%Y-%m') AS k, s, COUNT(*) AS c FROM t " +
        "GROUP BY DATETIME_FORMAT(ts, '%Y-%m'), s" ->
        ElasticAggregation.dateFormatOfDocValueUnsupportedOn(6, "DATETIME_FORMAT", "ts", "SECOND")
      ).foreach { case (sql, message) =>
        withClue(s"[$sql] ") {
          val refusal = intercept[ElasticError](client.singleSearchToJsonQuery(resolved(sql)))
          refusal.message shouldBe message
          refusal.message should include("Elasticsearch 6")
          refusal.statusCode shouldBe Some(400)
          refusal.operation shouldBe Some("search")
        }
      }
    }

    "write every other key exactly as the default serializer does, with no refusal tag" in {
      Seq(
        "SELECT DATE_FORMAT(DATE_TRUNC(d, DAY), '%Y-%m') AS k, COUNT(*) AS c FROM t " +
        "GROUP BY DATE_FORMAT(DATE_TRUNC(d, DAY), '%Y-%m')",
        "SELECT YEAR(d) AS k, COUNT(*) AS c FROM t GROUP BY YEAR(d)",
        "SELECT s, d, COUNT(*) AS c FROM t GROUP BY s, d"
      ).foreach { sql =>
        withClue(s"[$sql] ") {
          val request: ElasticSearchRequest = resolved(sql)
          val body = client.singleSearchToJsonQuery(resolved(sql))
          body shouldBe
          SearchBodySerializer.Default.serialize(request.search).replace("\"version\":true,", "")
          body should not include SearchBodySerializer.Refusal
        }
      }
    }

    "render its scripts for Elasticsearch 6" in {
      client.painlessTarget.jodaCompatibleDates shouldBe true
      client.searchBodySerializer shouldBe RestHighLevelClientSearchBodySerializer
    }
  }
}
