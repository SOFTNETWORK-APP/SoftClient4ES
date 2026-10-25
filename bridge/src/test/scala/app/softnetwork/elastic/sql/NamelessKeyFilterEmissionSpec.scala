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
import com.fasterxml.jackson.databind.ObjectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.ZonedDateTime

/** A HAVING that repeats a NAMELESS GROUP BY key filters that key's own level: `GROUP BY UPPER(s)
  * HAVING UPPER(s) = 'A'` is the `UPPER` level's `include`, as an exact-value list, at both
  * resolutions.
  */
class NamelessKeyFilterEmissionSpec extends AnyFlatSpec with Matchers {

  implicit def timestamp: Long = ZonedDateTime.parse("2025-12-31T00:00:00Z").toInstant.toEpochMilli

  private val schema: Table = Table(
    "t",
    columns = List(Column("s", SQLTypes.Keyword), Column("n", SQLTypes.Int))
  )

  private val mapper = new ObjectMapper()

  "a HAVING repeating a nameless key" should "be that key's level include, at both resolutions" in {
    Seq(
      ("UPPER(s)", "= 'A'", Seq("A"), "include"),
      ("UPPER(s)", "IN ('A', 'BB')", Seq("A", "BB"), "include"),
      ("UPPER(s)", "<> 'A'", Seq("A"), "exclude"),
      // a number is sent in every text the key's script value may print it as; DuckDB 1.5.5
      // answers `GROUP BY LENGTH(s) HAVING LENGTH(s) = 1` with (1, 3) over the HAVING key fixture
      // of `GroupByCompletenessSpec` (s = a, a, b), and the key prints `1`
      ("LENGTH(s)", "= 1", Seq("1", "1.0"), "include")
    ).foreach { case (key, condition, values, channel) =>
      val sql = s"SELECT $key AS k, COUNT(*) AS c FROM t GROUP BY $key HAVING $key $condition"
      Parser(sql) match {
        case Right(ss: SingleSearch) =>
          Seq("R1" -> ss, "R2" -> ss.update(Some(schema))).foreach { case (resolution, s) =>
            val request: ElasticSearchRequest = s
            val terms = mapper.readTree(request.query).path("aggs").path("k").path("terms")
            withClue(s"$resolution [$sql]: ${request.query}") {
              terms.has("script") shouldBe true
              val filter = terms.path(channel)
              filter.isArray shouldBe true
              (0 until filter
                .size()).map(filter.get(_).asText()).distinct.sorted shouldBe values.sorted
            }
          }
        case other => fail(s"[$sql] expected a SingleSearch, got $other")
      }
    }
  }
}
