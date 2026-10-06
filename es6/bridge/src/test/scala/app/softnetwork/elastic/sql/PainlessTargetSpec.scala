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
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.ZonedDateTime

/** The Elasticsearch major the client module puts in implicit scope (`PainlessTarget`) reaches
  * EVERY query script the bridge's conversions create: a WHERE, a script field, an aggregation over
  * an expression.
  *
  * The probe is a comparison between a `date` doc value and another date, which Elasticsearch 6.8
  * alone needs normalised to UTC: with 6 in scope the body carries the normalisation, with any
  * other major -- or none -- it is the very body no target produces.
  */
class PainlessTargetSpec extends AnyFlatSpec with Matchers {

  private val now: Long = ZonedDateTime.parse("2025-12-31T00:00:00Z").toInstant.toEpochMilli

  private val normalisation = ".withZoneSameInstant(ZoneId.of('Z'))"

  private val schema = Parser(
    "CREATE TABLE t (id KEYWORD, g KEYWORD, d DATE, d2 DATE, ts TIMESTAMP, n INT, PRIMARY KEY (id))"
  ) match {
    case Right(ct: CreateTable) => ct.schema
    case other                  => fail(s"$other")
  }

  private def resolved(sql: String): SingleSearch =
    Parser(sql) match {
      case Right(ss: SingleSearch) =>
        val r = ss.update(Some(schema))
        r.validateResolved() shouldBe Right(())
        r
      case other => fail(s"[$sql] expected a SingleSearch, got $other")
    }

  /** The body `requestToElasticSearchRequest` sends. (`sqlQueryToAggregations`, the other door,
    * parses its statement from text: no schema, so no type to normalise a comparison by.)
    */
  private def body(sql: String, target: PainlessTarget): String =
    requestToElasticSearchRequest(resolved(sql))(
      now,
      PainlessContextType.Query,
      SearchBodySerializer.Default,
      target
    ).query

  private val venues: Seq[String] = Seq(
    // a WHERE: a script query, alone and inside a bool
    "SELECT id FROM t WHERE CAST(ts AS DATE) = d",
    "SELECT id FROM t WHERE CURRENT_DATE > d AND n > 1",
    // a script field
    "SELECT id, CASE WHEN CURRENT_DATE > d THEN 1 ELSE 0 END AS c FROM t",
    // an aggregation over an expression
    "SELECT SUM(CASE WHEN CURRENT_DATE > d THEN 1 ELSE 0 END) AS s FROM t",
    // the WHERE of an aggregate query
    "SELECT g, COUNT(*) AS c FROM t WHERE CAST(ts AS DATE) = d GROUP BY g"
  )

  "a script comparing a date doc value" should
  "carry the UTC normalisation when the client module gives Elasticsearch 6" in {
    venues.foreach { sql =>
      withClue(s"[$sql] ") {
        body(sql, PainlessTarget(6)) should include(normalisation)
      }
    }
  }

  it should "be the body no target produces for Elasticsearch 7, 8 and 9" in {
    venues.foreach { sql =>
      withClue(s"[$sql] ") {
        val default = body(sql, PainlessTarget.Default)
        default should not include normalisation
        Seq(7, 8, 9).foreach(major => body(sql, PainlessTarget(major)) shouldBe default)
        body(sql, PainlessTarget(6)).replace(normalisation, "") shouldBe default
      }
    }
  }
}
