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
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.ZonedDateTime

/** Issue #373, item 4 — a comparison of one column with another must run as a SCRIPT.
  *
  * `requiresScript` only ever inspected the LEFT identifier, so a column-to-column comparison --
  * which the Elasticsearch query DSL cannot express at all -- fell through to `rangeQuery` keyed on
  * the left field NAME, with the RIGHT column rendered as DATE MATH. MEASURED on ES 8.18:
  * `{"range":{"d":{"gt":"ts||/M"}}}` answered `failed to parse date field [ts] with format
  * [strict_date_optional_time||epoch_millis]` -- Elasticsearch read the column NAME as a date
  * literal. A range can only compare a field with a CONSTANT.
  *
  * Executed on a real index (`a` d=2025-01-01/ts=2025-01-01, `b` d=2025-01-05/ts=2025-01-04, `c`
  * d=2025-01-05 with NO ts): before, the shard error above; after, `['b']`, which is the truth.
  */
class ColumnVsColumnSpec extends AnyFlatSpec with Matchers {

  implicit def timestamp: Long = ZonedDateTime.parse("2025-12-31T00:00:00Z").toInstant.toEpochMilli

  private val schema: Table = Table(
    "t",
    columns = List(
      Column("d", SQLTypes.Date),
      Column("ts", SQLTypes.Timestamp),
      Column("name", SQLTypes.Keyword),
      Column("n", SQLTypes.Int)
    )
  )

  /** 🔴 A schema-CARRYING parse, and that is what makes this spec able to see the defect at all.
    * Without one every identifier stays `Any`, the date-math branch is never taken, and BOTH the
    * fixed and the unfixed engine emit a script -- the first version of this spec passed with the
    * fix reverted for exactly that reason. It is #306's rule again: the no-schema rendering is not
    * the one a live query takes.
    */
  private def queryOf(sql: String): String =
    Parser(sql) match {
      case Right(ss: SingleSearch) =>
        val request: ElasticSearchRequest = ss.update(Some(schema))
        request.query.replaceAll("\\s+", "")
      case other => fail(s"[$sql] expected a SingleSearch, got $other")
    }

  "a comparison of one column with a function of another" should "run as a script" in {
    val q = queryOf("SELECT name FROM t WHERE d > DATE_TRUNC(ts, MONTH)")
    withClue(q) {
      q should include("\"script\"")
      q should not include "\"range\""
      q should include("doc['ts']")
      q should not include "ts||/M"
    }
  }

  it should "run as a script for two bare columns too" in {
    val q = queryOf("SELECT name FROM t WHERE d > ts")
    withClue(q) {
      q should include("\"script\"")
      q should not include "\"range\""
      q should not include "ts||"
    }
  }

  "a comparison against a CONSTANT" should "still use the query DSL" in {
    // The range/term/terms paths must not move: they are what makes a filter cheap.
    queryOf("SELECT name FROM t WHERE d > '2025-01-01'") should include(
      "\"range\":{\"d\":{\"gt\":\"2025-01-01\"}}"
    )
    queryOf("SELECT name FROM t WHERE n > 3") should include("\"range\":{\"n\":{\"gt\":3}}")
    queryOf("SELECT name FROM t WHERE name = 'x'") should include("\"term\"")
    queryOf("SELECT name FROM t WHERE n IN (1,2)") should include("\"terms\"")
    queryOf("SELECT name FROM t WHERE d BETWEEN '2025-01-01' AND '2025-02-01'") should include(
      "\"range\""
    )
  }

  "a function-wrapped LEFT operand" should "keep running as a script" in {
    queryOf("SELECT name FROM t WHERE UPPER(name) = 'X'") should include("\"script\"")
  }
}
