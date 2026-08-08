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

package app.softnetwork.elastic.sql.query

import app.softnetwork.elastic.sql.parser.Parser
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** Issue #209 — `SingleSearch.returnsRows` is the canonical response-shape discriminator: row
  * queries with no LIMIT are routed through the scroll path (row completeness), while
  * aggregation-shaped queries must never be. A wrong `true` would scroll-route an aggregation
  * query (mishandling buckets); a wrong `false` would silently truncate rows at the ES default.
  */
class ReturnsRowsSpec extends AnyFlatSpec with Matchers {

  private def single(sql: String): SingleSearch =
    Parser(sql) match {
      case Right(select: SelectStatement) =>
        select.statement match {
          case Some(s: SingleSearch) => s
          case other                 => fail(s"Not a SingleSearch: $other")
        }
      case Right(s: SingleSearch) => s
      case other                  => fail(s"Failed to parse '$sql': $other")
    }

  "returnsRows" should "be true for a plain projection" in {
    single("SELECT id, amount FROM t").returnsRows shouldBe true
  }

  it should "be true for SELECT *" in {
    single("SELECT * FROM t").returnsRows shouldBe true
  }

  it should "be true for a script-field-only projection" in {
    single("SELECT UPPER(name) AS n FROM t").returnsRows shouldBe true
  }

  it should "be true for a window-enriched row projection" in {
    val s = single(
      "SELECT category, amount, ROW_NUMBER() OVER (PARTITION BY category ORDER BY amount DESC) AS rnum FROM t"
    )
    s.windowRowQuery shouldBe true
    s.returnsRows shouldBe true
  }

  it should "be false for a metric-only SELECT" in {
    single("SELECT COUNT(*) AS cnt FROM t").returnsRows shouldBe false
  }

  it should "be false for a GROUP BY query" in {
    single("SELECT category, COUNT(*) AS cnt FROM t GROUP BY category").returnsRows shouldBe false
  }

  it should "be false for a GROUP BY combined with window functions" in {
    val s = single(
      """SELECT department, AVG(salary) AS avg_salary, MAX(salary) AS max_salary
        |FROM emp
        |GROUP BY department""".stripMargin
    )
    s.windowRowQuery shouldBe false
    s.returnsRows shouldBe false
  }

  it should "not depend on LIMIT" in {
    single("SELECT id FROM t LIMIT 5").returnsRows shouldBe true
    single("SELECT COUNT(*) AS cnt FROM t LIMIT 5").returnsRows shouldBe false
  }
}
