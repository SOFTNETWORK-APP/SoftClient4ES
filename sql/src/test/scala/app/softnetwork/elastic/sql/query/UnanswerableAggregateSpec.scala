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

/** The aggregate shapes Elasticsearch cannot answer as SQL does, and that are therefore refused by
  * name (`UnanswerableAggregate`), each beside the shapes of the same family that stay answered.
  * DuckDB 1.5.5's answers are MEASURED.
  */
class UnanswerableAggregateSpec extends AnyFlatSpec with Matchers {

  /** Why the statement's validation refuses `sql`, or `None` when it is answered. */
  private def refusal(sql: String): Option[String] =
    Parser(sql) match {
      case Right(_: SingleSearch) => None
      case Left(error)            => Some(error.msg)
      case Right(other)           => fail(s"[$sql] expected a SingleSearch, got $other")
    }

  behavior of "an aggregate written with DISTINCT"

  it should "be refused by name where Elasticsearch would compute it over every value" in {
    // DuckDB over (1, 1, 4): AVG(DISTINCT n) 2.5 where AVG(n) is 2.0, SUM(DISTINCT n) 5 where
    // SUM(n) is 6, VAR_POP(DISTINCT n) 2.25, QUANTILE_CONT(DISTINCT n, 0.5) 2.5 where it is 1.0
    Seq(
      "SELECT g, AVG(DISTINCT n) AS a FROM t GROUP BY g",
      "SELECT AVG(DISTINCT n) AS a FROM t",
      "SELECT g, SUM(DISTINCT n) AS a FROM t GROUP BY g",
      "SELECT g, STDDEV(DISTINCT n) AS a FROM t GROUP BY g",
      "SELECT g, STDDEV_POP(DISTINCT n) AS a FROM t GROUP BY g",
      "SELECT g, STDDEV_SAMP(DISTINCT n) AS a FROM t GROUP BY g",
      "SELECT g, VARIANCE(DISTINCT n) AS a FROM t GROUP BY g",
      "SELECT g, VAR_POP(DISTINCT n) AS a FROM t GROUP BY g",
      "SELECT g, VAR_SAMP(DISTINCT n) AS a FROM t GROUP BY g",
      "SELECT g, PERCENTILE_CONT(DISTINCT n, 0.5) AS a FROM t GROUP BY g",
      "SELECT g, PERCENTILE_DISC(DISTINCT n, 0.5) AS a FROM t GROUP BY g",
      // in a HAVING, an ORDER BY, a per-group calculation, over a window
      "SELECT g, COUNT(*) AS c FROM t GROUP BY g HAVING AVG(DISTINCT n) > 2",
      "SELECT g, COUNT(*) AS c FROM t GROUP BY g ORDER BY SUM(DISTINCT n) DESC",
      "SELECT g, AVG(DISTINCT n) * 2 AS a FROM t GROUP BY g",
      "SELECT g, AVG(DISTINCT n) OVER (PARTITION BY g) AS a FROM t"
    ).foreach { sql =>
      withClue(s"[$sql] ") {
        val reason = refusal(sql).getOrElse(fail("not refused"))
        reason should include("DISTINCT")
        reason should include("computes this aggregate over every value")
      }
    }
  }

  it should "stay answered where Elasticsearch honours DISTINCT or the answer is the same" in {
    Seq(
      "SELECT g, COUNT(DISTINCT n) AS a FROM t GROUP BY g",
      "SELECT COUNT(DISTINCT n) AS a FROM t",
      "SELECT g, MIN(DISTINCT n) AS a FROM t GROUP BY g",
      "SELECT g, MAX(DISTINCT n) AS a FROM t GROUP BY g",
      "SELECT g, COUNT(DISTINCT n) * 2 AS a FROM t GROUP BY g",
      // the control: no DISTINCT
      "SELECT g, AVG(n) AS a, SUM(n) AS s, STDDEV(n) AS d FROM t GROUP BY g"
    ).foreach { sql =>
      withClue(s"[$sql] ") {
        refusal(sql) shouldBe None
      }
    }
  }
}
