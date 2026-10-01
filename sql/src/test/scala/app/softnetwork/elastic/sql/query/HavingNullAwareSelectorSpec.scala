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
import org.scalatest.OptionValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** F1 -- an aggregate reads in `HAVING` exactly as SELECT returns it; F2 -- a `MATCH` in `HAVING`
  * over an aggregate, or over a column that is neither an aggregate nor a GROUP BY key, is refused
  * by name.
  *
  * The `sql` half: what `MetricSelectorScript.nullAwareSelectorScript` renders, and the refusal.
  * Whether Elasticsearch keeps the right groups is EXECUTED in the testkit's
  * `GroupByCompletenessSpec`, through the gateway, on every major; the emitted request is pinned in
  * the bridge suites.
  */
class HavingNullAwareSelectorSpec extends AnyFlatSpec with Matchers with OptionValues {

  private def parsed(sql: String): SingleSearch = Parser(sql) match {
    case Right(s: SingleSearch) => s
    case other                  => fail(s"[$sql] expected a SingleSearch, got $other")
  }

  private def rejection(sql: String): String = Parser(sql) match {
    case Left(e)  => e.msg
    case Right(s) => fail(s"[$sql] expected a rejection, got $s")
  }

  private def having(sql: String): Criteria =
    parsed(sql).having.flatMap(_.criteria).getOrElse(fail(s"[$sql] has no HAVING criteria"))

  private def script(sql: String): String =
    MetricSelectorScript.nullAwareSelectorScript(having(sql)).value

  private val group = "SELECT g, COUNT(*) AS c FROM t GROUP BY g HAVING "

  /** The null-aware read of `params.<name>`, spelled out here: the function under test is never its
    * own oracle.
    */
  private def read(name: String): String =
    s"((def) (params.$name == null || Double.isNaN(params.$name) || " +
    s"Double.isInfinite(params.$name) ? null : params.$name))"

  private val maxV = read("max_v")

  private val Param = """params\.([A-Za-z_][A-Za-z0-9_]*)""".r

  "the null-aware selector" should "read an aggregate as NULL when it is null, NaN or infinite" in {
    script(group + "MAX(v) <> 5") shouldBe s"($maxV == null ? false : ($maxV != 5))"
  }

  it should "keep the guard OUTSIDE the negation, so NOT of UNKNOWN stays UNKNOWN" in {
    script(group + "NOT MAX(v) = 5") shouldBe s"($maxV == null ? false : ($maxV != 5))"
    script(group + "MAX(v) NOT BETWEEN 4 AND 6") shouldBe
    s"($maxV == null ? false : !($maxV >= 4 && $maxV <= 6))"
    script(group + "MAX(v) NOT IN (5, 8)") shouldBe
    s"($maxV == null ? false : !($maxV == 5 || $maxV == 8))"
    script(group + "COUNT(*) > 2 AND NOT MAX(v) > 6") shouldBe
    s"((params.c == null ? false : (params.c > 2))) && ($maxV == null ? false : ($maxV <= 6))"
  }

  it should "test the value itself for ISNULL and ISNOTNULL" in {
    script(group + "ISNULL(MAX(v))") shouldBe s"$maxV == null"
    script(group + "ISNOTNULL(MAX(v))") shouldBe s"$maxV != null"
    script(group + "COUNT(*) > 2 AND NOT ISNULL(MAX(v))") shouldBe
    s"((params.c == null ? false : (params.c > 2))) && $maxV != null"
  }

  it should "leave a function of the aggregate its own NULL handling" in {
    // COALESCE decides what a NULL means: over a group with no value it answers 5, and 5 > 1.
    script(group + "COALESCE(MAX(v), 5) > 1") shouldBe s"($maxV != null ? $maxV : 5) > 1"
    // GREATEST over a NULL aggregate is UNKNOWN in a HAVING, as before: the guard is the
    // rendering's own. The cast keeps the read `def`: `Math.max(<conditional>, 0)` does not
    // compile otherwise (`Cannot cast null to a primitive type [double]`).
    script(group + "GREATEST(MAX(v), 0) > 1") shouldBe
    s"($maxV == null ? false : (Math.max($maxV, 0) > 1))"
  }

  it should "guard a COALESCE on its result, never on its arguments" in {
    val a = read("max_a")
    val b = read("min_b")
    val coalesce = s"($a != null ? $a : $b)"
    // UNKNOWN only when every argument is NULL: a group with `a` and no `b` compares its `MAX(a)`
    script(group + "COALESCE(MAX(a), MIN(b)) > 1") shouldBe
    s"($coalesce == null ? false : ($coalesce > 1))"
    script(group + "COUNT(*) > 0 AND NOT COALESCE(MAX(a), MIN(b)) > 1") shouldBe
    s"((params.c == null ? false : (params.c > 0))) && " +
    s"($coalesce == null ? false : ($coalesce <= 1))"
    // on the right of an aggregate, whose own guard stays
    script(group + "COUNT(*) > COALESCE(MAX(a), MIN(b))") shouldBe
    s"(params.c == null || $coalesce == null ? false : (params.c > $coalesce))"
    // a function of the COALESCE reads its value
    script(group + "SIGN(COALESCE(MAX(a), MIN(b))) > 0") shouldBe
    s"($coalesce == null ? false : (($coalesce > 0 ? 1 : ($coalesce < 0 ? -1 : 0)) > 0))"
    // a null test reads the value itself, NULL included
    script(group + "ISNULL(COALESCE(MAX(a), MIN(b)))") shouldBe s"$coalesce == null"
    script(group + "ISNOTNULL(COALESCE(MAX(a), MIN(b)))") shouldBe s"$coalesce != null"
    // never NULL -- a literal among the arguments: nothing to guard
    script(group + "COALESCE(MAX(a), MIN(b), 5) > 1") shouldBe
    s"($a != null ? $a : ($b != null ? $b : 5)) > 1"
    script(group + "COUNT(*) > COALESCE(SUM(a), 0)") shouldBe
    "(params.c == null ? false : (params.c > (params.sum_a != null ? params.sum_a : 0)))"
  }

  it should "read COUNT and SUM as before, beside a nullable aggregate" in {
    script(group + "COUNT(*) > MAX(v)") shouldBe
    s"(params.c == null || $maxV == null ? false : (params.c > $maxV))"
    script(group + "SUM(v) > 1 AND MIN(v) < 9") shouldBe
    s"(params.sum_v == null ? false : (params.sum_v > 1)) && " +
    s"(${read("min_v")} == null ? false : (${read("min_v")} < 9))"
  }

  it should "read as NULL every aggregate SELECT answers NULL over no value, and no other" in {
    // F1's list (`ClientAggregation.nullOverEmptyInput`; the core suite asserts the two rules
    // agree over every aggregation type)
    Seq(
      "MIN(v)",
      "MAX(v)",
      "AVG(v)",
      "STDDEV(v)",
      "STDDEV_POP(v)",
      "STDDEV_SAMP(v)",
      "VARIANCE(v)",
      "VAR_POP(v)",
      "VAR_SAMP(v)",
      "PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY v)",
      "PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY v)",
      "FIRST_VALUE(v) OVER (PARTITION BY g ORDER BY v)",
      "LAST_VALUE(v) OVER (PARTITION BY g ORDER BY v)",
      "MAX(ABS(v))"
    ).foreach { a =>
      withClue(s"[$a] ") {
        val criteria = having(group + s"$a > 1")
        val plain = MetricSelectorScript.selectorScript(criteria).value
        val name = Param.findFirstMatchIn(plain).value.group(1)
        script(group + s"$a > 1") shouldBe plain.replace(s"params.$name", read(name))
      }
    }
    // arithmetic over aggregates, read by its SELECT alias: a `bucket_script` result
    script("SELECT g, MAX(v) - MIN(v) AS d FROM t GROUP BY g HAVING d > 1") shouldBe
    s"(${read("d")} == null ? false : (${read("d")} > 1))"
  }

  it should "leave a clause with no such aggregate exactly as rendered before" in {
    Seq(
      group + "COUNT(*) > 1",
      group + "COUNT(v) > 1",
      group + "COUNT(DISTINCT v) > 1",
      group + "SUM(v) > 1 AND COUNT(*) < 9",
      group + "SUM(v) NOT BETWEEN 1 AND 3 OR ISNULL(SUM(v))"
    ).foreach { sql =>
      withClue(s"[$sql] ") {
        val criteria = having(sql)
        MetricSelectorScript.nullAwareSelectorScript(criteria) shouldBe
        MetricSelectorScript.selectorScript(criteria)
      }
    }
  }

  it should "read no variable the clause does not already publish" in {
    Seq(
      group + "MAX(v) <> 5",
      group + "COUNT(*) > 2 OR NOT MIN(v) > 6",
      group + "COALESCE(AVG(v), 5) > 1 AND ISNULL(MAX(v))",
      "SELECT g, MAX(v) AS m FROM t GROUP BY g HAVING m <> 5",
      "SELECT g, MAX(v) - MIN(v) AS d FROM t GROUP BY g HAVING d > 1",
      group + "MAX(d) > now - interval 7 day"
    ).foreach { sql =>
      withClue(s"[$sql] ") {
        val criteria = having(sql)
        def reads(s: Option[String]): Set[String] =
          s.toSeq.flatMap(Param.findAllMatchIn(_).map(_.group(1))).toSet
        reads(MetricSelectorScript.nullAwareSelectorScript(criteria)) shouldBe
        reads(MetricSelectorScript.selectorScript(criteria))
      }
    }
  }

  "Having.script" should "carry the null-aware read the bridge's selector carries (#292)" in {
    Seq(
      group + "MAX(v) <> 5",
      group + "ISNULL(MAX(v))",
      group + "COUNT(*) > 2 OR NOT MIN(v) > 6",
      group + "COALESCE(AVG(v), 5) > 1",
      "SELECT g, MAX(v) AS m FROM t GROUP BY g HAVING m <> 5",
      group + "COUNT(*) > 1"
    ).foreach { sql =>
      withClue(s"[$sql] ") {
        val search = parsed(sql)
        val criteria = search.having.flatMap(_.criteria).value
        @annotation.nowarn("cat=deprecation")
        val viewScript = search.having.flatMap(_.script)
        viewScript shouldBe MetricSelectorScript.nullAwareSelectorScript(criteria).map(_.trim)
      }
    }
  }

  "MetricSelectorScript.metricSelector" should "read NULL as the bridge's selector does (#292)" in {
    metricSelectorOf(group + "MAX(v) <> 5") shouldBe s"($maxV == null ? false : ($maxV != 5))"
    Seq(
      group + "ISNULL(MAX(v))",
      group + "COALESCE(MAX(a), MIN(b)) > 1",
      "SELECT g, MAX(v) AS m FROM t GROUP BY g HAVING m <> 5",
      group + "COUNT(*) > 1"
    ).foreach { sql =>
      withClue(s"[$sql] ") {
        metricSelectorOf(sql) shouldBe
        MetricSelectorScript.nullAwareSelectorScript(having(sql)).value
      }
    }
  }

  private def metricSelectorOf(sql: String): String =
    MetricSelectorScript.metricSelector(having(sql))

  // ---------------------------------------------------------------------------------------------
  // F2 -- a MATCH in HAVING over an aggregate, or over a column that is neither an aggregate nor
  // a GROUP BY key
  // ---------------------------------------------------------------------------------------------

  "a MATCH over an aggregate in HAVING" should "be refused by name, with the remedy" in {
    Seq(
      group + "MATCH (MAX(t)) AGAINST ('x')"                                     -> "MAX(t)",
      group + "COUNT(*) > 1 AND MATCH (MAX(t)) AGAINST ('x')"                    -> "MAX(t)",
      group + "COUNT(*) > 1 OR MATCH (MAX(t)) AGAINST ('x')"                     -> "MAX(t)",
      group + "COUNT(*) > 1 AND NOT MATCH (MAX(t)) AGAINST ('x')"                -> "MAX(t)",
      group + "MATCH (t, MAX(t)) AGAINST ('x')"                                  -> "MAX(t)",
      group + "MATCH (COUNT(t)) AGAINST ('x')"                                   -> "COUNT(t)",
      group + "MATCH (UPPER(MAX(t))) AGAINST ('x')"                              -> "MAX(t)",
      "SELECT g, MAX(t) AS mt FROM t GROUP BY g HAVING MATCH (mt) AGAINST ('x')" -> "mt (MAX(t))",
      "SELECT MAX(t) AS mt FROM t HAVING MATCH (MAX(t)) AGAINST ('x')"           -> "MAX(t)"
    ).foreach { case (sql, aggregate) =>
      withClue(s"[$sql] ") {
        val msg = rejection(sql)
        msg should startWith(s"HAVING cannot apply MATCH to the aggregate $aggregate in MATCH (")
        msg should include("Put the MATCH in WHERE.")
        msg should not startWith Parser.InternalParseFailure
      }
    }
  }

  "a MATCH over a column that is neither an aggregate nor a GROUP BY key" should
  "be refused by name, with the remedy" in {
    Seq(
      group + "MATCH (t) AGAINST ('x')",
      group + "COUNT(*) > 1 AND MATCH (t) AGAINST ('x')",
      group + "COUNT(*) > 1 OR MATCH (t) AGAINST ('x')",
      group + "COUNT(*) > 1 AND NOT MATCH (t) AGAINST ('x')",
      group + "MATCH (g, t) AGAINST ('x')",
      group + "MATCH (UPPER(t)) AGAINST ('x')",
      "SELECT g, COUNT(*) AS c FROM t GROUP BY g, h HAVING MATCH (t) AGAINST ('x')",
      // no GROUP BY: there is no key at all
      "SELECT COUNT(*) AS c FROM t HAVING MATCH (t) AGAINST ('x')"
    ).foreach { sql =>
      withClue(s"[$sql] ") {
        val msg = rejection(sql)
        msg should startWith("HAVING cannot apply MATCH to the column t in MATCH (")
        msg should include("t is neither an aggregate nor a GROUP BY key")
        msg should include("Put the MATCH in WHERE.")
        msg should not startWith Parser.InternalParseFailure
      }
    }
  }

  it should "leave a MATCH over the GROUP BY key as it is" in {
    parsed(group + "MATCH (g) AGAINST ('x')")
    parsed("SELECT g, COUNT(*) AS c FROM t GROUP BY g, h HAVING MATCH (g, h) AGAINST ('x')")
  }

  it should "leave a MATCH the nested filter applies as it is" in {
    // over nested columns only: emitted as a match inside the nested filter, never dropped
    parsed(
      "SELECT p.category AS cat, MIN(p.price) AS min_price FROM stores s JOIN UNNEST(s.products) " +
      "AS p GROUP BY p.category HAVING MATCH (p.name, p.description) AGAINST ('lasagnes') AND " +
      "MIN(p.price) > 5.0"
    )
  }

  it should "leave the same MATCH in WHERE untouched" in {
    parsed(
      "SELECT g, COUNT(*) AS c FROM t WHERE MATCH (t) AGAINST ('x') GROUP BY g HAVING COUNT(*) > 1"
    )
  }
}
