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

package app.softnetwork.elastic.sql.parser

import app.softnetwork.elastic.sql.function.time.{DateDiff, DateDiffSpelling}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** `DATEDIFF`, `DATE_DIFF` and `TIMESTAMPDIFF` subtract and count as the vendors define them.
  *
  * Every vendor computes `end - start`, and the LAYOUT decides where the end sits. With the dates
  * first it is the FIRST operand: MySQL's `DATEDIFF(expr1, expr2)`, BigQuery's `DATE_DIFF(end_date,
  * start_date, part)` (`DATE_DIFF('2010-07-07', '2008-12-25', DAY)` is 559). With the unit first it
  * is the LAST: `DATEDIFF(unit, start, end)` in SQL Server, Snowflake, Redshift, DuckDB and
  * Elasticsearch SQL, and MySQL's `TIMESTAMPDIFF(unit, dt1, dt2)`. The NAME decides the counting:
  * `TIMESTAMPDIFF` counts whole units elapsed, the other two calendar boundaries.
  *
  * 🔴 Issue #363 rested on a wrong premise, that BigQuery's `DATE_DIFF` takes `(start, end, unit)`.
  * Until 0.24.0 the dates-first forms with a unit (`DATE_DIFF(a, b, unit)`, `DATEDIFF(a, b, unit)`)
  * therefore answered `b - a`, and adding a unit flipped `DATEDIFF`'s sign.
  *
  * 🔴 The assertions here are on the AST's `start`/`end`, never on "it parsed". The node always
  * means `end - start`, so which operand landed in which field IS the direction, and a test that
  * only checked `isRight`, or only the rendered text, would pass against the defect. The answers
  * themselves are RUN in `DateFunctionExecutionSpec`.
  */
class DateDiffSignSpec extends AnyFlatSpec with Matchers {

  private def dateDiffOf(sql: String): DateDiff =
    Parser(s"SELECT $sql AS v FROM t") match {
      case Right(stmt) =>
        val fns = stmt.sql // force the render path too
        fns.length should be > 0
        findDateDiff(stmt).getOrElse(fail(s"[$sql] parsed but carries no DateDiff"))
      case Left(e) => fail(s"[$sql] rejected: $e")
    }

  private def findDateDiff(any: Any): Option[DateDiff] = any match {
    case d: DateDiff => Some(d)
    case p: Product  => p.productIterator.flatMap(findDateDiff).toList.headOption
    case _           => None
  }

  private def renderOf(sql: String): String =
    Parser(s"SELECT $sql AS v FROM t").map(_.sql).getOrElse(fail(s"[$sql] rejected"))

  // ──────────────────────────── the direction, by layout ─────────────────────────────

  "the dates first" should "subtract the second operand from the first, in both names and at every arity" in {
    Seq(
      "DATEDIFF(a, b)",
      "DATEDIFF(a, b, DAY)",
      "DATEDIFF(a, b, MONTH)",
      "DATE_DIFF(a, b)",
      "DATE_DIFF(a, b, YEAR)"
    ).foreach { sql =>
      withClue(s"[$sql] ") {
        val d = dateDiffOf(sql)
        // `end - start` is what the node means, so `a - b` requires start = b, end = a.
        (d.start.sql, d.end.sql) shouldBe (("b", "a"))
      }
    }
  }

  it should "read BigQuery's own documented example with its end first" in {
    // DATE_DIFF('2010-07-07', '2008-12-25', DAY) is 559, a positive number: the end comes first.
    val d = dateDiffOf("DATE_DIFF('2010-07-07', '2008-12-25', DAY)")
    (d.start.sql, d.end.sql) shouldBe (("'2008-12-25'", "'2010-07-07'"))
    d.spelling shouldBe DateDiffSpelling.DateFirst
  }

  it should "no longer change DATEDIFF's sign when a unit is added" in {
    // The pair #363 pinned as deliberately different: one layout, one direction.
    val two = dateDiffOf("DATEDIFF(a, b)")
    val three = dateDiffOf("DATEDIFF(a, b, DAY)")
    (two.start.sql, two.end.sql) shouldBe ((three.start.sql, three.end.sql))
    two.spelling shouldBe DateDiffSpelling.MySql
    three.spelling shouldBe DateDiffSpelling.DateFirst
  }

  "the unit first" should "subtract the middle operand from the last, in all three names" in {
    Seq(
      "DATEDIFF(DAY, a, b)",
      "DATEDIFF(MONTH, a, b)",
      "DATE_DIFF(YEAR, a, b)",
      "TIMESTAMPDIFF(DAY, a, b)"
    ).foreach { sql =>
      withClue(s"[$sql] ") {
        val d = dateDiffOf(sql)
        (d.start.sql, d.end.sql) shouldBe (("a", "b"))
      }
    }
  }

  /** 🔴 The regression an independent review caught on #363, and the reason it is pinned here.
    *
    * `DATEDIFF(unit, start, end)` is what a BI tool set to a SQL Server, Snowflake, Redshift or
    * DuckDB dialect emits. Moving `DATEDIFF` onto its own token for the MySQL form silently took
    * the unit-first spelling with it, because `date_diff_transact_sql` keyed on `DateDiff.regex`
    * alone.
    *
    * 🔴 `GrammarDiffProbe` did NOT catch it: its corpus contains no `DATEDIFF(unit, a, b)` input. A
    * spelling removed from a `words` list needs its own assertion, not a corpus sweep.
    */
  "DATEDIFF with the unit first" should "keep its spelling" in {
    Seq("DAY", "MONTH", "YEAR").foreach { unit =>
      withClue(s"[DATEDIFF($unit, a, b)] ") {
        dateDiffOf(s"DATEDIFF($unit, a, b)").spelling shouldBe DateDiffSpelling.UnitFirst
      }
    }
  }

  it should "still let the two-argument MySQL form through, which follows it in the alternation" in {
    // `time_unit` fails on a plain column, so `DATEDIFF(a, b)` falls past the unit-first
    // production to the MySQL one. If that ordering ever breaks, this goes red rather than the
    // direction silently changing.
    val d = dateDiffOf("DATEDIFF(a, b)")
    d.spelling shouldBe DateDiffSpelling.MySql
    (d.start.sql, d.end.sql) shouldBe (("b", "a"))
  }

  // ──────────────────────────── the counting, by name ─────────────────────────────

  "TIMESTAMPDIFF" should "count whole units elapsed, where DATEDIFF and DATE_DIFF count boundaries" in {
    dateDiffOf("TIMESTAMPDIFF(MONTH, a, b)").spelling shouldBe DateDiffSpelling.TimestampDiff
    dateDiffOf("TIMESTAMPDIFF(MONTH, a, b)").spelling.elapsed shouldBe true
    Seq(
      "DATEDIFF(a, b)",
      "DATEDIFF(a, b, MONTH)",
      "DATEDIFF(MONTH, a, b)",
      "DATE_DIFF(a, b, MONTH)",
      "DATE_DIFF(MONTH, a, b)"
    ).foreach { sql =>
      withClue(s"[$sql] ")(dateDiffOf(sql).spelling.elapsed shouldBe false)
    }
  }

  it should "take the dates first too, as no vendor writes it: first - second, elapsed" in {
    // It parses because TIMESTAMPDIFF is a word of the DATE_DIFF token, and the two rules give it
    // its meaning.
    val d = dateDiffOf("TIMESTAMPDIFF(a, b, DAY)")
    (d.start.sql, d.end.sql) shouldBe (("b", "a"))
    d.spelling shouldBe DateDiffSpelling.TimestampDiffDateFirst
    d.spelling.elapsed shouldBe true
  }

  // ────────────────────── the render must re-parse to the SAME node ──────────────────────

  /** 🔴 THIS is the correctness property, and it is the one the naive fix would have broken: with
    * the operands kept as written and reversed at emission, the canonical render flips the sign
    * back on a round trip. It matters because `MaterializedViewExtension` PERSISTS the render and
    * re-runs it. The counting must survive it too, which is why `TIMESTAMPDIFF` keeps its name.
    */
  "the render" should "re-parse to a node with the same start, end and counting, for every spelling" in {
    Seq(
      "DATEDIFF(a, b)",
      "DATEDIFF(a, b, DAY)",
      "DATEDIFF(a, b, MONTH)",
      "DATE_DIFF(a, b)",
      "DATE_DIFF(a, b, MONTH)",
      "DATEDIFF(WEEK, a, b)",
      "DATE_DIFF(DAY, a, b)",
      "TIMESTAMPDIFF(DAY, a, b)",
      "TIMESTAMPDIFF(a, b, QUARTER)"
    ).foreach { sql =>
      withClue(s"[$sql] ") {
        val once = dateDiffOf(sql)
        val rendered = renderOf(sql)
        val twice =
          dateDiffOf(rendered.substring(rendered.indexOf("SELECT ") + 7).split(" AS ").head)
        (twice.start.sql, twice.end.sql, twice.unit, twice.spelling.elapsed) shouldBe
        ((once.start.sql, once.end.sql, once.unit, once.spelling.elapsed))
        withClue(s"render [$rendered] must be a fixed point - ") {
          renderOf(
            rendered.substring(rendered.indexOf("SELECT ") + 7).split(" AS ").head
          ) shouldBe rendered
        }
      }
    }
  }

  /** A READABILITY choice, pinned so it is not changed by accident — not a correctness one.
    * `DATE_DIFF(a, b, DAY)` means the same; it would just hand the user back another function.
    */
  it should "keep MySQL's spelling, so SHOW CREATE hands back what was written" in {
    renderOf("DATEDIFF(a, b)") should include("DATEDIFF(a, b)")
    renderOf("DATEDIFF(a, b)") should not include "DATE_DIFF"
  }

  it should "keep TIMESTAMPDIFF's name, which decides what it counts" in {
    renderOf("TIMESTAMPDIFF(DAY, a, b)") should include("TIMESTAMPDIFF(DAY, a, b)")
    renderOf("TIMESTAMPDIFF(a, b, DAY)") should include("TIMESTAMPDIFF(a, b, DAY)")
  }

  it should "render every other spelling as DATE_DIFF, its operands in the order written" in {
    renderOf("DATE_DIFF(a, b)") should include("DATE_DIFF(a, b, DAY)")
    renderOf("DATEDIFF(a, b, MONTH)") should include("DATE_DIFF(a, b, MONTH)")
    renderOf("DATEDIFF(DAY, a, b)") should include("DATE_DIFF(DAY, a, b)")
  }
}
