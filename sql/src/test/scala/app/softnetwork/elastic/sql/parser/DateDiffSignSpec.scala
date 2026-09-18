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

/** Issue #363 — `DATEDIFF` is MySQL's function and must return MySQL's sign.
  *
  * MySQL 8.4 defines `DATEDIFF(expr1, expr2)` as `expr1 - expr2`; BigQuery's `DATE_DIFF(start, end,
  * unit)` is `end - start`. While `DATEDIFF` was only a WORD of the `DATE_DIFF` token it inherited
  * BigQuery's order and answered the opposite sign.
  *
  * 🔴 The assertions here are on the AST's `start`/`end`, never on "it parsed". The node always
  * means `end - start`, so which operand landed in which field IS the semantics — and a test that
  * only checked `isRight`, or only the rendered text, would pass against the defect.
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

  // ─────────────────────────────── the sign itself ────────────────────────────────

  /** MySQL's own two documented examples. Before this fix they answered -1 and 31. */
  "DATEDIFF with two arguments" should "compute expr1 - expr2, as MySQL does" in {
    val d = dateDiffOf("DATEDIFF(a, b)")
    // `end - start` is what the node means, so MySQL's `a - b` requires start = b, end = a.
    d.start.sql shouldBe "b"
    d.end.sql shouldBe "a"
    d.spelling shouldBe DateDiffSpelling.MySql
  }

  "DATE_DIFF" should "keep BigQuery's end - start, untouched by this change" in {
    val d = dateDiffOf("DATE_DIFF(a, b)")
    d.start.sql shouldBe "a"
    d.end.sql shouldBe "b"
    d.spelling shouldBe DateDiffSpelling.DateFirst
  }

  "TIMESTAMPDIFF" should "keep MySQL's dt2 - dt1, which it already matched" in {
    val d = dateDiffOf("TIMESTAMPDIFF(DAY, a, b)")
    d.start.sql shouldBe "a"
    d.end.sql shouldBe "b"
    d.spelling shouldBe DateDiffSpelling.UnitFirst
  }

  /** 🔴 The lead ruling, pinned BOTH WAYS so it stays deliberate: on this one spelling, adding a
    * unit changes the sign, because the 3-argument form is this engine's own extension and is not
    * MySQL. If either half of this ever moves, it should move on purpose.
    */
  "DATEDIFF with three arguments" should "keep this engine's end - start, unlike the two-arg form" in {
    val two = dateDiffOf("DATEDIFF(a, b)")
    val three = dateDiffOf("DATEDIFF(a, b, DAY)")
    withClue("the 2-arg MySQL form swaps: ") {
      (two.start.sql, two.end.sql) shouldBe ("b", "a")
    }
    withClue("the 3-arg extension does not: ") {
      (three.start.sql, three.end.sql) shouldBe ("a", "b")
    }
    three.spelling shouldBe DateDiffSpelling.DateFirst
    // Stated as a difference, so the test fails if they are ever quietly unified.
    (two.start.sql, two.end.sql) should not be (three.start.sql, three.end.sql)
  }

  // ────────────────────── the render must re-parse to the SAME node ──────────────────────

  /** 🔴 THIS is the correctness property, and it is the one the naive fix would have broken: with
    * the operands kept as written and reversed at emission, the canonical render flips the sign
    * back on a round trip. It matters because `MaterializedViewExtension` PERSISTS the render and
    * re-runs it.
    *
    * Note what this does NOT say: it does not say the `DATEDIFF` spelling must survive the render.
    * Because the parser stores MySQL's operands swapped, `DATE_DIFF(start, end, unit)` would
    * re-parse to this same node too. That is a separate, weaker assertion below.
    */
  "the render" should "re-parse to a node with the same start and end, for every spelling" in {
    Seq(
      "DATEDIFF(a, b)",
      "DATEDIFF(a, b, DAY)",
      "DATE_DIFF(a, b)",
      "DATE_DIFF(a, b, MONTH)",
      "TIMESTAMPDIFF(DAY, a, b)",
      "DATE_DIFF(DAY, a, b)"
    ).foreach { sql =>
      withClue(s"[$sql] ") {
        val once = dateDiffOf(sql)
        val rendered = renderOf(sql)
        val twice =
          dateDiffOf(rendered.substring(rendered.indexOf("SELECT ") + 7).split(" AS ").head)
        (twice.start.sql, twice.end.sql) shouldBe ((once.start.sql, once.end.sql))
        withClue(s"render [$rendered] must be a fixed point - ") {
          renderOf(
            rendered.substring(rendered.indexOf("SELECT ") + 7).split(" AS ").head
          ) shouldBe rendered
        }
      }
    }
  }

  /** A READABILITY choice, pinned so it is not changed by accident — not a correctness one.
    * Normalising to `DATE_DIFF(b, a, DAY)` would be equally correct; it would just hand the user
    * back their own statement with the two arguments visibly exchanged.
    */
  it should "keep MySQL's spelling, so SHOW CREATE hands back what was written" in {
    renderOf("DATEDIFF(a, b)") should include("DATEDIFF(a, b)")
    renderOf("DATEDIFF(a, b)") should not include "DATE_DIFF"
  }

  it should "render the two ODBC/BigQuery spellings as DATE_DIFF, as before" in {
    renderOf("DATE_DIFF(a, b)") should include("DATE_DIFF(a, b, DAY)")
    renderOf("TIMESTAMPDIFF(DAY, a, b)") should include("DATE_DIFF(DAY, a, b)")
  }
}
