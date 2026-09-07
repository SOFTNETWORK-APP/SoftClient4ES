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

/** Issue #253 / epic 21 OQ-1 -- ordinal `GROUP BY <n>` and `ORDER BY <n>`.
  *
  * Before this story (every verdict below MEASURED against a parser compiled from `origin/main`
  * `8c426253` on 2026-09-07, not inferred):
  *   - `GROUP BY <n>` resolved to the n-th SELECT item, but an out-of-range POSITIVE position
  *     raised `IndexOutOfBoundsException` from `SingleSearch.bucketNames`, which runs BEFORE
  *     `Bucket.update`'s own bounds check;
  *   - only a NEGATIVE position reached `Bucket.update` and raised `IllegalArgumentException`;
  *   - `bucketNames` sniffed the ordinal with `"\d+".r.findFirstIn` on the RENDERED name, so `GROUP
  *     BY city2` raised `IndexOutOfBoundsException: 1` and `GROUP BY logs2025` raised
  *     `IndexOutOfBoundsException: 2024`;
  *   - `ORDER BY <n>` PARSED (via `identifierWithArithmeticExpression`) and was silently DROPPED --
  *     a constant script sort on the row path, an unmatched bucket-order key on the GROUP BY path.
  *
  * 🔴 Since story 21.4 (#250) none of those crashes ESCAPES: `Parser.apply` has a `NonFatal`
  * boundary catch that turns them into a `Left` labelled `Parser.InternalParseFailure`. That makes
  * `noException` + `isLeft` UNFALSIFIABLE on their own -- restoring the crash still yields a
  * `Left`. Every rejection below therefore also asserts the message does NOT start with that label,
  * which is what distinguishes "the grammar/validator rejected it" from "something blew up".
  */
class GroupByOrdinalSpec extends AnyFlatSpec with Matchers {

  private def parsed(sql: String): SingleSearch =
    Parser(sql) match {
      case Right(select: SelectStatement) =>
        select.statement match {
          case Some(s: SingleSearch) => s
          case other                 => fail(s"Not a SingleSearch: $other")
        }
      case Right(s: SingleSearch) => s
      case other                  => fail(s"Failed to parse '$sql': $other")
    }

  private def bucketPaths(sql: String): Seq[String] = parsed(sql).buckets.map(_.path)

  private def reasonOf(sql: String): String =
    Parser(sql).swap.getOrElse(fail(s"expected Left for [$sql]")).msg

  /** A rejection the VALIDATOR owns: not a throw, not the boundary catch, and naming the problem.
    */
  private def rejects(sql: String, reasons: String*): Unit = {
    withClue(s"[$sql] ") { noException should be thrownBy Parser(sql) }
    withClue(s"[$sql] ") { Parser(sql).isLeft shouldBe true }
    val msg = reasonOf(sql)
    withClue(s"[$sql] msg=[$msg] ") { msg should not startWith Parser.InternalParseFailure }
    reasons.foreach(r => withClue(s"[$sql] msg=[$msg] ") { msg should include(r) })
    ()
  }

  // ---- resolution ----

  // ⚠️ EVERY statement below was run through the real parser and behaves as the comment says. Do
  //    NOT "simplify" one into a multi-column shape: an ordinal that groups only SOME of the
  //    projected columns is rejected by the non-aggregated-field check, before AND after this fix,
  //    because that is correct SQL (`SELECT country, category FROM t GROUP BY 1` is a Left both
  //    ways).

  "GROUP BY <n>" should "resolve to the n-th SELECT item" in {
    bucketPaths("SELECT country FROM t GROUP BY 1") shouldBe Seq("country")
    bucketPaths("SELECT country, category FROM t GROUP BY 1, 2") shouldBe
    Seq("country", "category")
  }

  it should "resolve several positions, in the order WRITTEN, not in SELECT order" in {
    bucketPaths("SELECT country, category FROM t GROUP BY 2, 1") shouldBe Seq("category", "country")
  }

  it should "resolve a position that names an aggregate-free item beside an aggregate" in {
    bucketPaths("SELECT country, COUNT(*) AS c FROM t GROUP BY 1") shouldBe Seq("country")
  }

  it should "resolve a QUALIFIED select item to its column, not to the qualified spelling" in {
    // Defect (b): the substituted identifier was not `update(request)`-ed, so the bucket named
    // "tbl.category" while the SELECT field resolved to "category", and the statement was rejected
    // with the unrelated "Non-aggregated fields tbl.category ..." (MEASURED pre-fix -- this test is
    // RED on `origin/main` as a Left, not as a wrong value).
    bucketPaths("SELECT tbl.category FROM idx tbl GROUP BY 1") shouldBe Seq("category")
    bucketPaths("SELECT tbl.category FROM idx tbl GROUP BY 1") shouldBe
    bucketPaths("SELECT tbl.category FROM idx tbl GROUP BY tbl.category")
  }

  it should "agree with the explicit column spelling" in {
    bucketPaths("SELECT country FROM t GROUP BY 1") shouldBe
    bucketPaths("SELECT country FROM t GROUP BY country")
  }

  // ---- rejection, never a throw (issue #250's family) ----
  //
  // MEASURED pre-fix, on `origin/main` `8c426253`: each of these is already a `Left`, but produced
  // by 21.4's boundary catch, so the message reads `Internal parser error: <Class>: <n>`:
  //   GROUP BY 99  -> Internal parser error: IndexOutOfBoundsException: 98
  //   GROUP BY 0   -> Internal parser error: IndexOutOfBoundsException: -1
  //   GROUP BY -1  -> Internal parser error: IllegalArgumentException: Bucket index must be ...
  // The `not startWith InternalParseFailure` assertion is what makes these tests fail pre-fix.

  "an out-of-range GROUP BY position" should "be rejected by validate(), not by the boundary catch" in {
    rejects("SELECT a FROM t GROUP BY 99", "GROUP BY position 99", "1 item(s)")
    rejects("SELECT a, b FROM t GROUP BY 9", "GROUP BY position 9", "2 item(s)")
  }

  "a non-positive GROUP BY position" should "be rejected by validate(), not by the boundary catch" in {
    rejects("SELECT a FROM t GROUP BY 0", "GROUP BY position 0")
    rejects("SELECT a, b FROM t GROUP BY 0", "GROUP BY position 0")
    rejects("SELECT a FROM t GROUP BY -1", "GROUP BY position -1")
  }

  // ---- defect (c): a digit inside a COLUMN NAME is not an ordinal ----

  "a bucket whose column name contains a digit" should "not be treated as an ordinal" in {
    // Pre-fix this crashed inside `Parser.apply` (one SELECT item,
    // findFirstIn("city2") == Some("2") -> select.fields(1)) and came back as the boundary catch's
    // `Internal parser error: IndexOutOfBoundsException: 1`.
    noException should be thrownBy Parser("SELECT city2 FROM t GROUP BY city2")
    bucketPaths("SELECT city2 FROM t GROUP BY city2") shouldBe Seq("city2")
  }

  it should "not silently re-point the bucket when the SELECT is long enough to index" in {
    // Pre-fix this mapped "2" -> the COUNT(*) identifier and LOST the "status2" key.
    val s = parsed("SELECT status2, COUNT(*) AS c FROM t GROUP BY status2")
    s.buckets.map(_.path) shouldBe Seq("status2")
    s.bucketNames.keys.toSeq should contain("status2")
    s.bucketNames.keys.toSeq should not contain "2"
  }

  it should "not be confused by a four-digit column name" in {
    noException should be thrownBy Parser("SELECT logs2025 FROM t GROUP BY logs2025")
    bucketPaths("SELECT logs2025 FROM t GROUP BY logs2025") shouldBe Seq("logs2025")
  }

  it should "not be confused by a digit inside a SELECT ALIAS" in {
    // Pre-fix `findFirstIn("pays2")` matched the `2` and indexed `select.fields(1)` on a one-column
    // SELECT, crashing inside `Parser.apply`. Post-fix the name is not an ordinal at all and the
    // alias resolves to its column, exactly as an alias without a digit does.
    noException should be thrownBy Parser("SELECT country AS pays2 FROM t GROUP BY pays2")
    bucketPaths("SELECT country AS pays2 FROM t GROUP BY pays2") shouldBe Seq("country")
    bucketPaths("SELECT country AS pays2 FROM t GROUP BY pays2") shouldBe
    bucketPaths("SELECT country AS pays FROM t GROUP BY pays")
  }

  it should "resolve an ordinal EXACTLY ONCE, even when update() runs twice" in {
    // 🔴 Idempotence. A bare literal in the SELECT list has exactly the ordinal's AST shape, so a
    // resolved bucket is itself ordinal-SHAPED. `SingleSearch.update()` really does run twice on
    // one statement (`Table.mergeWithSearch` re-updates an already-parsed search with a schema),
    // and without the `resolved` latch the second pass re-read the substituted literal as a
    // position: MEASURED, `GROUP BY COL2` re-pointed from the literal onto `SUM(amount)`.
    val once = parsed("SELECT 2 AS COL2, SUM(amount) AS s FROM t GROUP BY COL2")
    val twice = once.update()
    twice.buckets.map(b => (b.name, b.path)) shouldBe once.buckets.map(b => (b.name, b.path))

    val ordinalOnce = parsed("SELECT country, category FROM t GROUP BY 2, 1")
    ordinalOnce.update().buckets.map(_.path) shouldBe ordinalOnce.buckets.map(_.path)
  }

  it should "read a QUOTED digit as the column named with it, never as a position" in {
    // The 21.1 escape hatch: `GROUP BY `1`` is `name = "1"` with NO functions, so
    // `Bucket.ordinalOf` declines it. Under the old rendered-name regex it was sniffed as
    // position 1.
    val s = parsed("SELECT `1` FROM t GROUP BY `1`")
    s.buckets.map(_.path) shouldBe Seq("1")
    s.bucketNames.keys.toSeq shouldBe Seq("1")
  }

  // ---- ORDER BY <n> ----

  "ORDER BY <n>" should "resolve to the n-th SELECT item" in {
    parsed("SELECT country, category FROM t GROUP BY country, category ORDER BY 1 ASC").orderBy
      .map(_.sorts.map(_.name)) shouldBe Some(Seq("country"))
  }

  it should "carry the sort direction" in {
    val sorts = parsed("SELECT a, b FROM t ORDER BY 2 DESC").orderBy.map(_.sorts).getOrElse(Nil)
    sorts.map(_.name) shouldBe Seq("b")
    sorts.map(_.direction) shouldBe Seq(Desc)
  }

  it should "stop being a script sort over a constant once it resolves" in {
    // The row-path half of the silent drop: pre-fix `isScriptSort` was true and the Painless was
    // the constant `1`, so every document got the same sort key.
    val sorts = parsed("SELECT a, b FROM t ORDER BY 1").orderBy.map(_.sorts).getOrElse(Nil)
    sorts.map(_.isScriptSort) shouldBe Seq(false)
    sorts.map(_.field.name) shouldBe Seq("a")
  }

  it should "resolve a position that names an AGGREGATE, ordering the buckets by the metric" in {
    // The Tableau/Superset shape `GROUP BY 1 ORDER BY 2 DESC`. Pre-fix `sorts` was
    // ListMap("2" -> DESC), matched no bucket, and was silently dropped (MEASURED).
    val byOrdinal =
      parsed("SELECT category, COUNT(*) AS c FROM t GROUP BY 1 ORDER BY 2 DESC")
    val byExplicit =
      parsed("SELECT category, COUNT(*) AS c FROM t GROUP BY category ORDER BY COUNT(*) DESC")

    byOrdinal.orderBy.map(_.sorts.map(_.name)) should not be Some(Seq("2"))
    byOrdinal.orderBy.map(_.sorts.map(_.name)) shouldBe
    byExplicit.orderBy.map(_.sorts.map(_.name))
    // MEASURED post-fix: `COUNT(*) AS c` folds to the function chain, not to the alias.
    byOrdinal.orderBy.map(_.sorts.map(_.name)) shouldBe Some(Seq("COUNT(*)"))
    byOrdinal.orderBy.map(_.sorts.map(_.direction)) shouldBe Some(Seq(Desc))
  }

  it should "be rejected for an out-of-range position, not silently ignored" in {
    // Pre-fix this PARSED and silently sorted by a constant -- so the RED here is a Right, not a
    // throw and not the boundary catch.
    rejects("SELECT a FROM t ORDER BY 5", "ORDER BY position 5", "1 item(s)")
  }

  it should "reject `ORDER BY 0` and `ORDER BY -1`, which name no SELECT item" in {
    // MEASURED pre-fix: BOTH parse and render back as `ORDER BY 0 ASC` / `ORDER BY -1 ASC`, sorting
    // by a constant. `-1` is NOT "a column named -1" -- it is LongValue(-1) with an empty
    // field.name; `identifierName` merely renders it back. This is a deliberate behaviour change on
    // statements that "succeed" today (release-note item).
    rejects("SELECT a FROM t ORDER BY 0", "ORDER BY position 0")
    rejects("SELECT a FROM t ORDER BY -1", "ORDER BY position -1")
  }

  it should "read a QUOTED digit as the column named with it, never as a position" in {
    val sorts = parsed("SELECT a FROM t ORDER BY `1`").orderBy.map(_.sorts).getOrElse(Nil)
    sorts.map(_.name) shouldBe Seq("1")
    sorts.map(_.field.name) shouldBe Seq("1")
  }

  it should "not read an ARITHMETIC or FUNCTION sort as an ordinal" in {
    // The discriminator guard (AD-8). MEASURED chains: `a + 1` / `1 + 1` / `2 * 1` ->
    // List(ArithmeticExpression); `ABS(a)` -> List(MathematicalFunctionWithOp). None is an ordinal,
    // so none may be resolved against the SELECT list -- and `1 + 1` in particular must NOT become
    // "the 2nd SELECT item".
    parsed("SELECT a, b FROM t ORDER BY 1 + 1").orderBy.map(_.sorts.map(_.name)) shouldBe
    Some(Seq("1 + 1"))
    parsed("SELECT a FROM t ORDER BY a + 1").orderBy.map(_.sorts.map(_.name)) shouldBe
    Some(Seq("a + 1"))
    parsed("SELECT a FROM t ORDER BY 2 * 1").orderBy.map(_.sorts.map(_.name)) shouldBe
    Some(Seq("2 * 1"))
    parsed("SELECT a FROM t ORDER BY ABS(a)").orderBy.map(_.sorts.map(_.name)) shouldBe
    Some(Seq("ABS(a)"))
  }

  it should "not read a GROUP BY function or quoted expression as an ordinal" in {
    bucketPaths("SELECT SUBSTRING(a, 1, 3) AS s FROM t GROUP BY SUBSTRING(a, 1, 3)") shouldBe
    Seq("")
  }
}
