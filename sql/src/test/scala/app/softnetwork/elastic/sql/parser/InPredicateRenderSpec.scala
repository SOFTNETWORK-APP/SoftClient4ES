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

import app.softnetwork.elastic.sql.SQLKeywords
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** Issue #365 — a function on the left of `IN` / `NOT IN` was rendered TWICE.
  *
  * `Identifier.sql` already renders the function chain, and `InExpr.sql` re-applied the head
  * function on top of it. How that looked depended on the function's own `toString`:
  *
  * {{{
  * WHERE ABS(a)  IN (1)    ->  WHERE ABS(a)(ABS(a)) IN (1)     re-parse FAILS     (loud)
  * WHERE YEAR(a) IN (2024) ->  WHERE YEAR(YEAR(a)) IN (2024)   re-parse SUCCEEDS  (SILENT)
  * }}}
  *
  * ⚠️ `YEAR(YEAR(a))` is legal SQL, and the tempting reading — that it silently asks the year OF
  * THE YEAR — is WRONG, measured: the temporal emission collapses a repeated extractor, so its
  * Painless is byte-identical to `YEAR(a)`. The extractors rendered wrongly and still ANSWERED
  * correctly. The functions that broke LOUDLY are the ones that cost something, and they do not
  * self-heal: `MaterializedViewExtension` PERSISTS this render and re-runs it, so a view authored
  * before the fix still holds `ABS(a)(ABS(a))`.
  *
  * 🔴 The guard is a PROPERTY OVER THE FUNCTION CATALOGUE for the `f(a)` shape, deliberately, not a
  * set of pinned statements. Hand-picked pins would have been written against `ABS`-shaped
  * functions and would have missed the extractors — they are the ones whose broken render looks
  * fine.
  *
  * It is a property over `f(a)`, NOT over every writable call: the zero-argument `f()` degenerate
  * renders unparseably here and on the arms this change does not touch (`AVG() > 1`, `SELECT
  * AVG()`), so it is a parser-accepts-`f()` problem rather than an `InExpr` one, and adding
  * `$name()` below would redden this spec for an unrelated reason.
  *
  * 🔴 And do NOT "strengthen" this to AST equality. `Identifier` inherits `PainlessParam.equals`,
  * which compares only `param` — so `DAY(a)` and `DAY(DAY(a))` are EQUAL as ASTs, on both trees.
  * The render fixed point is the strong assertion here; `Parser(stmt.sql) == Right(stmt)` is blind
  * to this entire defect class.
  */
class InPredicateRenderSpec extends AnyFlatSpec with Matchers {

  /** Every accepted function spelling, from the registry rather than a list kept here — a list
    * would go stale the moment a function is added, which is the failure this spec exists to
    * prevent one layer down.
    */
  private val spellings: List[String] =
    SQLKeywords.functionTokens.flatMap(SQLKeywords.wordsOf).distinct.sorted

  /** Both VENUES an `IN` predicate can appear in. `HAVING` is not a variation on `WHERE` here — it
    * is where the AGGREGATES live, and `HAVING COUNT(x) IN (1, 2)` rendered `COUNT(x)(COUNT(x))`
    * under the defect. A `WHERE`-only sweep would have left that half unguarded; the differential
    * probe is what surfaced it, and it is pinned rather than trusted.
    */
  private def statements(name: String): List[String] = List(
    s"SELECT x FROM t WHERE $name(a) IN (1, 2)",
    s"SELECT x FROM t WHERE $name(a) IN ('u', 'v')",
    s"SELECT x FROM t WHERE $name(a) NOT IN (1, 2)",
    // 🔴 `GROUP BY a`, not `GROUP BY id`: since issue #389 a HAVING predicate must read either an
    // aggregate or a GROUP BY KEY -- `HAVING f(a) ...` over a non-grouped `a` is not constant
    // within a bucket and is now refused by name instead of being silently dropped. Grouping by the
    // column the function is applied to keeps this sweep exercising the HAVING venue, which is the
    // half that carried #365's loud failure (`COUNT(x)(COUNT(x))`).
    s"SELECT a FROM t GROUP BY a HAVING $name(a) IN (1, 2)",
    s"SELECT a FROM t GROUP BY a HAVING $name(a) NOT IN (1, 2)"
  )

  /** A statement is IN SCOPE only if it parses; a function that needs other arity or types simply
    * is not exercised by this shape. That makes the sweep partial, which is why the next test
    * asserts what it must contain.
    */
  private def accepted: List[(String, String)] =
    for {
      name <- spellings
      sql  <- statements(name)
      if Parser(sql).isRight
    } yield (name, sql)

  "the render of a function on the left of IN" should "re-parse to the same statement" in {
    val broken = accepted.flatMap { case (_, sql) =>
      val rendered = Parser(sql).map(_.sql).getOrElse("")
      Parser(rendered) match {
        case Left(e) =>
          Some(s"  $sql\n    renders  $rendered\n    re-parse REJECTED: ${e.toString.take(70)}")
        case Right(again) if again.sql != rendered =>
          Some(s"  $sql\n    renders  $rendered\n    then     ${again.sql}   (not a fixed point)")
        case _ => None
      }
    }
    withClue(
      s"${broken.size} of ${accepted.size} accepted statements do not survive their own render:\n" +
      broken.mkString("\n") + "\n"
    )(broken shouldBe empty)
  }

  /** 🔴 Non-vacuity, computed over the material the sweep guards — a sweep that silently stopped
    * matching anything would pass the test above with an empty list.
    *
    * The two named groups are the two FAILURE SHAPES of #365, so the guard cannot lose either one:
    * the extractors are the silent half, `ABS`-shaped functions the loud half.
    */
  it should "actually exercise both failure shapes, not an empty set" in {
    val names = accepted.map(_._1).distinct
    // The floor tracks the material: the sweep matches 88 spellings / 292 statements today. A loose
    // floor would let a registry regression drop two thirds of the catalogue and still pass.
    withClue(s"the sweep matched ${names.size} function spellings: ${names.mkString(", ")}\n") {
      names.size should be >= 80
    }
    withClue(s"the sweep built ${accepted.size} accepted statements\n") {
      accepted.size should be >= 250
    }
    withClue("the SILENT half of #365 - a function whose double render re-parses legally: ") {
      names should contain allOf ("YEAR", "MONTH", "DAY", "HOUR")
    }
    withClue("the LOUD half - a function whose double render is malformed: ") {
      names should contain allOf ("ABS", "LENGTH", "UPPER")
    }
  }

  /** The exact statements from the issue, kept as named rows beside the sweep so a failure says
    * WHICH shape broke rather than only how many.
    *
    * 🔴 The assertion counts the FUNCTION NAME inside the PREDICATE segment, and both details are
    * load-bearing. Counting the whole rendered statement is wrong — `COUNT(x)` legitimately appears
    * twice when it is in the SELECT list and the HAVING. And counting the operand spelling
    * (`YEAR(a)`) does not discriminate, because the silent defect `YEAR(YEAR(a))` still contains it
    * exactly once. The NAME is what goes from 1 to 2 under both failure shapes.
    */
  it should "hold for the statements the issue names, in both venues" in {
    Map(
      "SELECT x FROM t WHERE YEAR(a) IN (2024)"                          -> "YEAR",
      "SELECT x FROM t WHERE MONTH(a) IN (1)"                            -> "MONTH",
      "SELECT x FROM t WHERE ABS(a) IN (1)"                              -> "ABS",
      "SELECT x FROM t WHERE LOWER(a) IN ('x')"                          -> "LOWER",
      "SELECT x FROM t WHERE LENGTH(a) IN (1)"                           -> "LENGTH",
      "SELECT x FROM t WHERE ABS(a) NOT IN (1)"                          -> "ABS",
      "SELECT id, COUNT(x) FROM t GROUP BY id HAVING COUNT(x) IN (1, 2)" -> "COUNT",
      "SELECT id, MAX(x) FROM t GROUP BY id HAVING MAX(x) IN (40, 50)"   -> "MAX"
    ).foreach { case (sql, name) =>
      withClue(s"[$sql] ") {
        val rendered = Parser(sql).map(_.sql).getOrElse(fail(s"[$sql] rejected"))
        // `lastIndexOf`, not `split`: a value list containing the literal " WHERE " would
        // mis-split. Safe for the rows below either way, but the next row added may not be.
        val cut = math.max(rendered.lastIndexOf(" WHERE "), rendered.lastIndexOf(" HAVING "))
        val predicate = if (cut >= 0) rendered.substring(cut) else rendered
        withClue(s"predicate renders as [$predicate] - ") {
          countOf(predicate, name) shouldBe 1
        }
        Parser(rendered).map(_.sql) shouldBe Right(rendered)
      }
    }
  }

  private def countOf(haystack: String, needle: String): Int =
    haystack.sliding(needle.length).count(_ == needle)

  /** The controls that make the above mean something: these shapes were ALREADY fixed points, so if
    * a fix to `IN` broke them the blame is unambiguous.
    */
  it should "leave the shapes that were already correct alone" in {
    Seq(
      "SELECT x FROM t WHERE a IN (1, 2)",
      "SELECT x FROM t WHERE ABS(a) = 1",
      "SELECT x FROM t WHERE ABS(a) > 1",
      "SELECT x FROM t WHERE ABS(a) BETWEEN 1 AND 2",
      "SELECT ABS(a) AS v FROM t",
      "SELECT x FROM t WHERE YEAR(a) = 2024",
      "SELECT id, COUNT(x) FROM t GROUP BY id HAVING COUNT(x) > 1"
    ).foreach { sql =>
      withClue(s"[$sql] ") {
        // A FIXED POINT, not equality with the input: the render legitimately normalises
        // `IN (1, 2)` to `IN (1,2)`, which is pre-existing and not what this spec is about.
        val rendered = Parser(sql).map(_.sql).getOrElse(fail(s"[$sql] rejected"))
        Parser(rendered).map(_.sql) shouldBe Right(rendered)
        rendered.replace(" ", "") shouldBe sql.replace(" ", "")
      }
    }
  }
}
