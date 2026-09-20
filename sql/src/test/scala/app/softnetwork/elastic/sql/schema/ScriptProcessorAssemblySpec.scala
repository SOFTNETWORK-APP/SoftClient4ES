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

package app.softnetwork.elastic.sql.schema

import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query.CreateTable
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks

/** An ingest script is assembled by splitting the rendered Painless, assigning the LAST statement
  * to the computed column and rejoining the preamble, and read back by recovering that assignment.
  * Both halves used to treat a `;` inside a STRING LITERAL as code (issue #373).
  *
  * 🔴 WRITING — `ScriptProcessor.fromScript` split on a bare `;`, so the assignment was written
  * INTO the literal. Measured on real Elasticsearch 8.18.3 via `_ingest/pipeline/_simulate`:
  *
  *   - `REPLACE(name, ';', 'x')` emitted `param1.replace("; ctx.c = ", "x")` -> compile error
  *   - `CONCAT(name, 'a;b')` emitted `... + "a; ctx.c = b"` -> compile error
  *   - `UPPER('a;b')` emitted `"a; ctx.c = b".toUpperCase()` -> **HTTP 200, column ABSENT**
  *
  * The last is valid Painless that computes a value and discards it: nothing throws, so
  * `ignore_failure` never comes into it and the document is stored without the column. `REPLACE`
  * with a `;` needle needs no contrived input at all, which is what makes this worth a spec.
  *
  * 🔴 READING — `ScriptTarget.of` took the LAST `ctx.<name> = ` by regex, so a literal CONTAINING
  * that text was read as the assignment: `CONCAT(name, '; ctx.d = 1')` made column `c`'s processor
  * report itself as column `d`'s. `IngestPipeline.diff` keys processors by exactly this, and a
  * `Map` silently drops the loser of a collision, so an ALTER rewrote the sibling's processor and
  * never added the new column. **Found by independent review, and it is the WRITING fix that makes
  * it reachable** — before that fix the poisoned emission was a compile error, so the table could
  * not be created in the first place.
  *
  * 🔴 The oracle is INDEPENDENT of the code under test. Asking `PainlessOperandForm` where the
  * literals are would assert the fix with the fix; `literalFree` below strips string literals with
  * its own regex, and the assertion is made on what remains.
  */
class ScriptProcessorAssemblySpec extends AnyFlatSpec with Matchers with TableDrivenPropertyChecks {

  /** Blanks out every string literal, so an assertion can speak about code only. Deliberately a
    * different mechanism from the production scanner: a regex over both quote styles, honouring
    * backslash escapes.
    */
  private def literalFree(source: String): String =
    """"(?:\\.|[^"\\])*"|'(?:\\.|[^'\\])*'""".r
      .replaceAllIn(source, m => " " * m.matched.length)

  private def columnsOf(ddl: String): List[Column] =
    Parser(ddl) match {
      case Right(ct: CreateTable) => ct.schema.columns
      case other                  => fail(s"did not parse as CREATE TABLE: $other")
    }

  private def sourceOf(ddl: String, column: String = "c"): String =
    columnsOf(ddl)
      .find(_.name == column)
      .flatMap(_.script)
      .map(_.source)
      .getOrElse(fail(s"no script for column $column in [$ddl]"))

  private val shapes = Table(
    ("ddl", "literal"),
    ("CREATE TABLE t (name KEYWORD, c KEYWORD SCRIPT AS (REPLACE(name, ';', 'x')))", "\";\""),
    ("CREATE TABLE t (name KEYWORD, c KEYWORD SCRIPT AS (CONCAT(name, 'a;b')))", "\"a;b\""),
    ("CREATE TABLE t (name KEYWORD, c KEYWORD SCRIPT AS (UPPER('a;b')))", "\"a;b\""),
    (
      "CREATE TABLE t (name KEYWORD, c KEYWORD SCRIPT AS (CASE WHEN name = 'x' THEN 'a;b' ELSE 'c' END))",
      "\"a;b\""
    )
  )

  "a `;` inside a string literal" should "not be taken for a statement boundary" in {
    forAll(shapes) { (ddl, literal) =>
      val source = sourceOf(ddl)
      withClue(s"[$ddl] -> [$source] ")(source should include(literal))
    }
  }

  it should "leave the assignment to the computed column outside every literal" in {
    forAll(shapes) { (ddl, _) =>
      val source = sourceOf(ddl)
      withClue(s"[$ddl] -> [$source] ") {
        // exactly one assignment, and it survives the literal blanking -- i.e. it is CODE
        literalFree(source).split("ctx\\.c = ", -1).length - 1 shouldBe 1
      }
    }
  }

  /** A byte-exact drift pin on `;`-free assembly: these shapes were always correct and none of them
    * may move.
    */
  it should "assemble a literal-free expression exactly as before" in {
    forAll(
      Table(
        ("ddl", "expected"),
        (
          "CREATE TABLE t (name KEYWORD, c KEYWORD SCRIPT AS (CONCAT(name, 'ab')))",
          "def param1 = ctx.name; ctx.c = (param1 == null) ? null : String.valueOf(param1) + \"ab\""
        ),
        (
          "CREATE TABLE t (name KEYWORD, c KEYWORD SCRIPT AS (UPPER('ab')))",
          " ctx.c = \"ab\".toUpperCase()"
        )
      )
    )((ddl, expected) => withClue(s"[$ddl] ")(sourceOf(ddl) shouldBe expected))
  }

  "a `ctx.<name> = ` inside a string literal" should "not be read back as the computed column" in {
    val ddl =
      "CREATE TABLE t (name KEYWORD, d KEYWORD SCRIPT AS (LOWER(name)), " +
      "c KEYWORD SCRIPT AS (CONCAT(name, '; ctx.d = 1')))"
    val source = sourceOf(ddl)
    withClue(s"[$source] ") {
      source should include("\"; ctx.d = 1\"") // the literal is intact
      ScriptTarget.of(source) shouldBe Some("c") // ... and read as data, not as the assignment
    }
  }

  it should "not collide two processors onto one identity" in {
    val ddl =
      "CREATE TABLE t (name KEYWORD, d KEYWORD SCRIPT AS (LOWER(name)), " +
      "c KEYWORD SCRIPT AS (CONCAT(name, '; ctx.d = 1')))"
    val identities =
      columnsOf(ddl).flatMap(_.script).map(_.source).flatMap(ScriptTarget.of)
    // `IngestPipeline.diff` keys processors by this, and a Map DROPS the loser of a collision
    withClue(s"identities = $identities ")(identities.distinct.size shouldBe identities.size)
  }

  /** 🔴 CHARACTERISATION, not an endorsement. `fromScript` treats the last `;`-delimited chunk as
    * the expression, and a `;` inside a BLOCK defeats that exactly as a `;` inside a literal did:
    * `TRY_CAST` emits a `try`/`catch`, and the assignment lands inside the `catch` with no
    * right-hand side. Elasticsearch answers `compile error`, so it is LOUD and no data is lost —
    * but `TRY_CAST` in a computed column has never worked.
    *
    * Identical before and after this fix (verified by independent review against the pre-fix
    * assembly on the same rendered Painless), so it is recorded here rather than repaired: the real
    * remedy is for `fromScript` to be HANDED the prologue and the expression by `PainlessContext`
    * instead of re-deriving the boundary from their concatenation, which is a larger change than
    * this one. Recorded so the next reader sees it instead of rediscovering it.
    */
  "the last-statement strategy" should "still be defeated by a `;` inside a block" in {
    val source =
      sourceOf("CREATE TABLE t (name KEYWORD, c BIGINT SCRIPT AS (TRY_CAST(name AS BIGINT)))")
    withClue(s"[$source] ") {
      source should endWith("catch (Exception e) { return null; ctx.c = }")
    }
  }
}
