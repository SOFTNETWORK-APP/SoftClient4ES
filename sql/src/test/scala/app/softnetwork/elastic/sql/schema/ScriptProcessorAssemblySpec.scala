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

  /** A Painless BLOCK COMMENT around `body`. Assembled rather than written literally: Scala
    * comments NEST, so the delimiters cannot appear inside the scaladoc that explains them.
    */
  private def blockComment(body: String): String = "/" + "* " + body + " *" + "/"

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

  // -- issue #382: a `;` inside a BLOCK ---------------------------------------------------------

  /** 🔴 This block REPLACES a characterisation that used to pin the defect verbatim (`source should
    * endWith("catch (Exception e) { return null; ctx.c = }")`). A `;` inside a BLOCK defeated the
    * last-statement strategy exactly as a `;` inside a literal did: `TRY_CAST` renders a
    * `try`/`catch` STATEMENT, the assignment landed inside the `catch` with no right-hand side,
    * Elasticsearch answered `compile error`, and `TRY_CAST` in a computed column had never worked
    * in ANY shape.
    *
    * The remedy is the one that characterisation named: `fromScript` is HANDED the prologue and the
    * expression rather than re-deriving the boundary from their concatenation
    * ([[ScriptTarget.assemble]]), and a safe cast is hoisted into the prologue in every context
    * that HAS one, not only in a query (`function/convert`).
    *
    * 🔴 The two assertions below fail for DIFFERENT halves of that repair, which is why there are
    * two (`feedback_assert_the_mechanism_not_a_proxy`: revert the smallest unit, not the branch):
    *
    *   - restore the `ctx.context == Query` guard on the hoist, and the assigned expression is the
    *     `try`/`catch` itself -> `an EXPRESSION` reddens, `braces balance` does not;
    *   - restore the `splitStatements` assembly, and the assignment is spliced into the `try` block
    * -> `braces balance` reddens, `an EXPRESSION` does not.
    */
  private val blockShapes = Table(
    "ddl",
    "CREATE TABLE t (name KEYWORD, c BIGINT SCRIPT AS (TRY_CAST(name AS BIGINT)))",
    "CREATE TABLE t (name KEYWORD, c BIGINT SCRIPT AS (SAFE_CAST(name AS BIGINT)))",
    "CREATE TABLE t (name KEYWORD, c KEYWORD SCRIPT AS (CONCAT(TRY_CAST(name AS BIGINT), 'x')))",
    "CREATE TABLE t (name KEYWORD, c KEYWORD SCRIPT AS (UPPER(TRY_CAST(name AS KEYWORD))))",
    "CREATE TABLE t (name KEYWORD, c BIGINT SCRIPT AS (COALESCE(TRY_CAST(name AS BIGINT), 0)))",
    "CREATE TABLE t (name KEYWORD, c BIGINT SCRIPT AS " +
    "(CASE WHEN name = 'x' THEN TRY_CAST(name AS BIGINT) ELSE 0 END))",
    "CREATE TABLE t (name KEYWORD, c DOUBLE SCRIPT AS (TRY_CAST(name AS DOUBLE) + 1))"
  )

  /** Where the assignment `assemble` appended starts, located on the literal-blanked source so a
    * `ctx.c = ` written INSIDE a string literal cannot be mistaken for it. Offsets are preserved,
    * so the index addresses `source` too.
    */
  private def assignmentAt(source: String): Int = literalFree(source).lastIndexOf("ctx.c = ")

  it should "leave the assignment OUTSIDE every block, not only outside every literal" in {
    forAll(blockShapes) { ddl =>
      val source = sourceOf(ddl)
      val code = literalFree(source)
      val at = assignmentAt(source)
      withClue(s"[$ddl] -> [$source] ") {
        at should be >= 0
        val before = code.substring(0, at)
        // an unbalanced `{` before the assignment means it was spliced INTO a block
        before.count(_ == '{') shouldBe before.count(_ == '}')
      }
    }
  }

  it should "assign an EXPRESSION, never a statement" in {
    forAll(blockShapes) { ddl =>
      val source = sourceOf(ddl)
      val code = literalFree(source)
      val rhs = code.substring(assignmentAt(source) + "ctx.c = ".length)
      withClue(s"[$ddl] -> [$source] ") {
        // Painless has no expression-level `try`, and a `;` would start a second statement --
        // either one makes `ctx.c = <rhs>` a compile error. Asked of the literal-blanked source, so
        // this is about CODE.
        rhs should not include ";"
        """\btry\b""".r.findFirstIn(rhs) shouldBe None
        rhs.trim should not be empty
      }
    }
  }

  it should "keep exactly one assignment, readable back as the computed column" in {
    forAll(blockShapes) { ddl =>
      val source = sourceOf(ddl)
      withClue(s"[$ddl] -> [$source] ") {
        literalFree(source).split("ctx\\.c = ", -1).length - 1 shouldBe 1
        ScriptTarget.of(source) shouldBe Some("c")
      }
    }
  }

  /** The hoist itself: a safe cast's `try`/`catch` must be in the PROLOGUE. Without this the two
    * assertions above are satisfied by a script that simply dropped the conversion — which is what
    * `TRY_CAST(name AS DOUBLE) + 1` used to do, silently (issue #373's `placeable` residual).
    */
  it should "hoist a safe cast's try/catch into the prologue" in {
    forAll(blockShapes) { ddl =>
      val source = sourceOf(ddl)
      val code = literalFree(source)
      withClue(s"[$ddl] -> [$source] ") {
        code should include("try { safe")
        code.indexOf("try ") should be < assignmentAt(source)
        code should not include "return null"
      }
    }
  }

  /** The byte pin for the filed shape, so the repair itself cannot drift unnoticed. */
  it should "assemble the filed TRY_CAST shape exactly (#382)" in {
    sourceOf(
      "CREATE TABLE t (name KEYWORD, c BIGINT SCRIPT AS (TRY_CAST(name AS BIGINT)))"
    ) shouldBe
    "def param1 = ctx.name; def safe1 = null; " +
    "try { safe1 = (param1 != null ? (def)(Long.parseLong(param1).longValue()) : null); } " +
    "catch (Exception e) {} ctx.c = safe1"
  }

  /** 🔴 The sweep the fix owes (spec §3, task 3): once the safe cast is hoisted, is EVERY contexted
    * processor rendering an expression? Asked of a corpus that mixes every emitter that can produce
    * a statement — the safe cast, a CASE, an arithmetic local, a function chain — because a single
    * remaining statement producer would put `assemble` back in the business of splicing.
    *
    * Deliberately a GUARD and not an assumption: if a future emitter renders a statement in a
    * processor, this reddens here rather than in a customer's cluster.
    */
  it should "render every computed column as an EXPRESSION once the prologue is hoisted" in {
    forAll(
      Table(
        "ddl",
        "CREATE TABLE t (name KEYWORD, c BIGINT SCRIPT AS (TRY_CAST(name AS BIGINT)))",
        "CREATE TABLE t (name KEYWORD, n INTEGER, c BIGINT SCRIPT AS (TRY_CAST(name AS BIGINT) + n))",
        "CREATE TABLE t (name KEYWORD, c KEYWORD SCRIPT AS (CONCAT(TRY_CAST(name AS BIGINT), 'x')))",
        "CREATE TABLE t (name KEYWORD, c BIGINT SCRIPT AS (COALESCE(TRY_CAST(name AS BIGINT), 0)))",
        "CREATE TABLE t (name KEYWORD, c BIGINT SCRIPT AS " +
        "(CASE WHEN name = 'x' THEN TRY_CAST(name AS BIGINT) ELSE 0 END))",
        "CREATE TABLE t (n INTEGER, c BIGINT SCRIPT AS ((CAST(n AS BIGINT) + 1) * (CAST(n AS BIGINT) + 2)))",
        "CREATE TABLE t (d DATE, c INTEGER SCRIPT AS (YEAR(d) * 100 + MONTH(d)))",
        "CREATE TABLE t (name KEYWORD, c KEYWORD SCRIPT AS (UPPER('a;b')))",
        "CREATE TABLE t (name KEYWORD, c KEYWORD SCRIPT AS (REPLACE(name, ';', 'x')))",
        "CREATE TABLE t (d DATE, c KEYWORD SCRIPT AS (DATE_FORMAT(d, 'yyyy')))",
        "CREATE TABLE t (d DATE, c DATE SCRIPT AS (DATE_TRUNC(d, MONTH)))",
        "CREATE TABLE t (name KEYWORD, c BIGINT SCRIPT AS (LENGTH(name) + 1))",
        "CREATE TABLE t (name KEYWORD, c BIGINT SCRIPT AS (NULLIF(LENGTH(name), 0)))"
      )
    ) { ddl =>
      val source = sourceOf(ddl)
      val code = literalFree(source)
      val at = assignmentAt(source)
      withClue(s"[$ddl] -> [$source] ") {
        at should be >= 0
        code.substring(at + "ctx.c = ".length) should not include ";"
        val before = code.substring(0, at)
        before.count(_ == '{') shouldBe before.count(_ == '}')
        // 🔴 And no `return ` anywhere, which is what lets `assemble` drop the arm that stripped
        // one (issue #382). A `return` in a processor script leaves the WHOLE script, so it can
        // never be part of a computed column's value; the only emitter of the token is the safe
        // cast's context-FREE rendering, which `fromScript` cannot reach. Asserted as a PROPERTY
        // of every shape rather than argued in a comment alone.
        """\breturn\b""".r.findFirstIn(code) shouldBe None
      }
    }
  }

  // -- issue #382, F3: a `}` inside a COMMENT is not a statement boundary -----------------------

  /** 🔴 #382 widened `ScriptTarget.of`'s boundary set with `}` — a hoisted `try { … } catch
    * (Exception e) {} ` is a statement with no `;`, so the assignment that follows one is preceded
    * by `} ` — and a `}` inside a Painless COMMENT was then read as code. Found by independent
    * review; MEASURED, `of` on `ctx.c = 1` followed by a block comment holding `} ctx.zzz = 9`:
    *
    * {{{
    *   before #382   Some(c)
    *   after  #382   Some(zzz)     <- the processor claims another column's identity
    * }}}
    *
    * `IngestPipeline.diff` keys a processor by this, so a wrong identity is a COLLISION, and the
    * map that indexes them DROPS the loser. Comments are reachable from SQL today: both `ALTER
    * PIPELINE … ADD PROCESSOR SCRIPT(source = '…')` and `CREATE PIPELINE … WITH PROCESSORS
    * (SCRIPT(…))` take a hand-authored source.
    *
    * Fixed at the seam that already answers the same question for string literals — `is this
    * character CODE?` — rather than by narrowing the regex, so the two answers cannot drift.
    *
    * ⚠️ The two rows whose comment sits BEFORE the assignment used to answer `None` (no boundary
    * was recognised at all, so the caller fell back to a content-addressed identity). They answer
    * correctly now; that is a bonus, not the point, and they are here so a future narrowing of the
    * scanner is visible.
    */
  private val commentShapes = Table(
    ("source", "why"),
    ("ctx.c = 1 " + blockComment("} ctx.zzz = 9"), "a `}` in a block comment AFTER the assignment"),
    ("ctx.c = 1 // } ctx.zzz = 9", "a `}` in a line comment"),
    (blockComment("ctx.zzz = 9") + " ctx.c = 1", "a whole assignment inside a block comment"),
    ("// ctx.zzz = 1\nctx.c = 2", "a whole assignment inside a line comment"),
    (
      "ctx.c = 1 " + blockComment("a ' b"),
      "an apostrophe in a comment, which must not open a literal"
    ),
    ("ctx.c = 1 /* unterminated", "an unterminated block comment swallows the rest"),
    ("def p = ctx.a; ctx.c = p", "the control: no comment at all"),
    (
      "def safe1 = null; try { safe1 = 1; } catch (Exception e) {} ctx.c = safe1",
      "the control: the `}` boundary #382 added, which must keep working"
    ),
    (
      "if (ctx.s != null) { ctx.flag = true } ctx.c = 2",
      "a FOREIGN source whose assignment follows a real block"
    )
  )

  "a `}` inside a comment" should "not be read as a statement boundary" in {
    forAll(commentShapes) { (source, why) =>
      withClue(s"[$source] -- $why ")(ScriptTarget.of(source).map(_.take(1)) shouldBe Some("c"))
    }
  }

  /** The counter-property: blanking comments must not blank CODE. A `/` that opens nothing is
    * division, and a comment delimiter inside a STRING LITERAL is data.
    */
  it should "leave a bare `/` and a delimiter inside a literal alone" in {
    forAll(
      Table(
        ("source", "expected"),
        ("ctx.c = 1 / 2", Some("c")),
        ("ctx.c = \"/* } ctx.zzz = 9 */\"", Some("c")),
        ("ctx.c = '// } ctx.zzz = 9'", Some("c")),
        // ... and a source this object did NOT write still answers None, so the caller keeps its
        // content-addressed fallback rather than being handed an invented identity
        ("def x = 1", None)
      )
    )((source, expected) => withClue(s"[$source] ")(ScriptTarget.of(source) shouldBe expected))
  }

}
