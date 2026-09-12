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
import app.softnetwork.elastic.sql.schema.{Column, Table => SchemaTable}
import app.softnetwork.elastic.sql.`type`.SQLTypes
import app.softnetwork.elastic.sql.{toRegex, PainlessContext, PainlessContextType}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** Story BIDC-8, round 10 — what a SQL `LIKE` pattern MEANS, in one place.
  *
  * 🔴 HIGH-2. `toRegex` used to translate `%` and `_` and nothing else, so every other regex
  * metacharacter kept its REGEX meaning — and since a pattern only becomes a regex on SOME paths,
  * one SQL statement had three different readings. MEASURED on live ES 8.18.3 over documents `A.B`
  * and `AXB1`:
  *
  *   - `status LIKE 'A.B%'` (native `regexp`) matched BOTH — `.` was any character;
  *   - `UPPER(status) LIKE 'A.B%'` (string-method fast path) matched only `A.B` — `.` was literal;
  *   - `UPPER(status) LIKE 'A.B%1'` (Painless regex) matched only `AXB1`.
  *
  * In SQL only `%` and `_` are wildcards. Escaping in the SHARED translation makes all three agree,
  * which is why the fix belongs there and not in the Painless emitter: fixing it only there would
  * have produced a THIRD semantics.
  *
  * ⚠️ USER-VISIBLE, 0.23.0 release note: `LIKE 'A.B%'` no longer matches `AXB1`.
  */
class LikePatternSpec extends AnyFlatSpec with Matchers {

  private val schema: SchemaTable = SchemaTable(
    "t",
    columns = List(Column("status", SQLTypes.Keyword), Column("amount", SQLTypes.Double))
  )

  private def scriptOf(sql: String): String =
    Parser(sql) match {
      case Right(ss: SingleSearch) =>
        val ctx = PainlessContext(context = PainlessContextType.Query)
        val body = ss
          .update(Some(schema))
          .where
          .flatMap(_.criteria)
          .getOrElse(fail(s"[$sql] has no WHERE criteria"))
          .painless(Some(ctx))
        s"$ctx$body"
      case other => fail(s"[$sql] expected a SingleSearch, got $other")
    }

  "toRegex" should "treat %% and _ as the ONLY wildcards" in {
    toRegex("A%") shouldBe "A.*"
    toRegex("A_B") shouldBe "A.B"
    toRegex("%A%") shouldBe ".*A.*"
  }

  it should "escape every other regex metacharacter, so a literal stays a literal" in {
    // Each of these was a WILDCARD before the fix, in the native `regexp` query as well as in the
    // scripted form — `A.B` matched `AXB`.
    toRegex("A.B") shouldBe "A\\.B"
    toRegex("A+B") shouldBe "A\\+B"
    toRegex("A*B") shouldBe "A\\*B"
    toRegex("A|B") shouldBe "A\\|B"
    toRegex("A(B)") shouldBe "A\\(B\\)"
    toRegex("A[B]") shouldBe "A\\[B\\]"
    toRegex("A{2}") shouldBe "A\\{2\\}"
    toRegex("A^B$") shouldBe "A\\^B\\$"
    toRegex("A?B") shouldBe "A\\?B"
    toRegex("A-B") shouldBe "A\\-B"
    toRegex("""A\B""") shouldBe """A\\B"""
    // Lucene's regexp grammar has metacharacters Java's does not; every one of these is also a
    // legal Java backslash escape, so ONE escaping rule serves both engines.
    toRegex("A#B@C&D<E>F~G") shouldBe "A\\#B\\@C\\&D\\<E\\>F\\~G"
    // … and a wildcard still survives beside them.
    toRegex("A.B%") shouldBe "A\\.B.*"
  }

  /** 🔴 HIGH-1 — the documented rule used to be "only `%` patterns are safe", which is wrong in
    * both directions: `'A%B'` is pure `%` and still needs a regex, and a pure `_` pattern needs one
    * too. The real rule is: NO `_`, and `%` only at the ends.
    */
  "the string-method fast path" should "be taken exactly when the pattern has no _ and % only at the ends" in {
    def usesRegex(pattern: String): Boolean =
      scriptOf(s"SELECT id FROM t WHERE UPPER(status) LIKE '$pattern'").contains("==~")

    usesRegex("A%") shouldBe false // startsWith
    usesRegex("%A") shouldBe false // endsWith
    usesRegex("%A%") shouldBe false // contains
    usesRegex("A") shouldBe false // equals
    usesRegex("") shouldBe false // equals("") — MEDIUM-1
    usesRegex("%") shouldBe false // endsWith("")
    usesRegex("%%") shouldBe false // contains("")

    usesRegex("A%B") shouldBe true // pure `%`, still a regex — the falsified claim
    usesRegex("A_B") shouldBe true // `_` always needs one
    usesRegex("_") shouldBe true
    usesRegex("%A%B%") shouldBe true
  }

  /** 🔴 MEDIUM-1 — the empty pattern used to emit `left1 ==~ //`, where `//` opens a Painless
    * COMMENT: `unexpected character [//))))]` on live ES 8.18.3, while the native `status LIKE ''`
    * answered `[]`.
    */
  "an empty LIKE pattern" should "compare for equality, not open a comment" in {
    val script = scriptOf("SELECT id FROM t WHERE UPPER(status) LIKE ''")
    script should include("""equals("")""")
    script should not include "==~"
    script should not include "//"
  }
}
