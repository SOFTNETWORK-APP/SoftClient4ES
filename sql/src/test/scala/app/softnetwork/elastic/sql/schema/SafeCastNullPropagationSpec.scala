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

/** Issue #382, the lead's Ruling B: a FAILED safe cast must be a NULL operand, not the text
  * `"null"` and not a swallowed exception.
  *
  * A `TRY_CAST` hoists `def safe1 = null; try { safe1 = …; } catch (Exception e) {}` into the
  * prologue, so the value the call reads is null exactly when the conversion failed — while the
  * COLUMN it came from is perfectly non-null. `FunctionN.painless` guarded the column's parameter
  * and nothing else, so the null went straight through. MEASURED on real Elasticsearch 8.18.3 over
  * `s = 'abc'`:
  *
  * {{{
  *   CONCAT(TRY_CAST(s AS BIGINT), 'x')   stored  "nullx"   <- a wrong VALUE, HTTP 200
  *   CONCAT('x', TRY_CAST(s AS BIGINT))   stored  "xnull"
  *   ABS / ROUND / SUBSTRING / TRIM /
  *   LOWER / REPLACE / LENGTH  over it    threw an NPE, swallowed by `ignore_failure: true`
  *                                         => the column is silently ABSENT
  * }}}
  *
  * 🔴 The semantics are the engine's OWN, not a third set invented here. `CONCAT(s, 'x')` over a
  * null `s` already emits `(param1 == null) ? null : …` — ANSI: a NULL operand makes the whole call
  * NULL. A failed safe cast IS a null operand, so it now answers the same way. (The dialects that
  * SKIP a null fragment were considered and rejected: this engine has one rule for a null operand,
  * and a second one would have to be explained at every venue.)
  *
  * 🔴 The raw parameter is KEPT in the guard beside the derived reference, and that is not
  * belt-and-braces. A prologue declaration is evaluated UNCONDITIONALLY, so a derived reference
  * cannot stand in for its source where the derivation itself throws on a null: the ingest temporal
  * base (`param1 instanceof String ? LocalDate.parse(param1, …) : Instant.ofEpochMilli(param1)`)
  * NPEs before any guard is reached. Replacing the reference rather than adding to it reddened
  * `IngestTemporalSpec` and `DateDocValueEmissionSpec` — measured, which is why the extra test is
  * scoped to `Function.rendersNullOnFailure`.
  *
  * Executed in `GatewayApiIntegrationSpec` ("make a failed safe cast NULL, not the text ..."): the
  * defect is a wrong VALUE that Elasticsearch accepts, so only a cluster can settle it.
  */
class SafeCastNullPropagationSpec extends AnyFlatSpec with Matchers with TableDrivenPropertyChecks {

  private def processorOf(ddl: String): String =
    Parser(ddl) match {
      case Right(ct: CreateTable) =>
        ct.schema.columns
          .find(_.name == "c")
          .flatMap(_.script)
          .map(_.source)
          .getOrElse(fail(s"no script processor for [c] in [$ddl]"))
      case other => fail(s"[$ddl] expected a CreateTable, got $other")
    }

  private def ddlOf(expr: String, columnType: String): String =
    s"CREATE TABLE t (s KEYWORD, n INTEGER, c $columnType SCRIPT AS ($expr))"

  /** The name the hoisted safe cast's value reaches the call under — the last `def paramN = …`
    * declared before the assignment. Derived from the source rather than hard-coded, so the rows
    * below assert the RULE and not one numbering.
    */
  private def callParams(source: String): List[String] =
    """def (param\d+) =""".r.findAllMatchIn(source).map(_.group(1)).toList

  /** Every shape measured as broken on 8.18.3, in one table: the two CONCAT orientations (a wrong
    * VALUE) and the seven that threw (a silently absent column). One repair covers all nine, which
    * is the finding — they share a mechanism, not a symptom.
    */
  private val shapes = Table(
    ("expr", "columnType"),
    ("CONCAT(TRY_CAST(s AS BIGINT), 'x')", "KEYWORD"),
    ("CONCAT('x', TRY_CAST(s AS BIGINT))", "KEYWORD"),
    ("ABS(TRY_CAST(s AS BIGINT))", "DOUBLE"),
    ("ROUND(TRY_CAST(s AS DOUBLE), 1)", "DOUBLE"),
    ("SUBSTRING(TRY_CAST(s AS KEYWORD), 1, 2)", "KEYWORD"),
    ("TRIM(TRY_CAST(s AS KEYWORD))", "KEYWORD"),
    ("LOWER(TRY_CAST(s AS KEYWORD))", "KEYWORD"),
    ("REPLACE(TRY_CAST(s AS KEYWORD), 'a', 'b')", "KEYWORD"),
    ("LENGTH(TRY_CAST(s AS KEYWORD))", "BIGINT")
  )

  "a function over a FAILED safe cast" should "guard the reference the call actually reads" in {
    forAll(shapes) { (expr, columnType) =>
      val source = processorOf(ddlOf(expr, columnType))
      val guard = source.substring(source.indexOf("ctx.c = "))
      val params = callParams(source)
      withClue(s"[$expr] -> [$source] ") {
        // the safe cast really was hoisted -- without this the rest is about a different script
        source should include("try { safe1")
        // ... and the guard names the LAST parameter, i.e. the one derived from `safe1`, rather
        // than stopping at `param1` (the raw `ctx.s`, which is NOT null on the failing row)
        params.size should be >= 2
        guard should include(s"${params.last} == null")
      }
    }
  }

  /** The control: a function over a PLAIN cast, whose derived parameter can only be null when the
    * column is, must be byte-identical. The repair is scoped to `rendersNullOnFailure`, and this is
    * what says so — restore the scope to "every argument" and this reddens (measured: it also
    * reddens `IngestTemporalSpec` and `DateDocValueEmissionSpec`).
    */
  it should "leave a plain CAST, and a bare column, byte-identical" in {
    forAll(
      Table(
        ("ddl", "expected"),
        (
          ddlOf("LENGTH(CAST(s AS KEYWORD))", "BIGINT"),
          "def param1 = ctx.s; def param2 = (param1 != null ? String.valueOf(param1) : null); " +
          "ctx.c = (param1 == null) ? null : param2.length()"
        ),
        (
          ddlOf("CONCAT(s, 'x')", "KEYWORD"),
          "def param1 = ctx.s; ctx.c = (param1 == null) ? null : String.valueOf(param1) + \"x\""
        ),
        (
          ddlOf("UPPER(s)", "KEYWORD"),
          "def param1 = ctx.s; ctx.c = (param1 == null) ? null : param1.toUpperCase()"
        )
      )
    )((ddl, expected) => withClue(s"[$ddl] ")(processorOf(ddl) shouldBe expected))
  }

  /** The byte pin for the two CONCAT orientations, so the repair itself cannot drift unnoticed —
    * and so a reader can see that the raw parameter is still tested BESIDE the derived one.
    */
  it should "assemble the filed CONCAT shapes exactly (#382 Ruling B)" in {
    processorOf(ddlOf("CONCAT(TRY_CAST(s AS BIGINT), 'x')", "KEYWORD")) shouldBe
    "def param1 = ctx.s; def safe1 = null; " +
    "try { safe1 = (param1 != null ? (def)(Long.parseLong(param1).longValue()) : null); } " +
    "catch (Exception e) {} def param2 = safe1; " +
    "def param3 = (param2 != null ? String.valueOf(param2) : null); " +
    "ctx.c = (param1 == null || param3 == null) ? null : String.valueOf(param3) + \"x\""

    processorOf(ddlOf("CONCAT('x', TRY_CAST(s AS BIGINT))", "KEYWORD")) shouldBe
    "def param1 = ctx.s; def safe1 = null; " +
    "try { safe1 = (param1 != null ? (def)(Long.parseLong(param1).longValue()) : null); } " +
    "catch (Exception e) {} def param2 = safe1; " +
    "def param3 = (param2 != null ? String.valueOf(param2) : null); " +
    "ctx.c = (param1 == null || param3 == null) ? null : \"x\" + String.valueOf(param3)"
  }

  /** The same three shapes, now that the gap they RECORDED is closed (issue #382, the NULLIF
    * addendum). An earlier round of this branch published *"Known gap: NULLIF over any cast — safe
    * or plain — is still rejected at CREATE TABLE"* and pinned it here as a characterisation; the
    * gap was `NullIf.argTypes` reading each operand's `out` (the COLUMN's type) instead of what it
    * RENDERS, so a cast of a KEYWORD reported KEYWORD and the string arm's `0 != null` guard
    * reached Elasticsearch. 🔴 A characterisation that survives its own fix is a test MANDATING a
    * defect, so it flips rather than being deleted — and it still asserts the same INVARIANT, that
    * the two cast spellings agree, which is what makes `TRY_CAST` no worse than `CAST`.
    *
    * Executed in `GatewayApiIntegrationSpec` ("compute NULLIF over a cast, not compare a string"),
    * because this module cannot see a compile error.
    */
  it should "emit NULLIF over a SAFE cast exactly as over a plain one (#382)" in {
    val safe = processorOf(ddlOf("NULLIF(TRY_CAST(s AS BIGINT), 0)", "BIGINT"))
    val plain = processorOf(ddlOf("NULLIF(CAST(s AS BIGINT), 0)", "BIGINT"))
    val bare = processorOf(ddlOf("NULLIF(n, 0)", "BIGINT"))
    withClue(s"safe=[$safe] plain=[$plain] bare=[$bare] ") {
      // a cast that RENDERS a number takes the numeric arm, exactly like the bare numeric column
      safe should not include "compareTo"
      plain should not include "compareTo"
      bare should not include "compareTo"
      safe should include("== 0 ? null : ")
      plain should include("== 0 ? null : ")
      // ... and the guard Painless refuses over a primitive literal is gone from both
      safe should not include "0 != null"
      plain should not include "0 != null"
    }
  }
}
