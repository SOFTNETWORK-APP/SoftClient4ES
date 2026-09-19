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

import app.softnetwork.elastic.sql.PainlessOperandForm
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks

/** `PainlessOperandForm.placeable` decides whether a rendered fragment may sit where an operand
  * goes. `ArithmeticExpression` falls back to a parameter when it answers false (issue #373, item
  * 5), and `PainlessOperandFormSpec` uses it as the tree-wide guard over rendered criteria.
  *
  * 🔴 WHY THIS SPEC EXISTS. At the emission sites the predicate is INERT today: no reachable SQL
  * shape renders an operand carrying a `;` inside a string literal, and the `try` arm never decides
  * anything either, because both Painless `try` emitters (`function/convert/package.scala`) always
  * emit a `;` as well -- so the `;` arm disqualifies first. Review PROVED that by mutation:
  * blinding the literal handling, and separately blinding the `try` arm, left every other spec
  * green. A guard nothing can see is not a guard, so the rule is exercised HERE, one case per arm,
  * with the inputs review actually falsified.
  *
  * 🔴 The two wrong answers are NOT symmetric, and every accepted limitation sits on the safe side
  * of that. Answering "statement" about an expression demotes the operand and drops its function
  * chain -- a SILENT wrong answer, the failure this predicate exists to prevent. Answering
  * "expression" about a statement splices it into an operand slot, and Elasticsearch answers
  * `compile error`. When in doubt, prefer the LOUD direction.
  */
class PainlessExpressionFormSpec extends AnyFlatSpec with Matchers with TableDrivenPropertyChecks {

  private def placeable(rendered: String): Boolean = PainlessOperandForm.placeable(rendered)

  "a rendering with no statement boundary" should "be placeable" in {
    forAll(
      Table(
        "rendered",
        """param1 + 1""",
        """param2 ? 1 : 0 + 1""",
        """(param1 != null ? (def)(((long) param1)) : null)""",
        """ZoneId.of('Z')""",
        ""
      )
    )(r => withClue(s"[$r]")(placeable(r) shouldBe true))
  }

  "a `;` or a `try` outside a literal" should "not be placeable" in {
    forAll(
      Table(
        "rendered",
        """def lv0 = x; y""",
        """try { x } catch (Exception e) {}""",
        """String.valueOf(try { x } catch (Exception e) { return null; })""",
        """foo("x") ; try { }""",
        """x ; """
      )
    )(r => withClue(s"[$r]")(placeable(r) shouldBe false))
  }

  /** 🔴 The arm that prevents the silent failure: a `;` a user wrote inside a format or a pattern
    * is DATA. Reading it as a separator demotes the operand and drops its chain.
    */
  "a `;` inside a string literal" should "be data, not a separator" in {
    forAll(
      Table(
        "rendered",
        """param1.replace(";", "x")""",
        """DateTimeFormatter.ofPattern("yyyy;MM")""",
        """x.foo('a;b')""",
        """foo("a\"b;c") + 1""",
        """foo("a\\;b")""",
        """foo("it's") + 1""",
        """foo("it's;ok") + 1""",
        """foo('say "hi"') + 1""",
        """foo('say "hi";') + 1""",
        """foo("try ") + 1""",
        """foo('try ') + 1"""
      )
    )(r => withClue(s"[$r]")(placeable(r) shouldBe true))
  }

  /** 🔴 The load-bearing case. In the emitted Painless the literal is `"a\\"` -- an ESCAPED
    * backslash -- so the second quote really does terminate the string and the `;` after it is
    * code. It only comes out right because the scan consumes `\\` plus the NEXT character as a
    * pair. A scan that special-cased only `\"` would read the closing quote as escaped, swallow the
    * rest of the script, miss that `;`, and splice a statement into an operand slot.
    */
  "an escaped backslash" should "close the literal, so a following `;` still counts" in {
    forAll(
      Table(
        ("rendered", "placeableForm"),
        ("""foo("a\\") ; x""", false), // the escape CLOSES the literal
        ("""foo("a\\;b")""", true) // same escape, `;` genuinely inside
      )
    )((r, expected) => withClue(s"[$r]")(placeable(r) shouldBe expected))
  }

  /** `try` counts only on a word boundary -- otherwise `entry `, `retry` and `registry` all contain
    * it, and every one of them would lose its chain.
    */
  "a `try` inside an identifier" should "not count" in {
    forAll(
      Table(
        "rendered",
        """entry + 1""",
        """myentry [entry ] + 1""",
        """retry + 1""",
        """a_try x""",
        """registry + 1"""
      )
    )(r => withClue(s"[$r]")(placeable(r) shouldBe true))
  }

  /** Characterisation, NOT aspiration: these are the accepted limitations, all LOUD-direction (a
    * compile error, never a wrong value). Pinned so nobody "fixes" one silently and trades a loud
    * failure for a quiet one.
    */
  "the accepted limitations" should "stay exactly as documented" in {
    forAll(
      Table(
        ("rendered", "placeableForm", "why"),
        ("""try{ x }""", true, "the arm keys on the spelling `try ` -- no space, not matched"),
        ("""try""", true, "bare `try` at the end of input is not matched"),
        ("""x.try foo""", false, "a dot is not a word char, so this one IS matched"),
        (""""unterminated ; literal""", true, "an unterminated literal swallows the `;`")
      )
    )((r, expected, why) => withClue(s"[$r] -- $why")(placeable(r) shouldBe expected))
  }

  /** Bounds safety only, no semantic claim: a trailing backslash or a lone one must not throw. The
    * escape step can advance past the end, and the loop guard is what catches it.
    */
  "a malformed rendering" should "answer without throwing" in {
    forAll(
      Table(
        "rendered",
        "foo(\"a\\",
        "\\",
        "'"
      )
    )(r => withClue(s"[$r]")(noException should be thrownBy placeable(r)))
  }

  "the locating variant" should "agree with the decision and name the spot" in {
    PainlessOperandForm.firstStatementAt("""param1 + 1""") shouldBe None
    PainlessOperandForm.firstStatementAt("""def lv0 = x; y""") shouldBe Some(11)
    PainlessOperandForm.firstStatementAt("""try { x }""") shouldBe Some(0)
  }
}
