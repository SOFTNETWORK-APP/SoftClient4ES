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
  * 🔴 WHY THIS SPEC EXISTS. At the ARITHMETIC operand site the predicate is inert: no reachable SQL
  * shape renders an operand carrying a `;` inside a string literal, and the `try` arm never decides
  * anything either, because both Painless `try` emitters (`function/convert/package.scala`) always
  * emit a `;` as well -- so the `;` arm disqualifies first. Review PROVED that by mutation:
  * blinding the literal handling, and separately blinding the `try` arm, left every other spec
  * green. A guard nothing can see is not a guard, so the rule is exercised HERE, one case per arm,
  * with the inputs review actually falsified.
  *
  * 🔴 CORRECTED: the literal handling is NO LONGER inert anywhere. `splitStatements` shares this
  * scanner and `ScriptProcessor.fromScript` cuts an ingest script with it, where `UPPER('a;b')` IS
  * a reachable shape and a quote-blind split silently dropped the computed column (issue #373 --
  * see `ScriptProcessorAssemblySpec`). The `try` arm remains inert and stays pinned here as
  * characterisation.
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

  /** `splitStatements` is the same scanner used to CUT rather than to decide -- it is what
    * `ScriptProcessor.fromScript` splits an ingest script on. Two claims, and the second is the one
    * that carries the defect: it must agree with `String.split(";")` wherever no literal is
    * involved (so no existing emission moves), and it must not cut inside a literal.
    */
  private def split(rendered: String): List[String] =
    PainlessOperandForm.splitStatements(rendered).toList

  "splitting statements" should "agree with String.split where no literal is involved" in {
    forAll(
      Table(
        "rendered",
        "def a = 1; b",
        "def a = 1; def b = 2; c",
        "a",
        "",
        "a;",
        "a;;b",
        ";a",
        ";",
        ";;"
      )
    )(r => withClue(s"[$r]")(split(r) shouldBe r.split(";").toList))
  }

  it should "not cut inside a string literal" in {
    forAll(
      Table(
        ("rendered", "parts"),
        (
          """def a = ctx.n; "x;y".toUpperCase()""",
          List("""def a = ctx.n""", """ "x;y".toUpperCase()""")
        ),
        (""""a;b"""", List(""""a;b"""")),
        ("""'a;b'""", List("""'a;b'""")),
        ("""f("a;b"); g""", List("""f("a;b")""", """ g""")),
        ("""f("a\\"); x""", List("""f("a\\")""", """ x"""))
      )
    )((r, expected) => withClue(s"[$r]")(split(r) shouldBe expected))
  }

  /** The literal-bearing rows above are the ones a bare `split(";")` gets wrong; this states the
    * difference explicitly so the spec says WHY it exists, not merely what it expects.
    */
  it should "differ from String.split exactly where a literal holds the separator" in {
    val rendered = """def a = ctx.n; "x;y".toUpperCase()"""
    rendered.split(";").toList should have size 3
    split(rendered) should have size 2
  }

  /** `withoutLiterals` is the third user of the shared scanner. `ScriptTarget.of` recovers which
    * column an ingest script feeds by matching the LAST `ctx.<name> = `, and a literal is allowed
    * to CONTAIN that text, so the match has to run on code alone. Offsets and length must survive
    * the blanking, or the captured name would not be the one that was matched.
    */
  "blanking literals" should "erase their content and keep every offset" in {
    forAll(
      Table(
        ("rendered", "blanked"),
        ("""ctx.c = "; ctx.d = 1"""", "ctx.c = " + " " * 13),
        ("""a'b;c'd""", "a" + " " * 5 + "d"),
        ("""plain code""", """plain code"""),
        ("", "")
      )
    ) { (r, expected) =>
      withClue(s"[$r]") {
        PainlessOperandForm.withoutLiterals(r) shouldBe expected
        PainlessOperandForm.withoutLiterals(r).length shouldBe r.length
      }
    }
  }
}
