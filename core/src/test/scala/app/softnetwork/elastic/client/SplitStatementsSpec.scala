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

package app.softnetwork.elastic.client

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** `GatewayApi.run`'s multi-statement contract was documented but dead: the old `split(";\\s*$")`
  * anchors to the end of the (newline-free) input, so it could only strip a trailing `;` —
  * measured: `"SELECT 1; SELECT 2".split(";\\s*$")` yields ONE element with the interior `;`
  * intact, and the lenient parser then silently executed only the first statement.
  * `splitStatements` makes the documented behaviour real, and quote-aware.
  */
class SplitStatementsSpec extends AnyFlatSpec with Matchers {

  "splitStatements" should "split two statements on a top-level semicolon" in {
    GatewayApi.splitStatements("SELECT 1; SELECT 2") shouldBe List("SELECT 1", "SELECT 2")
  }

  it should "return a single statement unchanged" in {
    GatewayApi.splitStatements("SELECT * FROM t WHERE a = 1") shouldBe
    List("SELECT * FROM t WHERE a = 1")
  }

  it should "drop trailing and doubled semicolons" in {
    GatewayApi.splitStatements("SELECT 1;") shouldBe List("SELECT 1")
    GatewayApi.splitStatements("SELECT 1;;") shouldBe List("SELECT 1")
    GatewayApi.splitStatements("SELECT 1; ; SELECT 2 ;") shouldBe List("SELECT 1", "SELECT 2")
  }

  it should "not split on a semicolon inside a single-quoted literal" in {
    GatewayApi.splitStatements("SELECT * FROM t WHERE x = 'a;b'; SELECT 2") shouldBe
    List("SELECT * FROM t WHERE x = 'a;b'", "SELECT 2")
  }

  it should "not split on a semicolon inside a double-quoted identifier" in {
    GatewayApi.splitStatements("""SELECT "a;b" FROM t; SELECT 2""") shouldBe
    List("""SELECT "a;b" FROM t""", "SELECT 2")
  }

  // The grammar's literal rule is `([^'\\]|\\.)*` — backslash escapes a quote, so the literal
  // below is `a'b` and the quote does NOT close at the escaped `'`.
  it should "honor a backslash-escaped quote inside a literal" in {
    GatewayApi.splitStatements("""SELECT * FROM t WHERE x = 'a\'b;c'; SELECT 2""") shouldBe
    List("""SELECT * FROM t WHERE x = 'a\'b;c'""", "SELECT 2")
  }

  it should "treat an escaped backslash before the closing quote as closing" in {
    // `'a\\'` is the one-character-plus-backslash literal, properly closed: the `;` splits.
    GatewayApi.splitStatements("""SELECT '<a\\'; SELECT 2""".replace("<", "")) shouldBe
    List("""SELECT 'a\\'""", "SELECT 2")
  }

  it should "consume to the end on an unterminated quote and let the parser report it" in {
    GatewayApi.splitStatements("SELECT * FROM t WHERE x = 'oops; SELECT 2") shouldBe
    List("SELECT * FROM t WHERE x = 'oops; SELECT 2")
  }

  it should "return Nil for empty or semicolon-only input" in {
    GatewayApi.splitStatements("") shouldBe Nil
    GatewayApi.splitStatements("  ;;  ; ") shouldBe Nil
  }

  // ── `--` comments: the REPL batch path feeds RAW script text, with comments and newlines ──

  it should "not let an apostrophe in a comment open a phantom quote" in {
    GatewayApi.splitStatements("-- don't split here\nSELECT 1;\nSELECT 2;") shouldBe
    List("SELECT 1", "SELECT 2")
  }

  it should "not split on a semicolon inside a comment" in {
    GatewayApi.splitStatements("SELECT 1; -- note; not a boundary\nSELECT 2;") shouldBe
    List("SELECT 1", "SELECT 2")
  }

  it should "keep a double dash inside a literal as data" in {
    // The line-based pre-normalization truncates this literal; the scanner must not.
    GatewayApi.splitStatements("SELECT * FROM t WHERE sku = 'AB--12'; SELECT 2") shouldBe
    List("SELECT * FROM t WHERE sku = 'AB--12'", "SELECT 2")
  }

  it should "handle a comment on the last line without a trailing newline" in {
    GatewayApi.splitStatements("SELECT 1; -- done") shouldBe List("SELECT 1")
  }
}
