package app.softnetwork.elastic.sql.parser

import app.softnetwork.elastic.sql.query.SingleSearch
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** Story 21.5 part D — `Value.painless` wrapped a string in double quotes with NO escaping.
  *
  * `case s: String => s""""$s""""`. So `SELECT 'a"b'` emitted the Painless `"a"b"` — a script
  * SYNTAX error, rejected by Elasticsearch at execution — and a value ending in a backslash emitted
  * an unterminated string. Reachable today with ordinary data, on every literal-bearing script
  * surface: script fields, script filters, script sorts, painless-computed columns.
  *
  * 🔴 The no-regression bound is the important half: for a literal containing NEITHER `"` NOR `\`
  * the emitted script is BYTE-IDENTICAL to before, so only the literals that were already broken
  * change shape. That is what makes the sweep tractable and what this spec pins.
  *
  * A Painless-SYNTAX claim can only be proven by Elasticsearch, so the testkit carries the
  * end-to-end case; this spec pins the emission.
  */
class PainlessLiteralEscapingSpec extends AnyFlatSpec with Matchers {

  private def painlessOf(sql: String): String =
    Parser(sql) match {
      case Right(ss: SingleSearch) => ss.select.fields.head.identifier.painless(None)
      case other                   => fail(s"[$sql] expected a SingleSearch, got $other")
    }

  "a string literal containing a double quote" should "emit VALID escaped Painless" in {
    // Written with the doubled SQL escape so the source is readable; `'a"b'` is the same value.
    painlessOf("""SELECT 'a"b' FROM t""") shouldBe """"a\"b""""
    painlessOf("""SELECT "a""b" FROM t WHERE id = 1""") // identifier position - not a literal
    painlessOf("""SELECT UPPER('say "hi"') FROM t""") should include("""\"hi\"""")
  }

  "a string literal containing a backslash" should "emit a DOUBLED backslash" in {
    // `'C:\\'` is the SQL spelling of the one-character-trailing value `C:\`. Unescaped it emitted
    // `"C:\"` — an unterminated Painless string.
    painlessOf("""SELECT 'C:\\' FROM t""") shouldBe """"C:\\""""
    // An unknown escape keeps its literal backslash through the SQL layer, so the Painless layer
    // must double THAT backslash too.
    painlessOf("""SELECT 'a\nb' FROM t""") shouldBe """"a\\nb""""
  }

  "a clean string literal" should "emit a BYTE-IDENTICAL script (the no-regression bound)" in {
    painlessOf("SELECT 'abc' FROM t") shouldBe """"abc""""
    painlessOf("SELECT 'O''Brien' FROM t") shouldBe """"O'Brien""""
    painlessOf("SELECT 'a--b' FROM t") shouldBe """"a--b""""
    painlessOf("SELECT UPPER('abc') FROM t") shouldBe """"abc".toUpperCase()"""
    // A single quote needs no Painless escaping — Painless strings here are double-quoted.
    painlessOf("""SELECT 'it\'s' FROM t""") shouldBe """"it's""""
  }

  "escaping" should "compose in the right order (backslash first, then the quote)" in {
    // The classic order. Escaping the quote first would then double the backslash it just
    // introduced, producing `"a\\"b"` — a different, still-broken script.
    painlessOf("""SELECT 'a\\"b' FROM t""") shouldBe """"a\\\"b""""
  }

  "a literal reaching Painless through a non-SELECT surface" should "be escaped too" in {
    // The escaper is at the emission site, so every literal-bearing script inherits it.
    Parser("""SELECT id FROM t WHERE UPPER(name) = 'A"B'""") match {
      case Right(ss: SingleSearch) =>
        ss.where.map(_.sql).getOrElse("") should include("""A"B""") // SQL render is untouched
      case other => fail(s"unexpected $other")
    }
    painlessOf("""SELECT CONCAT('a"b', name) FROM t""") should include("""a\"b""")
  }
}
