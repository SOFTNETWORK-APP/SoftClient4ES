package app.softnetwork.elastic.sql.parser

import app.softnetwork.elastic.sql.{unescapeStringLiteral, StringValue}
import app.softnetwork.elastic.sql.query.{SingleSearch, Statement}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** Issue #274: SQL-standard `''` escaping was unsupported anywhere in the grammar, so any literal
  * containing an apostrophe was a parse error on every surface. Story 21.5 also folds in the
  * double-quoted twin (`""`, lead ruling 2026-09-07 on OQ-2), which was left unowned when story
  * 21.1 shipped.
  *
  * This spec pins the accepted forms, the VALUES they produce, the round trip, the
  * identifier-vs-string disambiguation the `""` fold-in must not disturb, and — critically — the
  * three hand-written quote-aware scanners, which needed NO change (a doubled delimiter closes and
  * immediately re-opens the run, so their state machines stay balanced) and must keep needing none.
  *
  * Discipline: never pin a parser rejection message (it names an internal production and is
  * unstable); assert the VALUE, not merely that something parsed; and every rejection assertion
  * also excludes story 21.4's boundary catch, without which a crashing parser still returns a
  * `Left` whose message CONTAINS the expected text.
  */
class StringLiteralEscapingSpec extends AnyFlatSpec with Matchers {

  private def statementOf(sql: String): Statement =
    Parser(sql) match {
      case Right(s)  => s
      case Left(err) => fail(s"[$sql] rejected: ${err.msg}")
    }

  /** A rejection that is a real grammar rejection, not story 21.4's `NonFatal` boundary catch. */
  private def rejects(sql: String): Unit = {
    withClue(s"[$sql] ") {
      noException should be thrownBy Parser(sql)
      Parser(sql) match {
        case Left(err) => err.msg should not startWith Parser.InternalParseFailure
        case Right(s)  => fail(s"expected a rejection, parsed as [${s.sql}]")
      }
    }
    ()
  }

  private def firstValue(sql: String): String =
    Parser(sql) match {
      case Right(s: SingleSearch) =>
        s.select.fields.head.identifier.functions
          .collectFirst { case v: StringValue => v.value }
          .getOrElse(fail(s"[$sql] no StringValue in the select field's function chain"))
      case other => fail(s"[$sql] expected a SingleSearch, got $other")
    }

  private def firstName(sql: String): String =
    Parser(sql) match {
      case Right(s: SingleSearch) => s.select.fields.head.identifier.name
      case other                  => fail(s"[$sql] expected a SingleSearch, got $other")
    }

  // -- A - the doubled single quote is accepted in every position -------------
  "a doubled single quote" should "be accepted in every literal position" in {
    val rows = Seq(
      "SELECT 'O''Brien' FROM t",
      "SELECT name FROM t WHERE name = 'O''Brien'",
      "SELECT name FROM t WHERE name IN ('a''b','c')",
      "SELECT name FROM t WHERE name LIKE 'O''%'",
      "SELECT UPPER('it''s') FROM t",
      "SELECT CONCAT('a''b', name) FROM t",
      "INSERT INTO t (name) VALUES ('O''Brien')",
      "UPDATE t SET name = 'O''Brien' WHERE id = 1",
      "DELETE FROM t WHERE name = 'O''Brien'",
      "COPY INTO t FROM 'a''b.json'",
      "CREATE TABLE t (name VARCHAR DEFAULT 'O''Brien')",
      "ALTER TABLE users ALTER COLUMN name SET COMMENT 'it''s here'",
      "CREATE TABLE t (c VARCHAR SCRIPT AS (UPPER('a'')b')))"
    )
    rows.foreach { sql =>
      withClue(s"[$sql] ") { Parser(sql).isRight shouldBe true }
    }
  }

  it should "round-trip to an EQUAL AST for every accepted position" in {
    Seq(
      "SELECT 'O''Brien' FROM t",
      "SELECT name FROM t WHERE name = 'O''Brien'",
      "SELECT name FROM t WHERE name IN ('a''b','c')",
      "SELECT UPPER('it''s') FROM t",
      "INSERT INTO t (name) VALUES ('O''Brien')",
      "UPDATE t SET name = 'O''Brien' WHERE id = 1",
      "DELETE FROM t WHERE name = 'O''Brien'",
      "COPY INTO t FROM 'a''b.json'",
      "CREATE TABLE t (name VARCHAR DEFAULT 'O''Brien')",
      "ALTER TABLE users ALTER COLUMN name SET COMMENT 'it''s here'",
      "CREATE TABLE t (c VARCHAR SCRIPT AS (UPPER('a'')b')))"
    ).foreach { sql =>
      val stmt = statementOf(sql)
      withClue(s"[$sql] rendered as [${stmt.sql}] ") {
        Parser(stmt.sql) shouldBe Right(stmt)
      }
    }
  }

  it should "produce the right VALUE, not merely parse" in {
    firstValue("SELECT 'O''Brien' FROM t") shouldBe "O'Brien"
    firstValue("SELECT '''' FROM t") shouldBe "'"
    firstValue("SELECT '''a' FROM t") shouldBe "'a"
    firstValue("SELECT 'a''' FROM t") shouldBe "a'"
    firstValue("SELECT 'a''''b' FROM t") shouldBe "a''b"
    firstValue("SELECT '' FROM t") shouldBe ""
  }

  // -- A2 - the doubled DOUBLE quote (OQ-2 fold-in) ---------------------------
  // Widening `literal`'s double-quote branch cannot flip a reading: in identifier position
  // `quotedIdentifier` already accepted `""` (story 21.1) and still wins by production order; in
  // value position `literal` wins and the shape was a parse error before.
  "a doubled double quote" should "be accepted as a string literal in value positions" in {
    val rows = Seq(
      """SELECT name FROM t WHERE name = "x""y"""",
      """SELECT name FROM t WHERE name IN ("a""b","c")""",
      """UPDATE t SET name = "O""Brien" WHERE id = 1""",
      """DELETE FROM t WHERE name = "O""Brien""""
    )
    rows.foreach { sql =>
      withClue(s"[$sql] ") { Parser(sql).isRight shouldBe true }
    }
    // The VALUE, not merely acceptance: the render is the canonical single-quoted form, and an
    // embedded DOUBLE quote needs no escaping there — only `'` and `\` do.
    statementOf("""SELECT name FROM t WHERE name = "x""y"""").sql should include("""'x"y'""")
  }

  it should "keep a doubled-double-quote lexeme an IDENTIFIER in name position (the disambiguation pin)" in {
    // The pair that fixes the boundary: the SAME lexeme, two positions, two readings, decided by
    // production order and NOT by the literal regex. `SELECT "a""b" FROM t` already parsed as a
    // column before this story (story 21.1 taught `quotedNameRegex` the doubled escape); widening
    // `literal` must not disturb it.
    firstName("""SELECT "a""b" FROM t""") shouldBe """a"b"""
    Parser("""SELECT "a""b" FROM t""") match {
      case Right(s: SingleSearch) =>
        s.select.fields.head.identifier.functions
          .collectFirst { case v: StringValue => v.value } shouldBe None
      case other => fail(s"unexpected $other")
    }
    // ... and in OPERAND position too (story 21.1 AD-13): a quoted lexeme inside a function is the
    // COLUMN, not the string. The wrapping identifier is nameless there, so the proof is the
    // emitted Painless — a `doc[...]` read, never a constant.
    Parser("""SELECT UPPER("a""b") FROM t""") match {
      case Right(s: SingleSearch) =>
        s.select.fields.head.identifier.painless(None) should include("""doc['a"b']""")
      case other => fail(s"unexpected $other")
    }
  }

  it should "keep an EMPTY double-quoted lexeme the empty STRING, not a nameless column" in {
    // `quotedNameRegex`'s content quantifier is `+` on purpose, so `""` falls through to `literal`.
    firstValue("""SELECT "" FROM t""") shouldBe ""
  }

  it should "round-trip a double-quoted string literal (rendered in the canonical single-quote form)" in {
    Seq(
      """SELECT name FROM t WHERE name = "x""y"""",
      """UPDATE t SET name = "O""Brien" WHERE id = 1""",
      """DELETE FROM t WHERE name = "O""Brien""""
    ).foreach { sql =>
      val stmt = statementOf(sql)
      withClue(s"[$sql] rendered as [${stmt.sql}] ") {
        Parser(stmt.sql) shouldBe Right(stmt)
      }
    }
  }

  // -- B - the legacy backslash form is untouched ----------------------------
  "the legacy backslash escape" should "keep working exactly as before" in {
    firstValue("""SELECT 'it\'s' FROM t""") shouldBe "it's"
    firstValue("""SELECT 'C:\\' FROM t""") shouldBe """C:\"""
    // An unknown escape keeps its backslash verbatim - the two-replace chain this supersedes
    // behaved the same way, and COPY INTO paths depend on it.
    firstValue("""SELECT 'a\nb' FROM t""") shouldBe """a\nb"""
    Parser("""CREATE TABLE t (name VARCHAR DEFAULT 'O\'Brien')""").isRight shouldBe true
    Parser("""ALTER TABLE users ALTER COLUMN name SET COMMENT 'it\'s here'""").isRight shouldBe true
  }

  it should "render the backslash form and re-parse it (the render is deliberately unchanged)" in {
    val stmt = statementOf("SELECT 'O''Brien' FROM t")
    stmt.sql should include("""'O\'Brien'""")
    Parser(stmt.sql) shouldBe Right(stmt)
  }

  // -- B2 - the shared un-escaper, directly ----------------------------------
  // It is delimiter-parameterised so ONE scan serves both branches of `literal`. It deliberately
  // does NOT share `Parser.unquoteName`'s alphabet: an identifier un-escapes a backslash before ANY
  // character, a literal only before the delimiter or another backslash (AD-12).
  "unescapeStringLiteral" should "handle both accepted escapes in one left-to-right pass" in {
    unescapeStringLiteral("O''Brien", '\'') shouldBe "O'Brien"
    unescapeStringLiteral("""it\'s""", '\'') shouldBe "it's"
    unescapeStringLiteral("""C:\\""", '\'') shouldBe """C:\"""
    unescapeStringLiteral("""a\nb""", '\'') shouldBe """a\nb"""
    unescapeStringLiteral("""x""y""", '"') shouldBe """x"y"""
    unescapeStringLiteral("""a\"b""", '"') shouldBe """a"b"""
    unescapeStringLiteral("""a\nb""", '"') shouldBe """a\nb"""
    unescapeStringLiteral("", '\'') shouldBe ""
    // A trailing lone backslash is copied verbatim rather than eating the terminator.
    unescapeStringLiteral("""a\""", '\'') shouldBe """a\"""
  }

  // -- C - the three quote-aware SCANNERS (measured: none needed an edit) -----
  "Parser.normalize" should "not read a -- inside a doubled-quote literal as a comment" in {
    // If normalize mis-counted the doubled quote it would cut the line at `--` and the rest of the
    // statement would vanish - a silent truncation, not a parse error.
    firstValue("SELECT 'a''--b' FROM t") shouldBe "a'--b"
    firstValue("SELECT 'a''--b' FROM t -- trailing comment") shouldBe "a'--b"
    // The double-quoted twin: here the lexeme is a COLUMN, so the `--` must survive in its NAME.
    firstName("""SELECT "a""--b" FROM t""") shouldBe """a"--b"""
  }

  it should "survive a doubled quote in a MULTI-LINE statement" in {
    // The shape where a mis-counted quote silently TRUNCATES rather than failing: the `--` comment
    // stripper is line-based, so the newline is what makes the truncation invisible.
    val sql = "SELECT 'a''--b' AS c\nFROM t\nWHERE id = 1 -- tail\n"
    firstValue(sql) shouldBe "a'--b"
    statementOf(sql).sql should include("FROM t")
  }

  "Parser.scriptBody" should "not let a ')' inside a doubled-quote literal close the body early" in {
    val stmt = statementOf("CREATE TABLE t (c VARCHAR SCRIPT AS (UPPER('a'')b')))")
    Parser(stmt.sql) shouldBe Right(stmt)
  }

  it should "cover the OTHER scriptBody call site (ALTER COLUMN), which shares the scanner" in {
    val sql = "ALTER TABLE t ALTER COLUMN c SET SCRIPT AS (UPPER('a'')b'))"
    val stmt = statementOf(sql)
    Parser(stmt.sql) shouldBe Right(stmt)
  }

  // -- D - no regression on what already parsed -----------------------------
  "existing literal shapes" should "be unaffected" in {
    Seq(
      "SELECT 'a', 'b' FROM t",
      "SELECT 'a' FROM t WHERE x = 'b'",
      "SELECT '' FROM t WHERE x = '' AND y = 'z'",
      "SELECT 'a--b' FROM t",
      "SELECT DATE_PARSE('2024-01-01', 'yyyy-MM-dd') FROM t"
    ).foreach { sql =>
      withClue(s"[$sql] ") {
        val stmt = statementOf(sql)
        Parser(stmt.sql) shouldBe Right(stmt)
      }
    }
    // An odd trailing backslash still cannot terminate a literal - unchanged, and the reason the
    // renderer keeps escaping backslashes (AD-2).
    rejects("""SELECT 'C:\' FROM t""")
  }

  it should "keep a quoted identifier a column and a quoted alias an alias" in {
    firstName("""SELECT "category" FROM t""") shouldBe "category"
    firstName("""SELECT MAX("salary") FROM t""") shouldBe "salary"
    statementOf("""SELECT a AS "b" FROM t""").sql should include(""""b"""")
  }
}
