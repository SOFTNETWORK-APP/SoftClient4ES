package app.softnetwork.elastic.sql.parser

import app.softnetwork.elastic.sql.query.SingleSearch
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** AC-5 — the AST/render contract is a FIXED POINT over the whole quoting matrix.
  *
  * The reason it exists is concrete: `MaterializedViewExtension` runs `client.run(alter.sql)` and
  * writes the same string into a user-runnable artifact, so a render that drops, adds or re-reads
  * quoting changes which field is read — silently. `AlterTableRoundTripSpec` is the precedent; this
  * is the same assertion over the identifier surface.
  *
  * This suite is also what FOUND story 21.1 AD-13. `Identifier.sql` emits ONE canonical delimiter
  * (the ANSI double quote), so a backticked operand renders as `MAX("amount")`; while a
  * double-quoted lexeme in operand position was still read as a string, that render came back as
  * `MAX('amount')` and the fixed point failed on six of the rows below. A parse-only test cannot
  * see that — only re-parsing the render can.
  */
class QuotedIdentifierRoundTripSpec extends AnyFlatSpec with Matchers {

  private def fixedPoint(sql: String): Unit = {
    val parsed = Parser(sql)
    withClue(s"did not parse: [$sql] -> $parsed ") { parsed.isRight shouldBe true }
    val stmt = parsed.toOption.get
    withClue(s"rendering of [$sql] was [${stmt.sql}] which ") {
      Parser(stmt.sql) shouldBe Right(stmt)
    }
    ()
  }

  // Every ORDER BY row carries an EXPLICIT direction. A direction-less `ORDER BY c` renders an added
  // `ASC`, so the AST fixed point fails for a reason that has nothing to do with quoting.
  private val matrix = Seq(
    // bare — the render must NOT acquire quotes
    "SELECT category FROM bi_events",
    "SELECT e.category FROM bi_events e",
    "SELECT category AS c FROM bi_events",
    // quoted, both styles, every position
    "SELECT `category` FROM bi_events",
    "SELECT \"category\" FROM bi_events",
    "SELECT `e`.`category` FROM bi_events e",
    "SELECT e.`category` FROM bi_events e",
    "SELECT `e`.category FROM bi_events e",
    "SELECT `category` AS `c` FROM bi_events",
    "SELECT category AS \"c\" FROM bi_events",
    "SELECT id FROM bi_events WHERE `category` = 'a'",
    "SELECT id FROM bi_events WHERE `category` IS NOT NULL",
    "SELECT `category`, COUNT(id) AS n FROM bi_events GROUP BY `category`",
    "SELECT `category`, COUNT(id) AS n FROM bi_events GROUP BY `category` HAVING COUNT(id) > 1",
    "SELECT `category`, COUNT(`id`) AS n FROM bi_events GROUP BY `category` HAVING COUNT(`id`) > 1",
    "SELECT id FROM bi_events ORDER BY `event_ts` DESC",
    "SELECT id FROM bi_events ORDER BY `event_ts` DESC NULLS LAST",
    // operand positions — the rows AD-13 exists for
    "SELECT MAX(`amount`) AS m FROM bi_events",
    "SELECT MAX(\"amount\") AS m FROM bi_events",
    "SELECT UPPER(`category`) AS u FROM bi_events",
    "SELECT CAST(`amount` AS BIGINT) AS a FROM bi_events",
    "SELECT TRY_CAST(`amount` AS BIGINT) AS a FROM bi_events",
    "SELECT CONVERT(`amount`, BIGINT) AS a FROM bi_events",
    "SELECT CONVERT(BIGINT, `amount`) AS a FROM bi_events",
    "SELECT `amount`::BIGINT AS a FROM bi_events",
    "SELECT DATE_TRUNC(`event_ts`, MONTH) AS m FROM bi_events",
    "SELECT EXTRACT(YEAR FROM `event_ts`) AS y FROM bi_events",
    "SELECT (`amount` + 1) AS a FROM bi_events",
    "SELECT MAX(`amount` + 1) AS a FROM bi_events",
    "SELECT SUM(\"bi_events\".\"amount\") AS s FROM bi_events",
    "SELECT ROW_NUMBER() OVER (PARTITION BY `category` ORDER BY `amount` DESC) AS rn FROM bi_events",
    "SELECT COUNT(DISTINCT `category`) AS c FROM bi_events",
    // the value positions that must STAY strings — their render is a single-quoted literal
    "SELECT id FROM bi_events WHERE category = \"a\"",
    "SELECT DATE_PARSE('2024-01-01', 'yyyy-MM-dd') AS d FROM bi_events",
    "SELECT DATE_PARSE(`event_ts`, 'yyyy-MM-dd') AS d FROM bi_events",
    // names that REQUIRE quoting to be re-emittable at all
    "SELECT `select` FROM t",
    "SELECT `order`, `count` FROM t",
    "SELECT `my col` FROM t",
    // Edge whitespace INSIDE the delimiters is part of the name. The bare render trims the joined
    // name; the quoted one must not, or `` `a ` `` renders `"a"` and re-parses to a different field.
    "SELECT `a ` FROM t",
    "SELECT ` a` FROM t",
    "SELECT `a `.`b` FROM t",
    "SELECT \"a \" FROM t",
    "SELECT `a\"b` FROM t",
    "SELECT `a``b` FROM t",
    "SELECT amount AS `my alias` FROM t",
    "SELECT amount AS `select` FROM t",
    // the FROM table alias — the alias moves in this story, the table name does not
    "SELECT id FROM bi_events `e`",
    "SELECT id FROM bi_events \"e\""
  )

  "every quoted-identifier shape" should "re-parse to an equal AST" in {
    matrix.foreach(fixedPoint)
  }

  it should "re-parse to an equal AST a SECOND time (idempotent rendering)" in {
    matrix.foreach { sql =>
      val once = Parser(sql).toOption.get.sql
      val twice = Parser(once).toOption.get.sql
      withClue(s"[$sql] rendered [$once] then [$twice] which ") { twice shouldBe once }
    }
  }

  "the two spellings of the same name" should "produce EQUAL ASTs" in {
    // Style is not retained (AD-1 rule 1): they denote the same column. AD-13 is what makes this
    // true in operand position as well — before it, the double-quoted spelling was a string there.
    Parser("SELECT `category` FROM t") shouldBe Parser("SELECT \"category\" FROM t")
    Parser("SELECT a AS `c` FROM t") shouldBe Parser("SELECT a AS \"c\" FROM t")
    Parser("SELECT MAX(`amount`) AS m FROM t") shouldBe Parser("SELECT MAX(\"amount\") AS m FROM t")
    Parser("SELECT CAST(`a` AS BIGINT) AS x FROM t") shouldBe
    Parser("SELECT CAST(\"a\" AS BIGINT) AS x FROM t")
  }

  "a name that requires quoting" should "be re-emitted quoted" in {
    Parser("SELECT `select` FROM t").toOption.get.sql should include("\"select\"")
    Parser("SELECT `my col` FROM t").toOption.get.sql should include("\"my col\"")
    Parser("SELECT a AS `my alias` FROM t").toOption.get.sql should include("\"my alias\"")
  }

  "a name that does NOT require quoting" should "still be re-emitted quoted when it was written so" in {
    // The bit is what makes the fixed point exact; it is NOT a "needs quoting" predicate.
    Parser("SELECT `category` FROM t").toOption.get.sql should include("\"category\"")
    Parser("SELECT category FROM t").toOption.get.sql should not include "\"category\""
  }

  "an embedded double quote" should "survive the round trip as a DOUBLED delimiter" in {
    val rendered = Parser("SELECT `a\"b` FROM t").toOption.get.sql
    rendered should include("\"a\"\"b\"")
    Parser(rendered) match {
      case Right(s: SingleSearch) => s.select.fields.head.identifier.name shouldBe "a\"b"
      case other                  => fail(s"[$rendered] did not re-parse to a SingleSearch: $other")
    }
  }

  "a qualified quoted name" should "render PER PART, not as one quoted blob" in {
    // AD-1 rule 3. `softclient4es-arrow`'s JoinPlanner feeds `field.identifier.sql` straight into a
    // DuckDB SELECT list, where `"e.category"` is a column literally NAMED `e.category` (binder
    // error) and `"e"."category"` is the qualified reference the statement means. DuckDB's own
    // documentation accepts the double quote and only the double quote, which is also why the
    // canonical delimiter could not simply be switched to the backtick when AD-13's round-trip
    // failure was found. Both forms re-parse to the same AST here, so this assertion is the only
    // thing holding the choice in place.
    val rendered = Parser("SELECT `e`.`category` FROM bi_events e").toOption.get.sql
    rendered should include("\"e\".\"category\"")
    rendered should not include "\"e.category\""
  }
}
