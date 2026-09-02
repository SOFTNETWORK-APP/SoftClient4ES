package app.softnetwork.elastic.sql.parser

import app.softnetwork.elastic.sql.query.{FromlessSelect, SingleSearch}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** Story 20.9 / issue #251 — FROM-less SELECT (the connection handshake).
  *
  * Parse-first discipline: every assertion below is pinned against real parser output, never a
  * guessed AST shape. Reason substrings are pinned ONLY where this story authors the reason
  * (validate() messages); grammar-internal wording is never pinned (20.4 F-4).
  */
class FromlessSelectParserSpec extends AnyFlatSpec with Matchers {

  private def parseFromless(sql: String): FromlessSelect =
    Parser(sql) match {
      case Right(f: FromlessSelect) => f
      case other                    => fail(s"expected FromlessSelect for [$sql], got $other")
    }

  // ── host-idiom minimum (AC 1) ──────────────────────────────────────────────
  "FROM-less SELECT" should "parse the Tableau/Superset handshake idioms" in {
    val shapes =
      Seq(
        "SELECT 1",
        "SELECT 1;",
        "select 1",
        "SELECT 1 AS x",
        "SELECT 1 x",
        "SELECT 1 LIMIT 100"
      )
    shapes.foreach { s => parseFromless(s) }
    parseFromless("SELECT 1 AS x").columnNames shouldBe Seq("x")
    parseFromless("SELECT 1 x").columnNames shouldBe Seq("x") // bare alias == AS alias
    parseFromless("SELECT 1").columnNames shouldBe Seq("1")
  }

  it should "not capture LIMIT as an alias" in {
    val f = parseFromless("SELECT 1 LIMIT 100")
    f.columnNames shouldBe Seq("1")
    f.limit.map(_.limit) shouldBe Some(100)
  }

  it should "leave FROM-bearing SELECTs on the search path" in {
    Parser("SELECT 1 FROM dual") match {
      case Right(_: SingleSearch) => succeed
      case other                  => fail(s"regression: $other")
    }
  }

  // ── fixed point (AC 10, #218 discipline) ───────────────────────────────────
  it should "round-trip every accepted form through its own .sql render" in {
    val accepted = Seq(
      "SELECT 1",
      "SELECT 1 AS x",
      "SELECT 1 x",
      "SELECT 1 LIMIT 100",
      "SELECT -1",
      "SELECT 1.5",
      "SELECT true",
      "SELECT NULL AS n",
      "SELECT 1+1",
      "SELECT (1+2)*3 AS nine",
      "SELECT 3/2.0 AS r",
      "SELECT 1/0 AS boom", // parses — no local evaluation; fails at EXECUTION on ES
      "SELECT CURRENT_TIMESTAMP AS ts",
      "SELECT CURRENT_DATE AS d",
      "SELECT '125'::BIGINT AS c", // CAST('125' AS BIGINT) does NOT parse — OQ-5
      "SELECT COALESCE(NULL, 1) AS c",
      "SELECT UPPER('ok') AS u",
      "SELECT LENGTH('abc') AS l",
      "SELECT ABS(-5) AS a",
      "SELECT PI",
      "SELECT RANDOM AS r",
      "SELECT 3000000000 AS big",
      "SELECT 1 AS a, 'x' AS b, true AS c",
      "SELECT 1 AS ok, UPPER('x') AS u, 1+1 AS two", // the documentation example, parse-probed
      // OQ-2 rows, dev-verified 2026-09-02: newly reachable under PD-5, parse + guards hold
      "SELECT CURRENT_DATE - INTERVAL 1 DAY AS d",
      "SELECT CASE WHEN 1 = 1 THEN 'a' ELSE 'b' END AS c"
    )
    accepted.foreach { s =>
      val first = parseFromless(s)
      withClue(s"render [${first.sql}] of [$s]: ") {
        Parser(first.sql).toOption should contain(first)
      }
    }
  }

  // ── rejects (AC 3) — reason pins only where WE author the reason ───────────
  it should "reject column references, star, aggregates, placeholders with named reasons" in {
    def leftMsg(sql: String): String =
      Parser(sql).swap.getOrElse(fail(s"[$sql] unexpectedly parsed")).msg
    leftMsg("SELECT col") should include("Column reference 'col' requires a FROM clause")
    leftMsg("SELECT *") should include("SELECT * requires a FROM clause")
    leftMsg("SELECT COUNT(*)") should include("requires a FROM clause")
    leftMsg("SELECT 1, 1") should include("Duplicate column name")
    leftMsg("SELECT a + 1") should include("Column reference 'a'")
    leftMsg("SELECT ?") should include("Unbound parameter")
    leftMsg("SELECT COALESCE(?, 1)") should include("Unbound parameter") // nested — walk depth
    leftMsg("SELECT 1 LIMIT -5") should include("must be non-negative")
  }

  it should "keep grammar-level rejects rejected (message unpinned — F-4)" in {
    Seq(
      "SELECT 1 WHERE 1=1",
      "SELECT 1 ORDER BY 1",
      "SELECT 1 GROUP BY 1",
      "SELECT 1 HAVING 1=1",
      "SELECT 1 UNION ALL SELECT 2",
      "SELECT DISTINCT 1",
      "SELECT 1 AS `x`" // backtick aliases stay #252's acceptance row
    ).foreach { s => withClue(s"[$s]: ")(Parser(s).isLeft shouldBe true) }
  }
}
