package app.softnetwork.elastic.sql.query

import app.softnetwork.elastic.sql.Identifier
import app.softnetwork.elastic.sql.parser.Parser
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** Story 20.9 / issue #251 — FromlessSelect guard arms + the toSingleSearch rewrite pins.
  *
  * Replaces the struck ConstantFoldingSpec (Wave A″): semantics now live in ES, so this spec pins
  * only what the sql module owns — the validate() guard matrix (parse-first: every row parsed with
  * the real Parser, never a hand-built AST unless the point IS the programmatic path) and the shape
  * of the internal SingleSearch rewrite.
  */
class FromlessSelectValidateSpec extends AnyFlatSpec with Matchers {

  private val idx = "softclient4es_handshake"

  private def parseFromless(sql: String): FromlessSelect =
    Parser(sql) match {
      case Right(f: FromlessSelect) => f
      case other                    => fail(s"expected FromlessSelect for [$sql], got $other")
    }

  private def leftMsg(sql: String): String =
    Parser(sql).swap.getOrElse(fail(s"[$sql] unexpectedly parsed")).msg

  // ── the rewrite (AD-3′) ────────────────────────────────────────────────────
  "FromlessSelect.toSingleSearch" should "build the parser-equivalent SingleSearch with LIMIT 1" in {
    val single = parseFromless("SELECT 1").toSingleSearch(idx)
    single.sql shouldBe s"SELECT 1 FROM $idx LIMIT 1"
    single.limit shouldBe Some(Limit(1, None))
    // every constant select item IS a script field — the FROM-ful script_fields pipeline
    single.scriptFields.size shouldBe 1
    single.returnsRows shouldBe true
    // the ES response keys are the computed aliases; assembly maps them back to PD-2 names
    single.select.fieldsWithComputedAliases.head.fieldAlias.map(_.alias) shouldBe Some("__c1")
  }

  it should "round-trip its own render through the real parser (generated-SQL fixed point)" in {
    Seq(
      "SELECT 1",
      "SELECT 1 AS x",
      "SELECT UPPER('ok') AS u",
      "SELECT CURRENT_TIMESTAMP AS ts, CURRENT_TIMESTAMP AS ts2"
    ).foreach { s =>
      val rewritten = parseFromless(s).toSingleSearch(idx)
      withClue(s"rewrite [${rewritten.sql}] of [$s]: ") {
        Parser(rewritten.sql).toOption should contain(rewritten)
      }
    }
  }

  it should "never leak the statement's LIMIT into the rewrite" in {
    // AD-12: a host's LIMIT must never reach the rewrite — LIMIT 11000 would flip
    // requiresScrollPaging and route a handshake into scroll/PIT.
    parseFromless("SELECT 1 LIMIT 100").toSingleSearch(idx).limit shouldBe Some(Limit(1, None))
    parseFromless("SELECT 1 LIMIT 11000 OFFSET 5")
      .toSingleSearch(idx)
      .limit shouldBe Some(Limit(1, None))
  }

  // ── guard arms (parse-first, PD-5 reject matrix) ───────────────────────────
  "FromlessSelect.validate" should "reject column references by own name and via dependencies" in {
    leftMsg("SELECT col") should include("Column reference 'col' requires a FROM clause")
    leftMsg("SELECT UPPER(col)") should include("Column reference")
    leftMsg("SELECT COALESCE(col, 1)") should include("Column reference")
    leftMsg("SELECT a + 1") should include("Column reference 'a'")
    // EValue is grammar-unreachable (value = literal|pi|random|...) — E is a column reference
    leftMsg("SELECT E") should include("Column reference 'E'")
  }

  it should "reject star, aggregates and windows — window checked BEFORE aggregation" in {
    leftMsg("SELECT *") should include("SELECT * requires a FROM clause")
    leftMsg("SELECT COUNT(*)") should include("requires a FROM clause")
    // WindowFunction extends AggregateFunction: aggregation-first would make this arm dead code
    val windowMsg = leftMsg("SELECT ROW_NUMBER() OVER (ORDER BY salary DESC) AS rn")
    windowMsg should include("Window function")
    (windowMsg should not).include("Aggregation")
  }

  it should "reject placeholders anywhere in the tree, array literals, EXCEPT and LIMIT bounds" in {
    leftMsg("SELECT ?") should include("Unbound parameter")
    leftMsg("SELECT COALESCE(?, 1)") should include("Unbound parameter") // nested — tree walk
    leftMsg("SELECT 1, 1") should include("Duplicate column name")
    leftMsg("SELECT 1 LIMIT -5") should include("must be non-negative")
    // LimitParser does .toInt — 4294967291 wraps to -5; without the guard the result would
    // silently empty through take(negative) (#253 family). NOTE the guard's honest bound:
    // an overflow that wraps POSITIVE (e.g. 9999999999 -> 1410065407) is indistinguishable
    // from that literal by the time Limit holds an Int — pre-existing LimitParser behaviour,
    // every statement kind, out of 20.9 scope.
    leftMsg("SELECT 1 LIMIT 4294967291") should include("must be non-negative")
    leftMsg("SELECT 1 EXCEPT(a)") should include("EXCEPT(...) requires a FROM clause")
    // array literal — multi-element painless lists defeat the 1-element unwrap contract (AD-11);
    // parse-first: whichever seam rejects it (grammar or validate), it must be a Left
    Parser("SELECT ['a', 'b']").isLeft shouldBe true
  }

  it should "guard the programmatic path too — built WITHOUT the parser" in {
    // run(statement) never calls validate(): the guard set must be self-contained.
    FromlessSelect(Select(Seq(Field(Identifier("*"))))).validate().isLeft shouldBe true
    FromlessSelect(Select(Seq(Field(Identifier("col"))))).validate() shouldBe
    Left("Column reference 'col' requires a FROM clause")
  }
}
