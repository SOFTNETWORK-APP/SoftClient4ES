package app.softnetwork.elastic.sql.query

import app.softnetwork.elastic.sql.parser.Parser
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** BIDC-2 (issues #54 / #223): what `validate()` accepts and rejects around aggregates in HAVING
  * and WHERE. Rejections are asserted as the GRAMMAR's (a boundary catch would also yield a `Left`
  * carrying the reason, so `not startWith Parser.InternalParseFailure` is what makes each case
  * falsifiable).
  */
class HavingAggregateResolutionSpec extends AnyFlatSpec with Matchers {

  private def rejection(sql: String): String =
    Parser(sql).swap.toOption.map(_.msg).getOrElse(fail(s"expected a rejection for [$sql]"))

  "a HAVING referencing a SELECT aggregate by its alias" should "be accepted" in {
    Seq(
      "SELECT id, COUNT(x) AS cnt FROM t GROUP BY id HAVING cnt > 1",
      "SELECT id, MAX(YEAR(createdAt)) AS y FROM t GROUP BY id HAVING y > 2020",
      // the alias of an arithmetic expression over aggregates (a bucket_script) as well
      "SELECT id, MAX(x) - MIN(x) AS d FROM t GROUP BY id HAVING d > 3",
      // the same expression repeated, aliased or not, is one aggregation
      "SELECT id, COUNT(x) AS cnt FROM t GROUP BY id HAVING COUNT(x) > 1 ORDER BY COUNT(x) DESC"
    ).foreach { sql =>
      withClue(s"[$sql] ") {
        Parser(sql).isRight shouldBe true
      }
    }
  }

  "an aggregate in WHERE" should "be rejected, naming HAVING (lead-confirmed 2026-09-06)" in {
    // It used to parse, create the aggregation and silently DROP the predicate (`match_all`).
    val msg = rejection("SELECT id FROM t WHERE COUNT(x) > 5 GROUP BY id")
    msg should include("Aggregate functions are not allowed in WHERE")
    msg should include("COUNT(x)")
    msg should include("use HAVING")
    msg should not startWith Parser.InternalParseFailure
  }

  it should "be rejected in DELETE and UPDATE too, where the drop touched every document (S2-2)" in {
    // `DELETE FROM t WHERE COUNT(x) > 5` became `match_all` and wiped the index; the same UPDATE
    // updated every row. The check lives in Where.validate() so every statement kind gets it.
    Seq(
      "DELETE FROM t WHERE COUNT(x) > 5",
      "UPDATE t SET a = 1 WHERE COUNT(x) > 5",
      "DELETE FROM t WHERE id = 1 AND MAX(x) > 5"
    ).foreach { sql =>
      withClue(s"[$sql] ") {
        val msg = rejection(sql)
        msg should include("Aggregate functions are not allowed in WHERE")
        msg should not startWith Parser.InternalParseFailure
      }
    }
    // The plain forms are untouched.
    Parser("DELETE FROM t WHERE id = 1").isRight shouldBe true
    Parser("UPDATE t SET a = 1 WHERE id = 1").isRight shouldBe true
  }

  "arithmetic over aggregates written inline in HAVING" should "be rejected, naming the remedy" in {
    // No aggregation to read from: it was silently dropped. Aliased in SELECT it is supported.
    val msg = rejection("SELECT id FROM t GROUP BY id HAVING MAX(x) - MIN(x) > 3")
    msg should include("HAVING cannot combine aggregates arithmetically inline")
    msg should include("alias the expression in SELECT")
    msg should not startWith Parser.InternalParseFailure
  }

  "a SELECT alias equal to the derived name of a different HAVING aggregate" should "be rejected" in {
    // Before: the HAVING read MIN(x) under `params.max_x` -- a silent wrong answer.
    val msg = rejection("SELECT id, MIN(x) AS max_x FROM t GROUP BY id HAVING MAX(x) > 3")
    msg should include("Alias 'max_x'")
    msg should include("rename one of them")
    msg should not startWith Parser.InternalParseFailure
  }

  "an aggregate in the source WHERE of CREATE ENRICH POLICY" should "be rejected (third look)" in {
    // CreateEnrichPolicy.validate() never validated its WHERE: the aggregate was dropped and the
    // policy enriched from EVERY source document (match_all).
    val msg = rejection(
      "CREATE ENRICH POLICY p FROM users ON user_id ENRICH name, email WHERE COUNT(orders) > 5"
    )
    msg should include("Aggregate functions are not allowed in WHERE")
    msg should include("COUNT(orders)")
    msg should not startWith Parser.InternalParseFailure
    // The document-level WHERE is untouched.
    Parser(
      "CREATE ENRICH POLICY p FROM users ON user_id ENRICH name, email WHERE status = 'active'"
    ).isRight shouldBe true
  }

  "an aggregate IN (…) in HAVING" should "type-check against the list's element type (third look)" in {
    // `COUNT(x) IN (1, 2)` used to be rejected as `BIGINT` vs `ARRAY<BIGINT>` while the untyped
    // `MAX(x) IN (…)` passed -- the element is compared with the element type now.
    Seq(
      "SELECT id, COUNT(x) FROM t GROUP BY id HAVING COUNT(x) IN (1, 2)",
      "SELECT id FROM t GROUP BY id HAVING COUNT(x) NOT IN (1, 2)",
      "SELECT id, MAX(x) FROM t GROUP BY id HAVING MAX(x) IN (40, 50)",
      "SELECT id, MAX(name) FROM t GROUP BY id HAVING MAX(name) IN ('a', 'b')",
      // a typed document-level element gets the same comparison
      "SELECT id FROM t WHERE YEAR(createdAt) IN (2020, 2021)"
    ).foreach { sql =>
      withClue(s"[$sql] ") {
        Parser(sql).isRight shouldBe true
      }
    }
    // A genuine mismatch stays loud, and names the ELEMENT types.
    val msg = rejection("SELECT id FROM t GROUP BY id HAVING COUNT(x) IN ('a', 'b')")
    msg should include("Type mismatch")
    msg should include("'BIGINT' is not compatible with 'VARCHAR'")
    msg should not startWith Parser.InternalParseFailure
  }
}
