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

  "an aggregate in WHERE" should "be rejected, naming HAVING (lead to confirm the product choice)" in {
    // It used to parse, create the aggregation and silently DROP the predicate (`match_all`).
    val msg = rejection("SELECT id FROM t WHERE COUNT(x) > 5 GROUP BY id")
    msg should include("Aggregate functions are not allowed in WHERE")
    msg should include("COUNT(x)")
    msg should include("use HAVING")
    msg should not startWith Parser.InternalParseFailure
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
}
