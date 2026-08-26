package app.softnetwork.elastic.sql.parser

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** Guards the window-function surface `documentation/sql/dql_statements.md` promises.
  *
  * That list silently fell four entries behind the parser (`AVG`, `MIN`, `MAX`,
  * `PERCENTILE_CONT`/`PERCENTILE_DISC`). Reading the code did not catch it and no test could,
  * because nothing asserted the documented surface.
  *
  * Each form is pinned to its **exact rendering**, not merely to "parses".
  * `Parser(rendered).isRight` alone is worth little here: a rendering that silently drops a clause
  * still parses, so the weaker assertion is green on precisely the corruption it looks like it
  * guards. Pinning the string makes a dropped clause a failing diff — and doubles as executable
  * documentation of what the engine gives back, which is not always what the caller wrote.
  *
  * Adding a window function means adding its documented spellings here and to `dql_statements.md`.
  */
class WindowFunctionCoverageSpec extends AnyFlatSpec with Matchers {

  behavior of "the documented window-function surface"

  private val select = "SELECT "
  private val from = " FROM t"

  /** (label, written, rendered-back). `rendered` differing from `written` is a documented fact. */
  private val documented: Seq[(String, String, String)] = {
    def same(l: String, sql: String) = (l, sql, sql)
    Seq(
      same("SUM", "SUM(amount) OVER (PARTITION BY product) AS s"),
      same("AVG", "AVG(price) OVER (PARTITION BY category) AS a"),
      same("MIN", "MIN(price) OVER (PARTITION BY brand) AS mn"),
      same("MAX", "MAX(price) OVER (PARTITION BY brand) AS mx"),
      same("COUNT star", "COUNT(*) OVER (PARTITION BY customer_id) AS c"),
      same("COUNT DISTINCT", "COUNT(DISTINCT city) OVER (PARTITION BY country) AS c"),
      same("FIRST_VALUE", "FIRST_VALUE(amount) OVER (PARTITION BY product ORDER BY ts ASC) AS f"),
      same("LAST_VALUE", "LAST_VALUE(amount) OVER (PARTITION BY product ORDER BY ts ASC) AS l"),
      same("ARRAY_AGG", "ARRAY_AGG(tag) OVER (PARTITION BY product ORDER BY ts ASC) AS t"),
      same("STDDEV", "STDDEV(salary) OVER (PARTITION BY department) AS sd"),
      same("STDDEV_SAMP", "STDDEV_SAMP(salary) OVER (PARTITION BY department) AS sd"),
      same("STDDEV_POP", "STDDEV_POP(salary) OVER (PARTITION BY department) AS sd"),
      same("VARIANCE", "VARIANCE(salary) OVER (PARTITION BY department) AS v"),
      same("VAR_SAMP", "VAR_SAMP(salary) OVER (PARTITION BY department) AS v"),
      same("VAR_POP", "VAR_POP(salary) OVER (PARTITION BY department) AS v"),
      same("ROW_NUMBER", "ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) AS rn"),
      same("RANK", "RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS rk"),
      same("DENSE_RANK", "DENSE_RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS dr"),
      same("ranking without PARTITION BY", "ROW_NUMBER() OVER (ORDER BY salary DESC) AS rn"),
      // ranking windows DO carry their inline LIMIT through the rendering (RankingWindow overrides
      // emitsLimitInOver); the ARRAY_AGG case below shows the other half of that decision.
      same(
        "RANK inline LIMIT",
        "RANK() OVER (PARTITION BY department ORDER BY salary DESC LIMIT 3) AS rk"
      ),
      // all five percentile spellings converge on one canonical rendering. This is the PR's headline
      // finding, and it is only a finding because the expected string is pinned.
      (
        "PERCENTILE_CONT OVER",
        "PERCENTILE_CONT(0.95) OVER (PARTITION BY service ORDER BY ms) AS p",
        "PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY ms) OVER (PARTITION BY service) AS p"
      ),
      (
        "PERCENTILE_DISC OVER",
        "PERCENTILE_DISC(0.95) OVER (PARTITION BY service ORDER BY ms) AS p",
        "PERCENTILE_DISC(0.95) WITHIN GROUP (ORDER BY ms) OVER (PARTITION BY service) AS p"
      ),
      same("PERCENTILE WITHIN GROUP", "PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY ms) AS p"),
      same(
        "PERCENTILE WITHIN + OVER",
        "PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY ms) OVER (PARTITION BY service) AS p"
      ),
      (
        "PERCENTILE shorthand",
        "PERCENTILE_CONT(ms, 0.95) AS p",
        "PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY ms) AS p"
      ),
      (
        "PERCENTILE shorthand + OVER",
        "PERCENTILE_CONT(ms, 0.95) OVER (PARTITION BY service) AS p",
        "PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY ms) OVER (PARTITION BY service) AS p"
      ),
      // the [0,1] bound is inclusive; the rejections below only probe it from outside
      (
        "PERCENTILE p = 0",
        "PERCENTILE_CONT(0) WITHIN GROUP (ORDER BY ms) AS p",
        "PERCENTILE_CONT(0.0) WITHIN GROUP (ORDER BY ms) AS p"
      ),
      (
        "PERCENTILE p = 1",
        "PERCENTILE_CONT(1) WITHIN GROUP (ORDER BY ms) AS p",
        "PERCENTILE_CONT(1.0) WITHIN GROUP (ORDER BY ms) AS p"
      )
    )
  }

  documented.foreach { case (label, written, expected) =>
    it should s"render the documented window form as documented: $label" in {
      val statement = Parser(select + written + from) match {
        case Right(st) => st
        case Left(err) => fail(s"documented form failed to parse: $written -> $err")
      }
      statement.sql shouldBe (select + expected + from)
      withClue(s"the rendering does not parse back: ${statement.sql}\n") {
        Parser(statement.sql).isRight shouldBe true
      }
    }
  }

  /** ⚠️ Known loss, pinned deliberately so the day it is fixed this test says so.
    *
    * `ARRAY_AGG`'s inline `LIMIT` is parsed and kept on the AST, but `emitsLimitInOver` is false
    * for everything except ranking windows, so the rendering drops it. Any consumer that
    * round-trips SQL through the AST — the REPL, JOIN reconstruction — loses the bound. Worse,
    * `ArrayAgg.update` then falls back to `request.limit`, so the statement's own LIMIT is
    * substituted for the inline one — a different answer, not an error. See issue #247. Pinned to
    * today's behaviour on purpose: fixing #247 makes this test fail, which is the point.
    */
  it should "drop ARRAY_AGG's inline LIMIT when rendering (known asymmetry)" in {
    val written = "ARRAY_AGG(tag) OVER (PARTITION BY product ORDER BY ts ASC LIMIT 10) AS t"
    val Right(statement) = Parser(select + written + from)
    statement.sql shouldBe
    (select + "ARRAY_AGG(tag) OVER (PARTITION BY product ORDER BY ts ASC) AS t" + from)
  }

  /** Rejections the documentation states explicitly. Each asserts WHY it was rejected: a negative
    * that only checks `isLeft` passes just as happily when the construct stops being recognised at
    * all, which is the opposite of what it claims to prove.
    */
  it should "reject a ranking window with no ORDER BY, and accept the same statement with one" in {
    val withoutOrder = s"${select}ROW_NUMBER() OVER (PARTITION BY department) AS rn$from"
    val withOrder =
      s"${select}ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) AS rn$from"
    // paired on purpose: the rejection message here is a generic one, so on its own it cannot tell
    // "the ANSI rule fired" from "ROW_NUMBER is no longer a window function". The pair can.
    Parser(withoutOrder).isRight shouldBe false
    Parser(withOrder).isRight shouldBe true
  }

  Seq("above 1" -> "1.5", "below 0" -> "-0.5").foreach { case (label, p) =>
    it should s"reject a percentile fraction $label, naming the bound" in {
      Parser(s"${select}PERCENTILE_CONT($p) WITHIN GROUP (ORDER BY ms) AS p$from") match {
        case Right(st) => fail(s"expected rejection, parsed as: ${st.sql}")
        case Left(err) => err.toString should include("[0,1]")
      }
    }
  }
}
