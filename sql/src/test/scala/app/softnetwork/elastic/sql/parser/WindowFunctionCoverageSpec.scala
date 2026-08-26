package app.softnetwork.elastic.sql.parser

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** Guards the window-function surface the documentation promises.
  *
  * `documentation/sql/dql_statements.md` enumerates which functions work with `OVER`, and that list
  * silently fell four entries behind the parser (`AVG`, `MIN`, `MAX`, `PERCENTILE_CONT/DISC`) —
  * reading the code did not catch it, and neither did any test, because nothing asserted the
  * documented surface. Every form below is one the docs tell a user to write.
  *
  * Two properties per form, because parsing alone is not enough: the statement must parse, and its
  * rendered `.sql` must parse again. A round-trip that silently drops a clause is how a documented
  * form turns into a wrong query downstream.
  *
  * When a window function is added, add its documented spellings here and to `dql_statements.md`.
  */
class WindowFunctionCoverageSpec extends AnyFlatSpec with Matchers {

  /** label -> statement, one per documented spelling. */
  private val documented: Seq[(String, String)] = Seq(
    "SUM"        -> "SELECT product, SUM(amount) OVER (PARTITION BY product) AS s FROM sales",
    "AVG"        -> "SELECT category, AVG(price) OVER (PARTITION BY category) AS a FROM products",
    "MIN"        -> "SELECT brand, MIN(price) OVER (PARTITION BY brand) AS mn FROM products",
    "MAX"        -> "SELECT brand, MAX(price) OVER (PARTITION BY brand) AS mx FROM products",
    "COUNT star" -> "SELECT customer_id, COUNT(*) OVER (PARTITION BY customer_id) AS c FROM orders",
    "COUNT DISTINCT" -> "SELECT country, COUNT(DISTINCT city) OVER (PARTITION BY country) AS c FROM places",
    "FIRST_VALUE" -> "SELECT product, FIRST_VALUE(amount) OVER (PARTITION BY product ORDER BY ts ASC) AS f FROM sales",
    "LAST_VALUE"  -> "SELECT product, LAST_VALUE(amount) OVER (PARTITION BY product ORDER BY ts ASC) AS l FROM sales",
    "ARRAY_AGG"   -> "SELECT product, ARRAY_AGG(tag) OVER (PARTITION BY product ORDER BY ts ASC) AS t FROM sales",
    "ARRAY_AGG inline LIMIT" -> "SELECT product, ARRAY_AGG(tag) OVER (PARTITION BY product ORDER BY ts ASC LIMIT 10) AS t FROM sales",
    "STDDEV"      -> "SELECT department, STDDEV(salary) OVER (PARTITION BY department) AS sd FROM employees",
    "STDDEV_SAMP" -> "SELECT department, STDDEV_SAMP(salary) OVER (PARTITION BY department) AS sd FROM employees",
    "STDDEV_POP"  -> "SELECT department, STDDEV_POP(salary) OVER (PARTITION BY department) AS sd FROM employees",
    "VARIANCE"    -> "SELECT department, VARIANCE(salary) OVER (PARTITION BY department) AS v FROM employees",
    "VAR_SAMP"    -> "SELECT department, VAR_SAMP(salary) OVER (PARTITION BY department) AS v FROM employees",
    "VAR_POP"     -> "SELECT department, VAR_POP(salary) OVER (PARTITION BY department) AS v FROM employees",
    // all four percentile spellings are documented; they normalize to one canonical rendering
    "PERCENTILE_CONT OVER"   -> "SELECT service, PERCENTILE_CONT(0.95) OVER (PARTITION BY service ORDER BY ms) AS p95 FROM logs",
    "PERCENTILE_DISC OVER"   -> "SELECT service, PERCENTILE_DISC(0.95) OVER (PARTITION BY service ORDER BY ms) AS p95 FROM logs",
    "PERCENTILE WITHIN GROUP" -> "SELECT PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY ms) AS p95 FROM logs",
    "PERCENTILE WITHIN + OVER" -> "SELECT service, PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY ms) OVER (PARTITION BY service) AS p95 FROM logs",
    "PERCENTILE shorthand"   -> "SELECT PERCENTILE_CONT(ms, 0.95) AS p95 FROM logs",
    "ROW_NUMBER"  -> "SELECT name, ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) AS rn FROM employees",
    "RANK"        -> "SELECT name, RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS rk FROM employees",
    "DENSE_RANK"  -> "SELECT name, DENSE_RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS dr FROM employees",
    "RANK inline LIMIT" -> "SELECT name, RANK() OVER (PARTITION BY department ORDER BY salary DESC LIMIT 3) AS rk FROM employees",
    "ranking without PARTITION BY" -> "SELECT name, ROW_NUMBER() OVER (ORDER BY salary DESC) AS rn FROM employees"
  )

  documented.foreach { case (label, sql) =>
    it should s"parse the documented window form: $label" in {
      Parser(sql) match {
        case Left(err) => fail(s"documented form failed to parse: $sql -> $err")
        case Right(statement) =>
          val rendered = statement.sql
          withClue(s"rendered SQL does not re-parse: $rendered\n") {
            Parser(rendered).isRight shouldBe true
          }
      }
    }
  }

  /** Rejections the documentation states explicitly; they must stay rejections. */
  private val rejected: Seq[(String, String)] = Seq(
    // ANSI: a ranking with no order is not a ranking. Rejected at parse time on purpose, rather
    // than parsing and breaking at execution.
    "ranking window without ORDER BY" ->
      "SELECT name, ROW_NUMBER() OVER (PARTITION BY department) AS rn FROM employees",
    "percentile fraction above 1" ->
      "SELECT PERCENTILE_CONT(1.5) WITHIN GROUP (ORDER BY ms) AS p FROM logs",
    "percentile fraction below 0" ->
      "SELECT PERCENTILE_CONT(-0.5) WITHIN GROUP (ORDER BY ms) AS p FROM logs"
  )

  rejected.foreach { case (label, sql) =>
    it should s"reject: $label" in {
      Parser(sql).isRight shouldBe false
    }
  }
}
