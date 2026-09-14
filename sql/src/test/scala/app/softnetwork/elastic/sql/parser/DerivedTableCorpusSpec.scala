package app.softnetwork.elastic.sql.parser

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** Story 22.1 — the ELEVEN derived-table statements of the Epic 19 BI corpus, each with its
  * EXPECTED verdict and, for a rejection, its declared owner.
  *
  * Table-driven so the attribution is machine-checked rather than argued: story 21.6 AD-3's rule is
  * that a verdict is never attributed by reading the error message, because `Parser.apply` reports
  * ONE combinator failure and a statement with several blockers reports whichever alternative got
  * furthest. Nine parse after this story; two are declared residuals with a named owner, and one of
  * those (`w8.054`) carries a blocker epic 22 does not own at all.
  *
  * Statements are pasted VERBATIM from `epic-19-bi-corpus.csv` column `captured_statement`
  * (whitespace collapsed; a trailing `;` is what `Parser.apply` strips anyway).
  */
class DerivedTableCorpusSpec extends AnyFlatSpec with Matchers {

  sealed trait Expected
  case object Parses extends Expected
  final case class Residual(owner: String, reasonIncludes: String) extends Expected

  behavior of "The epic-19 derived-table corpus rows"

  private val rows: Seq[(String, String, Expected)] = Seq(
    ("tableau.mysql.wx.003", "SELECT `COL` FROM (SELECT 1 AS `COL`) AS `SUBQUERY`", Parses),
    ("tableau.mysql.w1.018", "SELECT `COL` FROM (SELECT 1 AS `COL`) AS `SUBQUERY`", Parses),
    ("tableau.mysql.w7.043", "SELECT `COL` FROM (SELECT 1 AS `COL`) AS `SUBQUERY`", Parses),
    (
      "tableau.sql92.wx.003",
      """SELECT "COL" FROM (SELECT 1 AS "COL") AS "SUBQUERY"""",
      Parses
    ),
    (
      "tableau.mysql.w1.019",
      "SELECT MAX(1) AS `TblMax` FROM ( SELECT * FROM `elastic`.`bi_events` `bi_events` ) " +
      "`bi_events`",
      Parses
    ),
    (
      "tableau.mysql.w7.044",
      "SELECT MAX(1) AS `TblMax` FROM ( SELECT * FROM `bi_category_dim` ) `bi_category_dim`",
      Parses
    ),
    (
      "tableau.sql92.wx.009",
      """SELECT MAX(1) AS "TblMax" FROM ( SELECT * FROM "elastic"."bi_events" "bi_events" ) """ +
      """"bi_events"""",
      Parses
    ),
    (
      "superset.flightsql.w5.005",
      """SELECT country AS country, sum(amount) AS "SUM(amount)" FROM bi_events JOIN """ +
      """(SELECT country AS country__, sum(amount) AS mme_inner__ FROM bi_events GROUP BY """ +
      """country ORDER BY sum(amount) DESC LIMIT 10) AS series_limit ON country = country__ """ +
      """GROUP BY country ORDER BY "SUM(amount)" DESC LIMIT 10000""",
      Parses
    ),
    (
      "superset.flightsql.w7.007",
      """SELECT category_label AS category_label, sum(amount) AS "SUM(amount)" FROM """ +
      """(SELECT e.category, e.amount, e.qty, e.country, d.category_label, d.category_group """ +
      """FROM bi_events AS e JOIN bi_category_dim AS d ON e.category = d.category) AS """ +
      """virtual_table GROUP BY category_label ORDER BY "SUM(amount)" DESC LIMIT 10000""",
      Parses
    ),
    (
      "tableau.sql92.wx.012",
      """SELECT * FROM (SELECT * FROM "elastic"."bi_events") WHERE ROWNUM <= 1""",
      // The alias is mandatory (PD-1) AND the statement carries Oracle `ROWNUM`, which epic 22
      // does not own. The parser reports the FIRST blocker; the attribution is this table's.
      Residual(
        "PD-1 (no correlation name) + Oracle ROWNUM (epic 22 out of scope)",
        "requires an alias"
      )
    ),
    (
      "tableau.mysql.w8.054",
      "SELECT `t0`.`category_label` AS `category_label`, SUM(`bi_events`.`amount`) AS " +
      "`sum_amount_ok` FROM `elastic`.`bi_events` `bi_events` INNER JOIN ( SELECT " +
      "`bi_events`.`category` AS `category`, `bi_category_dim`.`category_label` AS " +
      "`category_label` FROM `elastic`.`bi_events` `bi_events` LEFT JOIN `bi_category_dim` ON " +
      "(`bi_events`.`category` = `bi_category_dim`.`category`) GROUP BY `bi_events`.`category`, " +
      "`bi_category_dim`.`category_label` ) `t0` ON (`bi_events`.`category` <=> " +
      "`t0`.`category`) GROUP BY `t0`.`category_label`",
      // The DERIVED TABLE itself parses; the MySQL null-safe `<=>` in the ON does not. The reason
      // is grammar-internal, so it is deliberately NOT pinned.
      Residual("MySQL <=> in the JOIN ON (epic 22 out of scope)", "")
    )
  )

  rows.foreach { case (id, sql, expected) =>
    // The label is computed OUTSIDE the interpolation: a string literal inside an `s"${…}"` block
    // is a Scala 2.12 lexer risk not worth taking in a cross-compiled test source.
    val label = expected match {
      case Parses         => "PARSE"
      case Residual(o, _) => s"stay rejected - $o"
    }
    it should s"$id: $label" in {
      noException should be thrownBy Parser(sql)
      expected match {
        case Parses =>
          val stmt = Parser(sql).toOption.getOrElse(
            fail(s"[$id] rejected: ${Parser(sql).swap.toOption.map(_.msg).getOrElse("")}")
          )
          withClue(s"[$id] render is not a fixed point: ${stmt.sql} ") {
            Parser(stmt.sql) shouldBe Right(stmt)
          }
        case Residual(_, reason) =>
          val msg = Parser(sql).swap.toOption.map(_.msg).getOrElse("")
          withClue(s"[$id] ") { Parser(sql).isLeft shouldBe true }
          // Without this a restored `throw` would still yield a Left carrying the same reason.
          withClue(s"[$id] msg=[$msg] ") { msg should not startWith Parser.InternalParseFailure }
          if (reason.nonEmpty) withClue(s"[$id] msg=[$msg] ") { msg should include(reason) }
      }
    }
  }

  /** 🔴 The control that makes `w8.054`'s attribution machine-checked rather than prose.
    *
    * Its `Residual` row carries an EMPTY `reasonIncludes` (the rejection is grammar-internal and
    * this project never pins such a message), so on its own it cannot tell "rejected by `<=>`" from
    * "rejected because derived tables regressed" — the 21.6 AD-3 failure mode. The SAME statement
    * with `=` in place of `<=>` must PARSE; then `<=>` is the only variable.
    */
  it should "w8.054 with = instead of <=>: PARSE — so <=> is the only blocker left" in {
    val withEq = rows
      .find(_._1 == "tableau.mysql.w8.054")
      .map(_._2.replace("<=>", "="))
      .getOrElse(fail("the w8.054 row is gone"))
    Parser(withEq).isRight shouldBe true
    // 🔴 Deliberately NOT a render fixed point, and the reason is a PRE-EXISTING defect this story
    // neither causes nor fixes: a QUOTED table name in JOIN position is rejected by the grammar
    // (`FROM "b"` parses, `JOIN "u"` does not). MEASURED on unmodified `origin/main` b7f20f40:
    // `SELECT k FROM b INNER JOIN "u" AS "t0" ON b.k = "t0"."k"` is `Left("end of input
    // expected")` there too. Since story 21.1 canonicalises backticks to double quotes, the render
    // of any statement whose JOIN leg carries a quoted name cannot be re-parsed. It does not touch
    // the nine rows above — every JOIN they carry is a DERIVED table, which renders parenthesised
    // and re-parses (asserted for each). Reported to the lead, not fixed here.
    ()
  }

  it should "report 9 parsing and 2 declared residuals — the number story 22.7 publishes" in {
    rows.count(_._3 == Parses) shouldBe 9
    rows.size shouldBe 11
  }
}
