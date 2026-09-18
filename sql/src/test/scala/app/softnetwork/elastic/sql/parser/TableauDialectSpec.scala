/*
 * Copyright 2025 SOFTNETWORK
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package app.softnetwork.elastic.sql.parser

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** The dialect spellings a BI tool emits that the grammar did not accept.
  *
  * Sourced from the Epic 19 capture (`sql/src/test/resources/corpus/epic-21-attribution.csv`),
  * where each of these was a `residual` row with a `local:` owner and a bisected cause.
  *
  * Every accepted spelling is asserted to be an ALIAS, not a second mechanism: the render
  * normalises to the canonical spelling and that render RE-PARSES. Asserting `isRight` alone would
  * be satisfied by a grammar that accepted the word and then produced something it could not read
  * back — the failure mode story 21.7 found in `CreateMaterializedView.sql`.
  *
  * The rejection assertions additionally rule out `Parser.apply`'s `NonFatal` boundary catch: a
  * restored `throw` yields a `Left` CONTAINING the same reason, so `isLeft` plus a substring match
  * is satisfied by the defect as well as by the fix (story 21.4's lesson).
  */
class TableauDialectSpec extends AnyFlatSpec with Matchers {

  private def canonicalises(sql: String, to: String): Unit =
    withClue(s"[$sql] ") {
      val parsed = Parser(sql)
      parsed.isRight shouldBe true
      val rendered = parsed.map(_.sql).getOrElse("")
      rendered should include(to)
      withClue(s"render [$rendered] must re-parse - ") {
        Parser(rendered).map(_.sql) shouldBe Right(rendered)
      }
    }

  private def refuses(sql: String, because: String): Unit =
    withClue(s"[$sql] ") {
      noException should be thrownBy Parser(sql)
      val reason = Parser(sql).left.map(_.toString).left.getOrElse("")
      reason should include(because)
      reason should not include "Internal parser error"
    }

  // ---------------------------------------------------------------- CHAR_LENGTH

  "CHAR_LENGTH" should "be accepted as a spelling of LENGTH, and render as LENGTH" in {
    canonicalises("SELECT CHAR_LENGTH(name) AS n FROM t", "LENGTH(name)")
    canonicalises("SELECT CHARACTER_LENGTH(name) AS n FROM t", "LENGTH(name)")
  }

  it should "still let LENGTH and LEN through, LENGTH first so LEN cannot eat its prefix" in {
    canonicalises("SELECT LENGTH(name) AS n FROM t", "LENGTH(name)")
    canonicalises("SELECT LEN(name) AS n FROM t", "LENGTH(name)")
  }

  it should "work where the capture actually used it - inside an aggregate" in {
    // tableau.sql92.w8.032: SUM(CHAR_LENGTH("bi_events"."name"))
    canonicalises(
      """SELECT SUM(CHAR_LENGTH("name")) AS "sum_ok" FROM "bi_events"""",
      "LENGTH("
    )
  }

  // ------------------------------------------------------------- ODBC functions

  "TIMESTAMPADD" should "be a spelling of DATETIME_ADD, in the ODBC (unit, count, base) order" in {
    canonicalises("SELECT TIMESTAMPADD(DAY, -89, event_ts) AS d FROM t", "DATETIME_ADD(DAY, -89,")
    canonicalises("SELECT TIMESTAMPADD(MONTH, 2, event_ts) AS d FROM t", "DATETIME_ADD(MONTH, 2,")

  }

  it should "accept the ODBC SQL_TSI_ interval names for every unit we already have" in {
    Seq("YEAR", "MONTH", "QUARTER", "WEEK", "DAY", "HOUR", "MINUTE", "SECOND").foreach { unit =>
      canonicalises(
        s"SELECT TIMESTAMPADD(SQL_TSI_$unit, 1, event_ts) AS d FROM t",
        s"DATETIME_ADD($unit, 1,"
      )
    }
  }

  it should "accept CURRENT_DATE as the base, which is the shape Tableau emits" in {
    // tableau.sql92.w4.025, minus the {fn ...} escape it is wrapped in.
    canonicalises(
      "SELECT a FROM t WHERE ts >= TIMESTAMPADD(SQL_TSI_DAY, -89, CURRENT_DATE)",
      "DATETIME_ADD(DAY, -89, CURRENT_DATE)"
    )
  }

  /** MySQL 8.4 defines `TIMESTAMPDIFF(unit, dt1, dt2)` as `dt2 - dt1`, and `date_diff_transact_sql`
    * binds `(unit, d1, d2)` to `DateDiff(start = d1, end = d2)`, i.e. `between(d1, d2)` — the same
    * answer with the same sign. The MySQL/ODBC order is the one that matters, so it is the one
    * asserted.
    */
  "TIMESTAMPDIFF" should "be a spelling of DATE_DIFF, in the MySQL/ODBC unit-first order" in {
    // 🔴 The ARGUMENT LIST, not just the function name: `include("DATE_DIFF(")` is satisfied by
    // the sign-inverted render `DATE_DIFF(MONTH, end_date, start_date)` too, so it would prove
    // nothing about the very binding this test exists to pin.
    canonicalises(
      "SELECT TIMESTAMPDIFF(MONTH, start_date, end_date) AS d FROM t",
      "DATE_DIFF(MONTH, start_date, end_date)"
    )
    canonicalises("SELECT TIMESTAMPDIFF(DAY, a, b) AS d FROM t", "DATE_DIFF(DAY, a, b)")
  }

  it should "leave the bare unit names alone" in {
    // The alias must not have widened what INTERVAL accepts, nor narrowed it.
    canonicalises("SELECT event_ts + INTERVAL 1 DAY AS d FROM t", "INTERVAL 1 DAY")
    canonicalises("SELECT event_ts + INTERVAL 2 MONTHS AS d FROM t", "INTERVAL 2 MONTH")
  }

  // --------------------------------------------------------------------- TOP n

  "SELECT TOP n" should "bind the rows, and render as the LIMIT it is" in {
    // tableau.sql92.wx.010
    canonicalises("SELECT TOP 1 * FROM t", "LIMIT 1")
    canonicalises("""SELECT TOP 1 * FROM "elastic"."bi_events"""", "LIMIT 1")
    canonicalises("SELECT TOP 10 a, b FROM t WHERE a = 1 ORDER BY b DESC", "LIMIT 10")
  }

  it should "produce exactly the same statement as the LIMIT spelling" in {
    Parser("SELECT TOP 5 a FROM t").map(_.sql) shouldBe Parser("SELECT a FROM t LIMIT 5").map(_.sql)
  }

  it should "leave a column named `top` alone - TOP is not reserved" in {
    canonicalises("SELECT top FROM t", "SELECT top FROM t")
    canonicalises("SELECT top AS x FROM t", "SELECT top AS x FROM t")
    canonicalises("SELECT top, a FROM t", "SELECT top, a FROM t")
    canonicalises("SELECT a FROM t WHERE top = 1", "top = 1")
  }

  it should "refuse TOP together with LIMIT rather than silently preferring one" in {
    refuses("SELECT TOP 5 a FROM t LIMIT 10", "TOP and LIMIT")
  }

  /** 🔴 This is the regression an independent review caught, and the reason `top` uses `failure`
    * and not `err`. `SELECT top -1 AS x FROM t` reads `top - 1` and parses on `main`. A named
    * "non-negative row count" refusal made it a HARD error, because `Parsers.|` never tries another
    * alternative after an `Error`, so `opt` could not backtrack to the column reading.
    *
    * The control is the same statement with a different identifier: whatever `foo` does, `top` must
    * do. Asserting only that `top` parses would pass against a grammar that read it as something
    * else entirely.
    */
  it should "read `top` as a column when what follows is not a row count" in {
    Seq("SELECT top -1 AS x FROM t", "SELECT top-1 AS x FROM t", "SELECT top -1 FROM t").foreach {
      sql =>
        withClue(s"[$sql] ") {
          val control = Parser(sql.replace("top", "foo")).map(_.sql.replace("foo", "top"))
          Parser(sql).map(_.sql) shouldBe control
        }
    }
  }

  it should "not silently wrap a row count that does not fit an Int" in {
    // `Limit` holds an Int. Checking the sign on the Long and then calling `.toInt` would turn
    // TOP 2147483648 into LIMIT -2147483648 -- a guard defeated one line after it is written.
    Seq("SELECT TOP 2147483648 a FROM t", "SELECT TOP 99999999999999 a FROM t").foreach { sql =>
      withClue(s"[$sql] -> ${Parser(sql).map(_.sql)} ") {
        Parser(sql).map(_.sql).getOrElse("") should not include "LIMIT -"
        Parser(sql).map(_.sql).getOrElse("") should not include "LIMIT 276447231"
      }
    }
  }

  /** `TOP n PERCENT` is real T-SQL that this engine does not implement. Before it was refused by
    * name, `top` consumed `TOP 5` and `field` then read `PERCENT a` as the column `PERCENT` aliased
    * to `a`: `SELECT TOP 5 PERCENT a FROM t` answered with rows for a column nobody named. A loud
    * rejection had become a silent wrong answer.
    */
  it should "refuse TOP n PERCENT by name rather than inventing a column called PERCENT" in {
    refuses("SELECT TOP 5 PERCENT a FROM t", "PERCENT")
    refuses("SELECT TOP 5 PERCENT a, b FROM t", "PERCENT")
    refuses("SELECT TOP 5 WITH TIES a FROM t", "TIES")
    Parser("SELECT TOP 5 PERCENT a FROM t").map(_.sql).getOrElse("") should not include "PERCENT AS"
  }

  it should "accept the parenthesised T-SQL spelling" in {
    canonicalises("SELECT TOP (5) a FROM t", "LIMIT 5")
  }

  it should "apply to the FROM-less form too" in {
    canonicalises("SELECT TOP 1 1", "LIMIT 1")
    refuses("SELECT TOP 1 1 LIMIT 2", "TOP and LIMIT")
  }

  // ------------------------------------------- watcher comparison operator order

  "A watcher condition" should "accept >= and <=, not just > and <" in {
    def watcher(op: String): String =
      s"""CREATE WATCHER w AS
         |  EVERY 1 MINUTE
         |  FROM metrics WHERE cpu_usage > 90 WITHIN 5 MINUTES
         |  WHEN ctx.payload.hits.total $op 0 DO
         |  alert LOG 'hi' AT ERROR
         |END""".stripMargin

    // `>` and `<` were the controls that passed while `>=` and `<=` did not: the literal `>`
    // matched the first character of `>=` and left `=` for the value production.
    Seq(">", ">=", "<", "<=", "=", "<>", "!=").foreach { op =>
      withClue(s"[WHEN ... $op 0] ") { Parser(watcher(op)).isRight shouldBe true }
    }
  }
}
