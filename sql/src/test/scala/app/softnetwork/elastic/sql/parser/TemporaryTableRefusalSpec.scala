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

/** `CREATE [LOCAL | GLOBAL] TEMPORARY TABLE` is recognised so that it can be refused BY NAME.
  *
  * Two contracts, and the second is the one that is easy to lose.
  *
  * FIRST, the refusal names the construct, says why, and gives the remedy.
  *
  * SECOND, it is a PARSE-TIME rejection — `Parser.apply` returns a `Left`. Recognising the shape
  * and letting an executor refuse it later would make the parse succeed, i.e. report the capability
  * as PRESENT to every consumer that only looks at the parse. The Epic 21 corpus replay is one such
  * consumer: its three `rejected_pending_policy` rows are these statements, and a flip to `parses`
  * there is defined to mean *a fix went too far*.
  *
  * Every assertion also rules out `Parser.apply`'s `NonFatal` boundary catch: a catch turns a
  * restored `throw` into a `Left` whose message CONTAINS the same reason, so `isLeft` plus a
  * substring match is satisfied by the defect as well as by the fix (story 21.4's lesson).
  */
class TemporaryTableRefusalSpec extends AnyFlatSpec with Matchers {

  private def refuses(sql: String, spelling: String): Unit = {
    withClue(s"[$sql] ") { noException should be thrownBy Parser(sql) }
    val result = Parser(sql)
    withClue(
      s"[$sql] -> $result - a temporary table must be refused AT PARSE TIME. A `Right` here " +
      "reports the capability as present to every consumer that only looks at the parse. "
    ) {
      result.isLeft shouldBe true
    }
    val msg = result.left.toOption.map(_.msg).getOrElse("")
    withClue(
      s"[$sql] msg=[$msg] - this must come from an `err(...)` in the grammar, not from " +
      "`Parser.apply`'s boundary catch. "
    ) {
      msg should not startWith Parser.InternalParseFailure
    }
    withClue(s"[$sql] msg=[$msg] ") {
      // names the construct, as the caller spelled it ...
      msg should startWith(s"CREATE $spelling is not supported:")
      // ... says why ...
      msg should include("no session scope")
      // ... and gives the remedy.
      msg should include("Use CREATE TABLE for a regular index")
      // The message it replaced named an unrelated production of the grammar.
      msg should not include "expected but"
    }
    ()
  }

  behavior of "CREATE TEMPORARY TABLE (recognise-to-reject)"

  it should "refuse MySQL's CREATE TEMPORARY TABLE" in {
    refuses("CREATE TEMPORARY TABLE tmp (COL INTEGER)", "TEMPORARY TABLE")
  }

  it should "refuse SQL-92's CREATE LOCAL TEMPORARY TABLE" in {
    refuses("CREATE LOCAL TEMPORARY TABLE tmp (COL INTEGER)", "LOCAL TEMPORARY TABLE")
  }

  it should "refuse SQL-92's CREATE GLOBAL TEMPORARY TABLE" in {
    refuses("CREATE GLOBAL TEMPORARY TABLE tmp (COL INTEGER)", "GLOBAL TEMPORARY TABLE")
  }

  /** `ON COMMIT ... ROWS` is transaction semantics and Elasticsearch has no transactions, so the
    * only safe treatment is to refuse the statement that carries it. `DropTable.cascade` is the
    * standing proof that a parsed-and-ignored clause survives for years; for a clause that governs
    * data LIFETIME that would be a silent wrong answer. These cases prove the clause cannot be
    * reached and silently dropped — the refusal fires whether it is present or not, and in both
    * spellings.
    */
  it should "refuse it with ON COMMIT PRESERVE ROWS, rather than accepting and ignoring the clause" in {
    refuses(
      "CREATE LOCAL TEMPORARY TABLE tmp (COL INTEGER) ON COMMIT PRESERVE ROWS",
      "LOCAL TEMPORARY TABLE"
    )
  }

  it should "refuse it with ON COMMIT DELETE ROWS" in {
    refuses(
      "CREATE GLOBAL TEMPORARY TABLE tmp (COL INTEGER) ON COMMIT DELETE ROWS",
      "GLOBAL TEMPORARY TABLE"
    )
  }

  /** The refusal is raised at the statement HEADER, before the body is parsed, so it is the same
    * reason whatever the body is. That is what makes the message deterministic: an unparseable
    * column list, or an `err` raised inside an embedded SELECT, cannot substitute its own reason.
    */
  it should "refuse it whatever the body is" in {
    refuses("CREATE TEMPORARY TABLE tmp AS SELECT a FROM src", "TEMPORARY TABLE")
    refuses("CREATE TEMPORARY TABLE tmp", "TEMPORARY TABLE")
    refuses("CREATE TEMPORARY TABLE tmp (", "TEMPORARY TABLE")
    refuses("CREATE TEMPORARY TABLE tmp (COL ???)", "TEMPORARY TABLE")
    refuses("CREATE TEMPORARY TABLE tmp AS SELECT a FROM src WHERE a = 1 AND", "TEMPORARY TABLE")
  }

  /** MySQL's own spelling of the probe-shaped create. The refusal consumes `CREATE TEMPORARY TABLE`
    * and fires before `IF NOT EXISTS` is read, so the clause cannot turn the refusal into a silent
    * no-op.
    */
  it should "refuse it with IF NOT EXISTS" in {
    refuses("CREATE TEMPORARY TABLE IF NOT EXISTS tmp (COL INTEGER)", "TEMPORARY TABLE")
  }

  it should "refuse it case-insensitively and with a quoted name" in {
    refuses("create temporary table tmp (COL INTEGER)", "TEMPORARY TABLE")
    refuses("Create Local Temporary Table tmp (COL INTEGER)", "LOCAL TEMPORARY TABLE")
    refuses("CREATE TEMPORARY TABLE `tmp` (`COL` INTEGER)", "TEMPORARY TABLE")
    refuses("""CREATE TEMPORARY TABLE "tmp" ("COL" INTEGER)""", "TEMPORARY TABLE")
  }

  /** The verbatim statements Tableau Desktop 2026.2.2 emitted through the JDBC driver in its SQL-92
    * dialect (Epic 19 BI corpus rows `tableau.sql92.wx.001` / `wx.005` / `wx.007`). They are the
    * reason this production exists, and the corpus replay asserts they stay rejected.
    */
  it should "refuse the captured Tableau SQL-92 capability probe" in {
    Seq(
      "XT__EEE____D_E_DA___CC____C__B_B__BFB____1_Connect_CheckCreateTempTableCap",
      "XT___A_E__B__B_C_______B_F_____AA___C_D__1_Connect_CheckCreateTempTableCap",
      "XT___DE_EA_A_________A_B_D___A__F_CBA__C_1_Connect_CheckCreateTempTableCap"
    ).foreach { name =>
      refuses(
        s"""CREATE LOCAL TEMPORARY TABLE "$name" ( "COL" INTEGER ) ON COMMIT PRESERVE ROWS""",
        "LOCAL TEMPORARY TABLE"
      )
    }
  }

  behavior of "what the refusal must NOT narrow"

  /** `TEMPORARY` / `LOCAL` / `GLOBAL` are not reserved words: the discriminating token is consumed
    * BEFORE the object name, so a table or column that happens to be called one of them is
    * unaffected. The differential probe over 994 statements found zero verdict changes; these are
    * the shapes closest to the new production, pinned by name.
    */
  it should "still accept a table or column named like the new keywords" in {
    Seq(
      "CREATE TABLE temporary (COL INTEGER)",
      "CREATE TABLE local (COL INTEGER)",
      "CREATE TABLE global (COL INTEGER)",
      "CREATE TABLE t (temporary INTEGER)",
      "DROP TABLE temporary",
      "SELECT temporary FROM t",
      "SELECT local, global FROM t",
      "SELECT t.temporary FROM t"
    ).foreach { sql =>
      withClue(s"[$sql] ") { Parser(sql).isRight shouldBe true }
    }
  }

  it should "still accept every ordinary CREATE TABLE spelling" in {
    Seq(
      "CREATE TABLE dest (COL INTEGER)",
      "CREATE TABLE IF NOT EXISTS dest (COL INTEGER)",
      "CREATE OR REPLACE TABLE dest (COL INTEGER)",
      "CREATE TABLE dest AS SELECT a FROM src",
      "CREATE OR REPLACE TABLE dest AS SELECT a FROM src",
      "CREATE TABLE dest (COL INTEGER) OPTIONS (number_of_shards = 1)"
    ).foreach { sql =>
      withClue(s"[$sql] ") { Parser(sql).isRight shouldBe true }
    }
  }

  /** A mistyped `CREATE TABLE` must not be told about temporary tables. `Failure.append` keeps the
    * LATER result when two alternatives fail at the same offset, which is why the production sits
    * FIRST in `ddlStatement` — from the front it yields the tie instead of winning it. Measured
    * both ways before choosing the position.
    */
  it should "not mention TEMPORARY when diagnosing an unrelated CREATE typo" in {
    Seq("CREATE TABEL t (COL INTEGER)", "CREATE PIPLINE p").foreach { sql =>
      val msg = Parser(sql).left.toOption.map(_.msg).getOrElse("")
      withClue(s"[$sql] msg=[$msg] ") {
        msg should not include "TEMPORARY"
      }
    }
  }

  /** The spellings this production deliberately does NOT claim, each measured on both trees: they
    * were rejected before it and are rejected after it, just not by name. Recorded so the boundary
    * of the change is a measured fact rather than an assumption, and so that widening it later is a
    * decision rather than an accident.
    *
    *   - `CREATE OR REPLACE TEMPORARY TABLE` — a DuckDB extension, neither SQL-92 nor MySQL, and
    *     the shape of no captured BI statement. `TEMPORARY` must follow `CREATE` (optionally after
    *     `LOCAL` / `GLOBAL`) for this production to fire.
    *   - `DROP TEMPORARY TABLE` — MySQL's matching drop. Tableau's probe drops with `DROP TABLE IF
    *     EXISTS`, which parses and is then refused by the index-name validator.
    */
  it should "leave the spellings it does not claim rejected, but unnamed" in {
    Seq(
      "CREATE OR REPLACE TEMPORARY TABLE tmp (COL INTEGER)",
      "DROP TEMPORARY TABLE tmp"
    ).foreach { sql =>
      val result = Parser(sql)
      withClue(s"[$sql] -> $result ") { result.isLeft shouldBe true }
      val msg = result.left.toOption.map(_.msg).getOrElse("")
      withClue(s"[$sql] msg=[$msg] ") {
        msg should not include "is not supported"
        msg should not startWith Parser.InternalParseFailure
      }
    }
  }

  /** A `CREATE` whose second word is `LOCAL` or `GLOBAL` but which never reaches `TEMPORARY` now
    * reports the missing `TEMPORARY` rather than a different alternative's failure, because this
    * production consumed one token more than any sibling. Both inputs were rejected before and are
    * rejected now; only the diagnosis moved, and it moved towards the word the user omitted.
    */
  it should "diagnose a scope word that is not followed by TEMPORARY" in {
    Seq("CREATE LOCAL TABLE t (COL INTEGER)", "CREATE GLOBAL TABLE t (COL INTEGER)").foreach {
      sql =>
        val result = Parser(sql)
        withClue(s"[$sql] -> $result ") { result.isLeft shouldBe true }
        withClue(s"[$sql] ") {
          result.left.toOption.map(_.msg).getOrElse("") should include("TEMPORARY")
        }
    }
  }

  /** `ON COMMIT ... ROWS` on a NON-temporary table is out of this production's scope and was
    * already loud before it: `Parser.apply` is `phrase`-anchored, so the trailing clause is a
    * rejection rather than a silently dropped suffix. Pinned so that the boundary of the change is
    * a measured fact rather than an assumption.
    */
  it should "leave ON COMMIT on a regular CREATE TABLE as the trailing-input rejection it was" in {
    val sql = "CREATE TABLE dest (COL INTEGER) ON COMMIT DELETE ROWS"
    Parser(sql).isLeft shouldBe true
    Parser(sql).left.toOption.map(_.msg).getOrElse("") should not include
    "is not supported"
  }
}
