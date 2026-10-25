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

package app.softnetwork.elastic.sql.query

import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.schema.{Column, Table}
import app.softnetwork.elastic.sql.`type`.SQLTypes
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** A HAVING key filter on a GROUP BY key whose SCRIPT returns a date, a time or a timestamp.
  *
  * Such a level is a `terms` aggregation on the key's script, a string `terms`: Elasticsearch
  * compares the filter's values with the TEXT the script's value prints (`"2024-01-31T00:00Z"` for
  * `LAST_DAY(d)`), which a date literal does not match -- `=` / `IN` kept no group and `<>` every
  * group, HTTP 200. The filter is refused, by the key's output type: at parse where the function
  * gives it, on the resolved statement where only the mapping does.
  */
class TemporalScriptKeyFilterSpec extends AnyFlatSpec with Matchers {

  private val schema: Table = Table(
    "t",
    columns = List(
      Column("d", SQLTypes.Date),
      Column("ts", SQLTypes.Timestamp),
      Column("s", SQLTypes.Keyword),
      Column("sd", SQLTypes.Keyword),
      Column("n", SQLTypes.Int)
    )
  )

  private val Lead = "HAVING cannot filter a DATE grouping level"

  private def parsed(sql: String): Either[String, SingleSearch] =
    Parser(sql) match {
      case Right(ss: SingleSearch) => Right(ss)
      case Right(other)            => fail(s"[$sql] expected a SingleSearch, got $other")
      case Left(err)               => Left(err.msg)
    }

  private def resolved(sql: String): Either[String, Unit] =
    parsed(sql) match {
      case Right(ss) => ss.update(Some(schema)).validateResolved()
      case Left(err) => fail(s"[$sql] refused at parse: $err")
    }

  private def grouped(companion: String, key: String, condition: String): String =
    companion match {
      case "alone" => s"SELECT $key AS k, COUNT(*) AS c FROM t GROUP BY $key HAVING $condition"
      case "first" =>
        s"SELECT s, $key AS k, COUNT(*) AS c FROM t GROUP BY s, $key HAVING $condition"
      case "date" => s"SELECT d, $key AS k, COUNT(*) AS c FROM t GROUP BY d, $key HAVING $condition"
    }

  "a key filter on LAST_DAY(d)" should "be refused at parse, alone and beside a companion level" in {
    val refusals = for {
      companion <- Seq("alone", "first", "date")
      condition <- Seq(
        "LAST_DAY(d) = '2024-01-31'",
        "LAST_DAY(d) IN ('2024-01-31', '2024-03-31')",
        "LAST_DAY(d) <> '2024-01-31'",
        "LAST_DAY(d) LIKE '2024-01%'"
      )
      sql = grouped(companion, "LAST_DAY(d)", condition)
    } yield sql -> parsed(sql)
    val wrong = refusals.collect {
      case (sql, outcome)
          if !outcome.left.exists(m =>
            m.startsWith(Lead) && m.contains("compares with the condition as text") &&
            m.endsWith(
              "Restrict the rows in WHERE instead, with the same condition on LAST_DAY(d)."
            )
          ) =>
        s"[$sql] $outcome"
    }
    withClue(wrong.mkString("\n")) { wrong shouldBe empty }
  }

  it should "name the key expression in its remedy when HAVING spells the key by its SELECT alias" in {
    val reason = parsed(grouped("alone", "LAST_DAY(d)", "k = '2024-01-31'")).left.getOrElse("")
    reason should startWith(Lead)
    reason should include("with the same condition on LAST_DAY(d)")
    reason should not include "WHERE k"
  }

  "a key filter on another function returning a date or a time" should "be refused at parse" in {
    Seq(
      "TO_DATE(sd, 'yyyy-MM-dd')"   -> "'2024-01-03'",
      "CAST(d AS DATE)"             -> "'2024-01-03'",
      "CAST(ts AS TIME)"            -> "'11:15:00'",
      "DATE_ADD(d, INTERVAL 1 DAY)" -> "'2024-01-04'",
      "COALESCE(d, CURRENT_DATE)"   -> "'2024-01-03'"
    ).foreach { case (key, value) =>
      val sql = grouped("date", key, s"$key = $value")
      withClue(sql) { parsed(sql).left.getOrElse("") should startWith(Lead) }
    }
  }

  "a key filter on a key whose date type only the mapping gives" should
  "be accepted at parse and refused once the statement is resolved" in {
    Seq("COALESCE(d, d)", "CASE WHEN n > 0 THEN d ELSE d END").foreach { key =>
      val sql = grouped("date", key, s"$key = '2024-01-03'")
      withClue(sql) {
        parsed(sql).isRight shouldBe true
        resolved(sql).left.getOrElse("") should startWith(Lead)
      }
    }
  }

  "the other key filters" should "keep the verdicts they had" in {
    // a DATE_TRUNC level is a date histogram: its own reason
    parsed(grouped("alone", "DATE_TRUNC(d, MONTH)", "DATE_TRUNC(d, MONTH) = '2024-01-01'")).left
      .getOrElse("") should include("which Elasticsearch groups with a date histogram")
    // a bare date column is a terms level on its field: filtered
    val bare = "SELECT d, COUNT(*) AS c FROM t GROUP BY d HAVING d = '2024-01-03'"
    parsed(bare).isRight shouldBe true
    resolved(bare) shouldBe Right(())
    // a number of a date: not a date
    val year = grouped("alone", "YEAR(d)", "YEAR(d) = 2024")
    parsed(year).isRight shouldBe true
    resolved(year) shouldBe Right(())
  }

  "Bucket.isTemporalScriptKey" should "hold for a scripted key rendering a date, a time or a timestamp" in {
    def bucket(key: String, withSchema: Boolean): Bucket =
      parsed(s"SELECT COUNT(*) AS c FROM t GROUP BY $key") match {
        case Right(ss) =>
          (if (withSchema) ss.update(Some(schema)) else ss).groupBy.get.buckets.head
        case Left(err) => fail(err)
      }
    Seq("LAST_DAY(d)", "DATE_ADD(d, INTERVAL 1 DAY)", "CAST(ts AS TIME)").foreach { key =>
      withClue(key) {
        bucket(key, withSchema = false).isTemporalScriptKey shouldBe true
        bucket(key, withSchema = true).isTemporalScriptKey shouldBe true
      }
    }
    withClue("COALESCE(d, d)") {
      bucket("COALESCE(d, d)", withSchema = false).isTemporalScriptKey shouldBe false
      bucket("COALESCE(d, d)", withSchema = true).isTemporalScriptKey shouldBe true
    }
    Seq("d", "DATE_TRUNC(d, MONTH)", "YEAR(d)", "DATE_FORMAT(d, '%Y-%m')", "UPPER(s)").foreach {
      key =>
        withClue(key) { bucket(key, withSchema = true).isTemporalScriptKey shouldBe false }
    }
  }
}
