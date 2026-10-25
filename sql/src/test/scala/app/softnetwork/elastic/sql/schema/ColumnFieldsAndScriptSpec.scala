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

package app.softnetwork.elastic.sql.schema

import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query.CreateTable
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** A column declares its sub-fields and its script together, in the order `Column.sql` renders
  * them: `FIELDS (…)` then `SCRIPT AS (…)`.
  *
  * 🔴 The grammar read a script OR sub-fields, never both, while `Column.sql` renders both: `c
  * VARCHAR FIELDS (raw KEYWORD) SCRIPT AS (UPPER(k))` was refused with `')' expected but 'S'
  * found`, so a rendered table did not re-parse. A materialized view renders exactly that for a
  * string GROUP BY key (the key gets a `keyword` sub-field), and re-parses its DDL when it deploys.
  *
  * Each row parses a table, renders it, parses the rendering again, and asserts that the second
  * parse renders the same text and reads the column the same way.
  */
class ColumnFieldsAndScriptSpec extends AnyFlatSpec with Matchers {

  /** What a parsed column says, compared across the round trip. */
  private case class Facts(
    script: Option[String],
    stored: Option[Boolean],
    fields: List[String],
    default: Option[String],
    comment: Option[String]
  )

  private def statement(column: String): String =
    s"CREATE OR REPLACE TABLE x (k KEYWORD, n INT, $column, PRIMARY KEY (k))"

  private def parsed(ddl: String): Table =
    Parser(ddl) match {
      case Right(ct: CreateTable) => ct.schema
      case other                  => fail(s"[$ddl] is not one CREATE TABLE: $other")
    }

  private def factsOf(table: Table): Facts = {
    val c = table.columns.find(_.name == "c").getOrElse(fail(s"no column `c` in [${table.sql}]"))
    Facts(
      c.script.map(_.script),
      c.script.map(_.materialized),
      c.multiFields.map(_.name),
      c.defaultValue.map(_.sql),
      c.comment
    )
  }

  /** Parse, render, re-parse: the rendering is stable and the column reads the same. */
  private def assertRoundTrip(column: String, expected: Facts): Unit = {
    val first = parsed(statement(column))
    val rendered = first.sql
    withClue(s"[$column] rendered as [$rendered] ") {
      factsOf(first) shouldBe expected
      val second = parsed(rendered)
      second.sql shouldBe rendered
      factsOf(second) shouldBe expected
    }
  }

  behavior of "a column with sub-fields and a script"

  it should "round-trip FIELDS (…) SCRIPT AS (…)" in {
    assertRoundTrip(
      "c VARCHAR FIELDS (raw KEYWORD) SCRIPT AS (UPPER(k))",
      Facts(Some("UPPER(k)"), Some(false), List("raw"), None, None)
    )
  }

  it should "round-trip FIELDS (…) SCRIPT AS (…) STORED" in {
    assertRoundTrip(
      "c VARCHAR FIELDS (raw KEYWORD) SCRIPT AS (UPPER(k)) STORED",
      Facts(Some("UPPER(k)"), Some(true), List("raw"), None, None)
    )
  }

  it should "round-trip FIELDS (…) SCRIPT AS (…) with a COMMENT, on the column and the sub-field" in {
    assertRoundTrip(
      "c VARCHAR FIELDS (raw KEYWORD COMMENT 'kw') SCRIPT AS (UPPER(k)) STORED COMMENT 'upper k'",
      Facts(Some("UPPER(k)"), Some(true), List("raw"), None, Some("upper k"))
    )
  }

  it should "round-trip FIELDS (…) SCRIPT AS (…) with a DEFAULT and a COMMENT" in {
    assertRoundTrip(
      "c VARCHAR FIELDS (raw KEYWORD) SCRIPT AS (UPPER(k)) DEFAULT 'none' COMMENT 'upper k'",
      Facts(Some("UPPER(k)"), Some(false), List("raw"), Some("'none'"), Some("upper k"))
    )
  }

  behavior of "the column shapes that already parsed"

  it should "round-trip a script alone" in {
    assertRoundTrip(
      "c VARCHAR SCRIPT AS (UPPER(k))",
      Facts(Some("UPPER(k)"), Some(false), Nil, None, None)
    )
  }

  it should "round-trip a STORED script alone" in {
    assertRoundTrip(
      "c VARCHAR SCRIPT AS (UPPER(k)) STORED",
      Facts(Some("UPPER(k)"), Some(true), Nil, None, None)
    )
  }

  it should "round-trip sub-fields alone" in {
    assertRoundTrip(
      "c VARCHAR FIELDS (raw KEYWORD)",
      Facts(None, None, List("raw"), None, None)
    )
  }

  it should "round-trip a script with a DEFAULT" in {
    assertRoundTrip(
      "c INT SCRIPT AS (ABS(n)) DEFAULT 0",
      Facts(Some("ABS(n)"), Some(false), Nil, Some("0"), None)
    )
  }

  behavior of "a column with its script before its sub-fields"

  // Never rendered, so never needed: the one order is `Column.sql`'s.
  it should "stay refused" in {
    Parser(statement("c VARCHAR SCRIPT AS (UPPER(k)) FIELDS (raw KEYWORD)")) match {
      case Left(error) => error.msg shouldBe "')' expected but 'F' found"
      case other       => fail(s"SCRIPT AS (…) FIELDS (…) was accepted: $other")
    }
  }
}
