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
import app.softnetwork.elastic.sql.schema.{Column, Table => SchemaTable}
import app.softnetwork.elastic.sql.`type`.SQLTypes
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks

/** No TYPE refusal before the column types are known (the lead's ruling of 2026-10-05).
  *
  * Every type rule `validate()` used to apply when the statement was PARSED -- where every column
  * is `Any` -- is asked once the schema is attached (`SingleSearch.validateResolved`), on the real
  * types. So:
  *   - parsing refuses no type, whatever the statement (with no schema, nothing is refused on types
  *     at all, and Elasticsearch answers);
  *   - each moved rule still refuses its TRUE mismatches, by name, once the types are known -- one
  *     row per rule below;
  *   - and the statements the parse-time rules refused although they are well typed now run: they
  *     are typed from the real columns.
  */
class TypeCheckResolutionSpec extends AnyFlatSpec with Matchers with TableDrivenPropertyChecks {

  // `SchemaTable`, because `TableDrivenPropertyChecks` shadows the name `Table`.
  private val schema = SchemaTable(
    "t",
    columns = List(
      Column("id", SQLTypes.Keyword),
      Column("g", SQLTypes.Keyword),
      Column("s", SQLTypes.Keyword),
      Column("n", SQLTypes.Int),
      Column("x", SQLTypes.Double),
      Column("d", SQLTypes.Date),
      Column("d2", SQLTypes.Date),
      Column("ts", SQLTypes.Timestamp),
      Column("b", SQLTypes.Boolean)
    )
  )

  private def parsed(sql: String): SingleSearch = Parser(sql) match {
    case Right(single: SingleSearch) => single
    case other => fail(s"[$sql] parsed as $other: no TYPE is refused at parse")
  }

  private def resolved(sql: String): Either[String, Unit] =
    parsed(sql).update(Some(schema)).validateResolved()

  "every TYPE rule moved off validate()" should
  "refuse nothing at parse, and refuse its true mismatches by name once the types are known" in {
    forAll(
      Table(
        ("rule", "sql", "refusal"),
        // a comparison: its operands' match, TIME against a date
        (
          "comparison",
          "SELECT id FROM t WHERE n + 1 > 'abc'",
          "'BIGINT' is not compatible with 'VARCHAR'"
        ),
        ("comparison", "SELECT id FROM t WHERE s = n", "'KEYWORD' is not compatible with 'INT'"),
        ("comparison (TIME)", "SELECT id FROM t WHERE CAST(ts AS TIME) > ts", "a TIME value"),
        // IN: the operand against the list's element type
        (
          "IN",
          "SELECT id FROM t WHERE CAST(n AS INT) IN ('a', 'b')",
          "'INT' is not compatible with 'VARCHAR'"
        ),
        (
          "IN (HAVING)",
          "SELECT g FROM t GROUP BY g HAVING COUNT(n) IN ('a', 'b')",
          "'BIGINT' is not compatible with 'VARCHAR'"
        ),
        // BETWEEN: its bounds, and its operand against them
        (
          "BETWEEN bounds",
          "SELECT id FROM t WHERE n BETWEEN 1 AND 'a'",
          "output 'BIGINT' is not compatible with input 'VARCHAR'"
        ),
        (
          "BETWEEN operand",
          "SELECT id FROM t WHERE CAST(s AS INT) BETWEEN 'a' AND 'b'",
          "output 'INT' is not compatible with input 'VARCHAR'"
        ),
        // NULLIF: #382's rule, and the matching rule over the types the statement fixes
        ("NULLIF (#382)", "SELECT NULLIF(s, 1) AS v FROM t", "compares KEYWORD with BIGINT"),
        // (a DATE against a TIMESTAMP is comparable, the ONE comparison rule: TIME is not)
        (
          "NULLIF (match)",
          "SELECT NULLIF(CAST(d AS DATE), CAST(ts AS TIME)) AS v FROM t",
          "output 'DATE' is not compatible with input 'TIME'"
        ),
        // CASE: boolean conditions, and a simple CASE's WHEN values against its operand
        (
          "CASE WHEN",
          "SELECT CASE WHEN n THEN 1 ELSE 0 END AS v FROM t",
          "CASE WHEN conditions must be of type BOOLEAN"
        ),
        (
          "CASE <expr> WHEN",
          "SELECT CASE n WHEN 'a' THEN 1 ELSE 0 END AS v FROM t",
          "CASE WHEN conditions must be of the same type as the expression"
        ),
        // GREATEST / LEAST: numbers, or dates and timestamps -- one kind
        (
          "GREATEST",
          "SELECT GREATEST('a', 'b') AS v FROM t",
          "GREATEST requires numeric arguments, or date and timestamp arguments, but got " +
          "VARCHAR for 'a'"
        ),
        (
          "LEAST",
          "SELECT LEAST(d, n) AS v FROM t",
          "LEAST requires numeric arguments, or date and timestamp arguments, but got DATE for d " +
          "and INT for n"
        ),
        // an arithmetic: date arithmetic's rules, and any other's operand match
        (
          "date arithmetic",
          "SELECT CURRENT_DATE * 2 AS v FROM t",
          "operator * cannot be applied to DATE and BIGINT"
        ),
        (
          "date arithmetic (column)",
          "SELECT d + d2 AS v FROM t",
          "operator + cannot be applied to DATE and DATE"
        ),
        (
          "arithmetic",
          "SELECT n + s AS v FROM t",
          "output 'INT' is not compatible with input 'KEYWORD'"
        ),
        ("arithmetic (literals)", "SELECT 1 + 'abc' AS v FROM t", "input 'VARCHAR'")
      )
    ) { (rule, sql, refusal) =>
      withClue(s"[$rule] [$sql] ") {
        // nothing is refused at parse, and with no schema nothing is refused on types: the
        // statement parses, and nothing else asks
        parsed(sql)
        resolved(sql).swap.getOrElse(fail("accepted once the types are known")) should include(
          refusal
        )
      }
    }
  }

  "the statements the parse-time rules refused although they are well typed" should
  "be accepted once the column types are known" in {
    forAll(
      Table(
        "sql",
        "SELECT id FROM t WHERE CURRENT_DATE - d > 30",
        "SELECT id FROM t WHERE d + 7 > CURRENT_DATE",
        "SELECT id FROM t WHERE NOW() - ts > 1",
        "SELECT id FROM t WHERE CAST(ts AS DATE) - d = 0",
        "SELECT id FROM t WHERE d + 1 = CAST('2024-02-01' AS DATE)",
        "SELECT id FROM t WHERE d + 1 > '2024-01-01'",
        "SELECT g, MAX(d) + 1 AS v FROM t GROUP BY g HAVING v > CAST('2024-01-01' AS DATE)",
        // a BOOLEAN column is a boolean condition -- refused at parse, where it was `Any`
        "SELECT CASE WHEN b THEN 1 ELSE 0 END AS v FROM t",
        // GREATEST / LEAST over dates and timestamps (the lead's ruling of 2026-10-05)
        "SELECT GREATEST(d, d2) AS v FROM t",
        "SELECT LEAST(d, ts, CURRENT_DATE) AS v FROM t",
        "SELECT id FROM t WHERE GREATEST(d, d2) > CAST('2024-02-01' AS DATE)",
        "SELECT g, GREATEST(MAX(d), MAX(d2)) AS v FROM t GROUP BY g"
      )
    ) { sql => withClue(s"[$sql] ")(resolved(sql) shouldBe Right(())) }
  }

  "a literal compared with a bare column" should
  "be left to Elasticsearch, which coerces it against the column's mapping" in {
    forAll(
      Table(
        "sql",
        "SELECT id FROM t WHERE n = '5'",
        "SELECT id FROM t WHERE d >= '2024-01-01'",
        "SELECT id FROM t WHERE s = 5",
        "SELECT id FROM t WHERE n IN ('1', '2')",
        "SELECT id FROM t WHERE n BETWEEN '1' AND '9'"
      )
    ) { sql => withClue(s"[$sql] ")(resolved(sql) shouldBe Right(())) }
  }

  "a string literal compared with a temporal expression" should
  "be the date or timestamp it spells, and refused when it spells none" in {
    resolved("SELECT id FROM t WHERE d + 1 > '2024-01-01T10:00:00Z'") shouldBe Right(())
    resolved("SELECT id FROM t WHERE DATE_TRUNC(d, MONTH) >= '2024-01-01'") shouldBe Right(())
    resolved("SELECT id FROM t WHERE d + 1 > 'tomorrow'").swap.getOrElse("") should include(
      "is not compatible with 'VARCHAR'"
    )
  }
}
