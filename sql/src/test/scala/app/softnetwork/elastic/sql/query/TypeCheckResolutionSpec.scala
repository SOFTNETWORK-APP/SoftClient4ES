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

import app.softnetwork.elastic.sql.{PainlessContext, PainlessContextType}
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
      Column("big", SQLTypes.BigInt),
      Column("x", SQLTypes.Double),
      Column("r", SQLTypes.Real),
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

  /** A quoted number is read as a VALUE of the other operand's type, as PostgreSQL's input for that
    * type reads it (the lead's ruling of 2026-10-05). Beside a whole-number type, `'1.5'` and
    * `'1.0'` are no integer -- PostgreSQL 16: `invalid input syntax for type integer`, for a
    * comparison and for `n + '1.5'` alike -- so the literal stays text and is refused by name at
    * every site that reads one; beside DOUBLE, REAL or DECIMAL (a DOUBLE here) any number is one.
    */
  "a quoted number beside a whole-number operand" should
  "be a whole number, refused by name otherwise, and any number beside a fractional one" in {
    forAll(
      Table(
        ("site", "sql", "refusal"),
        (
          "comparison (INT)",
          "SELECT id FROM t WHERE NULLIF(n, 0) = '1.5'",
          "'INT' is not compatible with 'VARCHAR'"
        ),
        (
          "comparison (INT)",
          "SELECT id FROM t WHERE NULLIF(n, 0) = '1.0'",
          "'INT' is not compatible with 'VARCHAR'"
        ),
        (
          "comparison (BIGINT)",
          "SELECT id FROM t WHERE COALESCE(big, 0) = '1.5'",
          "'BIGINT' is not compatible with 'VARCHAR'"
        ),
        (
          "comparison (BIGINT)",
          "SELECT id FROM t WHERE COALESCE(big, 0) = '1.0'",
          "'BIGINT' is not compatible with 'VARCHAR'"
        ),
        (
          "CASE condition",
          "SELECT id, CASE WHEN COALESCE(n, 0) = '1.5' THEN 1 ELSE 0 END AS v FROM t",
          "'BIGINT' is not compatible with 'VARCHAR'"
        ),
        (
          "an exponent",
          "SELECT id FROM t WHERE NULLIF(n, 0) = '1e3'",
          "'INT' is not compatible with 'VARCHAR'"
        ),
        (
          "past BIGINT",
          "SELECT id FROM t WHERE COALESCE(big, 0) = '99999999999999999999'",
          "'BIGINT' is not compatible with 'VARCHAR'"
        ),
        (
          "IN",
          "SELECT id FROM t WHERE NULLIF(n, 0) IN ('1', '1.5')",
          "'INT' is not compatible with 'VARCHAR'"
        ),
        (
          "IN (HAVING)",
          "SELECT g FROM t GROUP BY g HAVING MAX(big) IN ('1', '1.0')",
          "'BIGINT' is not compatible with 'VARCHAR'"
        ),
        (
          "BETWEEN",
          "SELECT id FROM t WHERE COALESCE(big, 0) BETWEEN '1' AND '1.0'",
          "output 'BIGINT' is not compatible with input 'VARCHAR'"
        ),
        ("NULLIF (INT)", "SELECT NULLIF(n, '1.5') AS v FROM t", "compares INT with VARCHAR"),
        (
          "NULLIF (BIGINT)",
          "SELECT NULLIF(big, '1.0') AS v FROM t",
          "compares BIGINT with VARCHAR"
        ),
        (
          "CASE <expr> WHEN (INT)",
          "SELECT CASE n WHEN '1.5' THEN 1 ELSE 0 END AS v FROM t",
          "CASE WHEN conditions must be of the same type as the expression"
        ),
        (
          "CASE <expr> WHEN (BIGINT)",
          "SELECT CASE big WHEN '1.0' THEN 1 ELSE 0 END AS v FROM t",
          "CASE WHEN conditions must be of the same type as the expression"
        ),
        // a group filter compares the metric, which is no text
        (
          "HAVING",
          "SELECT g FROM t GROUP BY g HAVING MAX(n) > '4.5'",
          "HAVING cannot be applied to MAX(n) > '4.5'"
        ),
        (
          "arithmetic (INT)",
          "SELECT n + '1.5' AS v FROM t",
          "output 'INT' is not compatible with input 'VARCHAR'"
        ),
        (
          "arithmetic (INT, literal first)",
          "SELECT '1.0' * n AS v FROM t",
          "output 'VARCHAR' is not compatible with input 'INT'"
        ),
        (
          "arithmetic (BIGINT)",
          "SELECT big - '1.0' AS v FROM t",
          "output 'BIGINT' is not compatible with input 'VARCHAR'"
        ),
        (
          "arithmetic (per group)",
          "SELECT g, SUM(n) + '1.5' AS v FROM t GROUP BY g",
          "output 'INT' is not compatible with input 'VARCHAR'"
        )
      )
    ) { (site, sql, refusal) =>
      withClue(s"[$site] [$sql] ") {
        parsed(sql)
        resolved(sql).swap.getOrElse(fail("accepted once the types are known")) should include(
          refusal
        )
      }
    }
    forAll(
      Table(
        "sql",
        // a whole number is an integer, however it is written
        "SELECT id FROM t WHERE NULLIF(n, 0) = '1'",
        "SELECT id FROM t WHERE NULLIF(n, 0) = ' 1 '",
        "SELECT id FROM t WHERE NULLIF(n, 0) = '+1'",
        "SELECT id FROM t WHERE COALESCE(big, 0) = '10000000000'",
        "SELECT CASE big WHEN '2' THEN 1 ELSE 0 END AS v FROM t",
        "SELECT n + '1' AS v FROM t",
        // beside DOUBLE, REAL or DECIMAL, any number
        "SELECT id FROM t WHERE COALESCE(x, 0) = '1.5'",
        "SELECT id FROM t WHERE COALESCE(x, 0) = '1e0'",
        "SELECT id FROM t WHERE COALESCE(r, 0) = '1.0'",
        "SELECT id FROM t WHERE CAST(s AS DECIMAL(10,2)) = '1.5'",
        "SELECT id FROM t WHERE COALESCE(x, 0) IN ('1', '1.5')",
        "SELECT id FROM t WHERE COALESCE(x, 0) BETWEEN '1.0' AND '1.5'",
        "SELECT NULLIF(x, '1.5') AS v FROM t",
        "SELECT CASE x WHEN '1.5' THEN 1 ELSE 0 END AS v FROM t",
        "SELECT x + '1.5' AS v FROM t",
        "SELECT g, AVG(n) + '1.5' AS v FROM t GROUP BY g",
        "SELECT g FROM t GROUP BY g HAVING AVG(n) > '4.5'",
        // beside a temporal a number is a number of days, whatever its fraction
        "SELECT d + '1.5' AS v FROM t"
      )
    ) { sql => withClue(s"[$sql] ")(resolved(sql) shouldBe Right(())) }
  }

  /** The literal reading a comparison renders is the one its type rule judged: a DOUBLE operand
    * compares the number a quoted `'1.5'` spells, and an integer one the whole number `'1'` spells.
    */
  "a quoted number read beside a number" should "render as that number" in {
    def criteriaPainless(sql: String): String = {
      val search = parsed(sql).update(Some(schema))
      search.validateResolved() shouldBe Right(())
      search.where
        .flatMap(_.criteria)
        .map(_.painless(Some(PainlessContext(context = PainlessContextType.Query))))
        .getOrElse(fail(s"[$sql] no WHERE"))
    }
    criteriaPainless("SELECT id FROM t WHERE COALESCE(x, 0) = '1.5'") should include("== 1.5")
    criteriaPainless("SELECT id FROM t WHERE COALESCE(big, 0) = '1'") should include("== 1L")
  }
}
