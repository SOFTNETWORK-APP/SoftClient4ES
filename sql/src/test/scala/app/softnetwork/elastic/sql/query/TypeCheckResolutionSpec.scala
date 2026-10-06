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

  /** The Painless a statement's one script field renders in a query, its declarations included. */
  private def fieldPainless(sql: String): String = {
    val search = parsed(sql).update(Some(schema))
    search.validateResolved() shouldBe Right(())
    val field = search.scriptFields
      .filterNot(_.isAggregation)
      .headOption
      .getOrElse(fail(s"[$sql] no script field"))
    val ctx = PainlessContext(context = PainlessContextType.Query)
    val body = field.painless(Some(ctx))
    s"$ctx$body"
  }

  /** The ingest script of every computed column of a CREATE TABLE. */
  private def ingestPainless(ddl: String): String = Parser(ddl) match {
    case Right(create: CreateTable) =>
      create.schema.columns.flatMap(_.script).map(_.source).mkString(" ;; ")
    case other => fail(s"[$ddl] parsed as $other")
  }

  /** NULLIF reads a string literal beside a temporal as the date or the timestamp it spells, and
    * renders it from that READING: the literal's text is never parsed on a document -- by the
    * operand's own format it could not be, for a date-time beside a DATE or a date beside a
    * TIMESTAMP -- and a literal that is the answer is the value its reading denotes.
    */
  "a NULLIF literal read as a date or a timestamp" should
  "be rendered from its reading, never parsed on a document, in a query and a computed column" in {
    forAll(
      Table(
        ("expression", "type", "value"),
        ("NULLIF(d, '2024-01-31 10:00:00')", "DATE", None),
        ("NULLIF(d, '2024-01-31T10:00:00Z')", "DATE", None),
        ("NULLIF(ts, '2024-01-31')", "TIMESTAMP", None),
        ("NULLIF(COALESCE(d, d2), '2024-01-31T10:00:00Z')", "DATE", None),
        ("NULLIF('2024-01-31T10:00:00Z', d)", "DATE", Some("LocalDate.ofEpochDay(19753L)")),
        (
          "NULLIF('2024-01-31', ts)",
          "TIMESTAMP",
          Some("Instant.ofEpochMilli(1706659200000L).atZone(ZoneId.of('Z'))")
        )
      )
    ) { (expression, sqlType, value) =>
      Seq(
        "query" -> fieldPainless(s"SELECT $expression AS v FROM t"),
        "computed column" -> ingestPainless(
          s"CREATE TABLE z (id INT, d DATE, d2 DATE, ts TIMESTAMP, " +
          s"v $sqlType SCRIPT AS ($expression), PRIMARY KEY (id))"
        )
      ).foreach { case (venue, script) =>
        withClue(s"[$venue] [$expression] $script ") {
          script should not include "\"2024-01-31"
          value.foreach(v => script should include(v))
        }
      }
    }
  }

  /** `CASE b WHEN '1'` / `WHEN TRUE` compares with a boolean CONSTANT: rendered as the number it
    * is, never as `(true ? 1L : 0L)`, a conditional over a constant, which Elasticsearch 6.8's
    * Painless refuses to compile ("Extraneous conditional statement").
    */
  "a simple CASE over a BOOLEAN, compared with a boolean constant" should
  "render the constant as a number, never as a conditional over a constant" in {
    forAll(
      Table(
        ("value", "constant"),
        ("'1'", "1L"),
        ("'true'", "1L"),
        ("TRUE", "1L"),
        ("'0'", "0L"),
        ("'false'", "0L"),
        ("FALSE", "0L")
      )
    ) { (value, constant) =>
      val script = fieldPainless(s"SELECT CASE b WHEN $value THEN 1 ELSE 0 END AS v FROM t")
      withClue(s"[$value] $script ") {
        script should not include "(true ?"
        script should not include "(false ?"
        script should include(s"= $constant;")
      }
    }
  }

  /** A BOOLEAN column as a CASE condition is NULL on a document that has no value, and a NULL
    * condition is not true: rendered `== true`, never as the bare value, which threw on that
    * document.
    */
  "a BOOLEAN column as a CASE condition" should
  "render NULL as not true, in a query and in a computed column" in {
    fieldPainless("SELECT CASE WHEN b THEN 1 ELSE 0 END AS v FROM t") should include(
      "== true ? 1 : 0"
    )
    ingestPainless(
      "CREATE TABLE z (id INT, flag BOOLEAN, c INT SCRIPT AS (CASE WHEN flag THEN 1 ELSE 0 END), " +
      "PRIMARY KEY (id))"
    ) should include("== true ? 1 : 0")
  }

  /** The type every SELECT item REPORTS -- what a `CREATE TABLE … AS SELECT` declares -- by alias.
    */
  private def reported(sql: String): Map[String, String] = {
    val search = parsed(sql).update(Some(schema))
    search.validateResolved() shouldBe Right(())
    search.select.fields.map { f =>
      f.fieldAlias.map(_.alias).getOrElse(f.identifier.sql) -> f.identifier.reportedType.typeId
    }.toMap
  }

  /** A NUMBER converted to a DATE is refused by name, as PostgreSQL refuses it (`cannot cast type
    * integer to date`) -- in a query, per group and in a computed column; a number converted to a
    * TIMESTAMP keeps core's epoch-millisecond conversion.
    */
  "a number converted to a DATE" should "be refused by name, in a query and a computed column" in {
    forAll(
      Table(
        ("sql", "refusal"),
        ("SELECT (d - d2)::DATE AS v FROM t", "cannot cast BIGINT to DATE"),
        ("SELECT CAST(n AS DATE) AS v FROM t", "cannot cast INT to DATE"),
        ("SELECT n::DATE AS v FROM t", "cannot cast INT to DATE"),
        ("SELECT CAST(x AS DATE) AS v FROM t", "cannot cast DOUBLE to DATE"),
        ("SELECT CAST(1 AS DATE) AS v FROM t", "cannot cast BIGINT to DATE"),
        (
          "SELECT CAST(YEAR(d) AS DATE) AS v FROM t",
          "to DATE in expression: CAST(YEAR(d) AS DATE)"
        ),
        ("SELECT TRY_CAST(n AS DATE) AS v FROM t", "cannot cast INT to DATE"),
        ("SELECT CONVERT(n, DATE) AS v FROM t", "cannot cast INT to DATE"),
        ("SELECT id FROM t WHERE (d - d2)::DATE = d", "cannot cast BIGINT to DATE"),
        ("SELECT g, (MAX(d) - MIN(d))::DATE AS v FROM t GROUP BY g", "cannot cast BIGINT to DATE")
      )
    ) { (sql, refusal) =>
      withClue(s"[$sql] ") {
        parsed(sql)
        resolved(sql).swap.getOrElse(fail("accepted once the types are known")) should include(
          refusal
        )
      }
    }
    Parser(
      "CREATE TABLE z (id INT, n INT, c DATE SCRIPT AS (CAST(n AS DATE)), PRIMARY KEY (id))"
    ) match {
      case Left(error) =>
        error.msg should include("Column 'c': Type mismatch: cannot cast INT to DATE")
      case other => fail(s"a computed column CAST(n AS DATE) was accepted: $other")
    }
    forAll(
      Table(
        "sql",
        "SELECT (d - d2)::TIMESTAMP AS v FROM t",
        "SELECT CAST(n AS TIMESTAMP) AS v FROM t",
        "SELECT (d - d2)::BIGINT AS v FROM t",
        "SELECT CAST(ts AS DATE) AS v FROM t",
        "SELECT CAST('2024-01-31' AS DATE) AS v FROM t"
      )
    ) { sql => withClue(s"[$sql] ")(resolved(sql) shouldBe Right(())) }
  }

  /** A `CASE` or a `NULLIF` used as an operand is ONE value (issue #380): the operator applies to
    * every branch -- never `c ? n : 0 + 1`, the ELSE branch plus one -- and a NULL it produces
    * makes the result NULL rather than failing the script.
    */
  "a CASE or a NULLIF used as an operand" should
  "be rendered as one value, in a query and in a computed column" in {
    val notNull = fieldPainless("SELECT (CASE WHEN n > 0 THEN 1 ELSE 0 END) + 1 AS v FROM t")
    withClue(notNull) {
      notNull should not include ": 0 + 1"
      notNull should include("? 1 : 0) + 1")
    }
    Seq(
      "SELECT CASE WHEN n > 0 THEN n ELSE 0 END + 1 AS v FROM t",
      "SELECT (CASE WHEN n > 0 THEN n ELSE 0 END) + 1 AS v FROM t",
      "SELECT CASE WHEN n > 0 THEN n END + 1 AS v FROM t"
    ).foreach { sql =>
      val script = fieldPainless(sql)
      withClue(s"[$sql] $script ") {
        script should not include ": 0 + 1"
        script should fullyMatch regex
        """.*def (lv\d+) = \(.*\); \(\1 == null\) \? null : \(\1 \+ 1\)"""
      }
    }
    val nullIf = fieldPainless("SELECT NULLIF(n, 3) + 1 AS v FROM t")
    withClue(nullIf) {
      nullIf should fullyMatch regex """.*\((param\d+) == null\) \? null : \(\1 \+ 1\)"""
    }
    val stored = ingestPainless(
      "CREATE TABLE z (id INT, n INT, c BIGINT SCRIPT AS (CASE WHEN n > 0 THEN n ELSE 0 END + 1), " +
      "c2 INT SCRIPT AS (NULLIF(n, 3) + 1), PRIMARY KEY (id))"
    )
    withClue(stored) {
      stored should not include ": 0 + 1"
      stored should include("== null) ? null : (")
    }
  }

  /** `GREATEST` / `LEAST` over whole numbers keep the winner's whole type (issue #380): Painless's
    * `Math.max` / `Math.min` take `double`s only.
    */
  "GREATEST / LEAST over whole numbers" should "choose the winner by a comparison" in {
    val greatest = fieldPainless("SELECT GREATEST(n, 2) AS v FROM t")
    withClue(greatest) {
      greatest should not include "Math.max"
      greatest should include(">= 2 ?")
    }
    val least = fieldPainless("SELECT LEAST(n, big) AS v FROM t")
    withClue(least) {
      least should not include "Math.min"
      least should include(" <= ")
    }
    fieldPainless("SELECT GREATEST(x, 2) AS v FROM t") should include("Math.max")
    // two literals: the winner is the literal itself, converted to the BIGINT both are -- never a
    // conditional over constants, which Elasticsearch 6.8 refuses as a whole script
    val literals = fieldPainless("SELECT GREATEST(2, 4) AS v FROM t")
    literals should endWith("((long) 4)")
    literals should not include "?"
  }

  /** `GREATEST` / `LEAST` answer the common type of ALL their arguments on every row, as PostgreSQL
    * types them, and report it: each argument is read by the type it is declared with -- a `CAST`
    * by the type it casts to, never by its column's -- and per document the value is converted to
    * that type on every path, the argument that survives a NULL included.
    */
  "GREATEST / LEAST" should "answer and report the common type of their arguments" in {
    reported(
      "SELECT GREATEST(CAST(n AS DOUBLE), 1) AS a, GREATEST(n, x) AS b, GREATEST(n, 2) AS c, " +
      "GREATEST(n, CAST(n AS BIGINT)) AS d, LEAST(n, r) AS e, GREATEST(ABS(x), 1) AS f FROM t"
    ) shouldBe Map(
      "a" -> "DOUBLE",
      "b" -> "DOUBLE",
      "c" -> "BIGINT",
      "d" -> "BIGINT",
      "e" -> "REAL",
      "f" -> "DOUBLE"
    )
    // a CAST to DOUBLE is a DOUBLE: compared by Math.max, never as a whole number
    val cast = fieldPainless("SELECT GREATEST(CAST(n AS DOUBLE), 1) AS v FROM t")
    withClue(cast) {
      cast should include("Math.max(")
      cast should not include ">= 1 ?"
      cast should include("((double) (")
    }
    // the argument that survives a NULL is converted with the winner: the value is read once
    val survivor = fieldPainless("SELECT GREATEST(n, x) AS v FROM t")
    withClue(survivor) {
      survivor should fullyMatch regex
      """.*def (lv\d+) = \(.*\); \(\1 == null \? \1 : \(def\) \(\(double\) \1\)\)"""
    }
    // a whole literal past the range of an int is a long literal beside a BIGINT
    fieldPainless("SELECT GREATEST(n, 3000000000) AS v FROM t") should include(
      ">= 3000000000L ? param1 : 3000000000L)"
    )
  }

  /** `ABS` is typed by its argument, as PostgreSQL's `abs(integer) -> integer`: a quoted fraction
    * beside `ABS` of an integer is refused by name, a whole number read; its value is that integer.
    */
  "ABS" should "be typed by its argument" in {
    reported("SELECT ABS(n) AS a, ABS(big) AS b, ABS(x) AS c FROM t") shouldBe Map(
      "a" -> "INT",
      "b" -> "BIGINT",
      "c" -> "DOUBLE"
    )
    forAll(
      Table(
        ("sql", "refusal"),
        ("SELECT id FROM t WHERE ABS(n) = '1.5'", "'INT' is not compatible with 'VARCHAR'"),
        ("SELECT id FROM t WHERE ABS(n) = '1.0'", "'INT' is not compatible with 'VARCHAR'"),
        ("SELECT id FROM t WHERE ABS(big) = '1.5'", "'BIGINT' is not compatible with 'VARCHAR'"),
        ("SELECT id FROM t WHERE ABS(n) IN ('1', '1.5')", "'INT' is not compatible with 'VARCHAR'"),
        (
          "SELECT id FROM t WHERE ABS(n) BETWEEN '1' AND '1.5'",
          "output 'INT' is not compatible with input 'VARCHAR'"
        ),
        ("SELECT ABS(n) + '1.5' AS v FROM t", "is not compatible with input 'VARCHAR'"),
        ("SELECT id FROM t WHERE ABS(n) + '1.5' > 0", "ABS(n) + '1.5'"),
        (
          "SELECT NULLIF(ABS(n), '1.5') AS v FROM t",
          "with VARCHAR: NULLIF requires two arguments of comparable types"
        ),
        (
          "SELECT CASE WHEN ABS(n) = '1.5' THEN 1 ELSE 0 END AS v FROM t",
          "'INT' is not compatible with 'VARCHAR'"
        )
      )
    ) { (sql, refusal) =>
      withClue(s"[$sql] ") {
        parsed(sql)
        resolved(sql).swap.getOrElse(fail("accepted once the types are known")) should include(
          refusal
        )
      }
    }
    forAll(
      Table(
        "sql",
        "SELECT id FROM t WHERE ABS(n) = '2'",
        "SELECT id FROM t WHERE ABS(x) = '1.5'",
        "SELECT id FROM t WHERE ABS(n) > 2"
      )
    ) { sql => withClue(s"[$sql] ")(resolved(sql) shouldBe Right(())) }
    fieldPainless("SELECT ABS(n) AS v FROM t") should not include "Math.abs"
    fieldPainless("SELECT ABS(x) AS v FROM t") should include("Double.valueOf(Math.abs(")
  }

  /** `NULLIF(<quoted literal>, x)` is typed by the literal's READING beside `x` -- what it answers
    * and what PostgreSQL types it -- never by the VARCHAR the literal is written as.
    */
  "NULLIF with a quoted literal first" should "be typed by the literal's reading" in {
    reported(
      "SELECT NULLIF('2024-01-31', ts) AS a, NULLIF('2024-01-31T10:00:00Z', d) AS b, " +
      "NULLIF('3', n) AS c, NULLIF('1.5', x) AS e, NULLIF('true', b) AS f, " +
      "NULLIF(ts, '2024-01-31') AS h FROM t"
    ) shouldBe Map(
      "a" -> "TIMESTAMP",
      "b" -> "DATE",
      "c" -> "BIGINT",
      "e" -> "DOUBLE",
      "f" -> "BOOLEAN",
      "h" -> "TIMESTAMP"
    )
  }

  /** A simple `CASE` answers its `THEN` / `ELSE` values in their own type: the operand is only
    * compared, in the type it is compared in, and never converts the results.
    */
  "a simple CASE" should "answer its THEN / ELSE values in their own type" in {
    reported(
      "SELECT CASE s WHEN 'a' THEN 1 ELSE 0 END AS a, CASE x WHEN 1.5 THEN 1 ELSE 0 END AS b FROM t"
    ) shouldBe Map("a" -> "BIGINT", "b" -> "BIGINT")
    val overText = fieldPainless("SELECT CASE s WHEN 'a' THEN 1 ELSE 0 END AS v FROM t")
    withClue(overText) {
      overText should include(".compareTo(")
      overText should endWith("? 1 : 0")
    }
    val overDouble = fieldPainless("SELECT CASE x WHEN 1.5 THEN 1 ELSE 0 END AS v FROM t")
    withClue(overDouble)(overDouble should endWith("? 1 : 0"))
    val stored = ingestPainless(
      "CREATE TABLE z (id INT, s KEYWORD, c BIGINT SCRIPT AS (CASE s WHEN '1' THEN 1 ELSE 0 END), " +
      "PRIMARY KEY (id))"
    )
    withClue(stored)(stored should include("? 1 : 0"))
  }
}
