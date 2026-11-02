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
        // over aggregates, in a HAVING (in a SELECT calculation it is refused, as it can be NULL)
        "SELECT g FROM t GROUP BY g HAVING GREATEST(MAX(d), MAX(d2)) > CAST('2024-02-01' AS DATE)"
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

  /** Every rendering of a statement: its WHERE, its group filter and its script fields. */
  private def renderedOf(sql: String): String = {
    val search = parsed(sql).update(Some(schema))
    val ctx = PainlessContext(context = PainlessContextType.Query)
    val where = search.where.flatMap(_.criteria).map(_.painless(Some(ctx))).getOrElse("")
    val having = search.having.flatMap(_.criteria).map(_.painless(None)).getOrElse("")
    val fields = search.scriptFields.filterNot(_.isAggregation).map(_.painless(Some(ctx)))
    (Seq(ctx.toString, where, having) ++ fields).mkString(" ")
  }

  /** A quoted number beside a whole-number operand is read as DuckDB 1.5.5 casts the string to that
    * type (the lead's ruling R-54 of 2026-11-02, which supersedes the PostgreSQL integer input of
    * 2026-10-05 for the whole types): the whole number it rounds to, half away from zero, at every
    * site that reads one -- T0-5: `'1.5'::INTEGER` = 2, `'1.0'` = 1, `'1e2'` = 100, `'4.5'` = 5
    * (half away from zero), `n > '1.5'` = `n > 2`, `IN ('1.5','3')` = `IN (2, 3)`, `5::INTEGER -
    * '0.4'`
    * = 5. Past BIGINT after rounding it is a value of no integer type and stays text, refused by
    * name as before. DuckDB's own refusals are not copied: its binder error on `+` / `*` (`n +
    * '1.5'` is `n + 2` here). Beside DOUBLE, REAL or DECIMAL (a DOUBLE here) any number is one.
    */
  "a quoted number beside a whole-number operand" should
  "be the whole number it rounds to, refused past BIGINT, and any number beside a fractional one" in {
    forAll(
      Table(
        ("site", "sql", "rendered"),
        ("comparison (INT)", "SELECT id FROM t WHERE NULLIF(n, 0) = '1.5'", "== 2L"),
        ("comparison (INT)", "SELECT id FROM t WHERE NULLIF(n, 0) = '1.0'", "== 1L"),
        ("comparison (INT)", "SELECT id FROM t WHERE NULLIF(n, 0) = '-2.5'", "== -3L"),
        ("comparison (INT)", "SELECT id FROM t WHERE NULLIF(n, 0) = '-0.5'", "== -1L"),
        ("comparison (INT)", "SELECT id FROM t WHERE NULLIF(n, 0) = '1.4'", "== 1L"),
        ("comparison (INT)", "SELECT id FROM t WHERE NULLIF(n, 0) = ' 2.5 '", "== 3L"),
        ("comparison (BIGINT)", "SELECT id FROM t WHERE COALESCE(big, 0) = '1.5'", "== 2L"),
        ("comparison (BIGINT)", "SELECT id FROM t WHERE COALESCE(big, 0) = '1.0'", "== 1L"),
        (
          "BIGINT max",
          "SELECT id FROM t WHERE COALESCE(big, 0) = '9223372036854775807.4'",
          "== 9223372036854775807L"
        ),
        (
          "CASE condition",
          "SELECT id, CASE WHEN COALESCE(n, 0) = '1.5' THEN 1 ELSE 0 END AS v FROM t",
          "== 2L"
        ),
        ("an exponent", "SELECT id FROM t WHERE NULLIF(n, 0) = '1e3'", "== 1000L"),
        ("an exponent", "SELECT id FROM t WHERE NULLIF(n, 0) = '1.45e1'", "== 15L"),
        ("IN", "SELECT id FROM t WHERE NULLIF(n, 0) IN ('1', '1.5')", "2L"),
        ("BETWEEN", "SELECT id FROM t WHERE COALESCE(big, 0) BETWEEN '1' AND '1.5'", "<= 2L"),
        ("NULLIF (INT)", "SELECT NULLIF(n, '1.5') AS v FROM t", "== 2L"),
        ("NULLIF (BIGINT)", "SELECT NULLIF(big, '1.0') AS v FROM t", "== 1L"),
        (
          "CASE <expr> WHEN (INT)",
          "SELECT CASE n WHEN '1.5' THEN 1 ELSE 0 END AS v FROM t",
          "= 2L"
        ),
        (
          "CASE <expr> WHEN (BIGINT)",
          "SELECT CASE big WHEN '1.0' THEN 1 ELSE 0 END AS v FROM t",
          "= 1L"
        ),
        // a group filter compares the metric with the number
        ("HAVING", "SELECT g FROM t GROUP BY g HAVING MAX(n) > '4.5'", "> 5L"),
        ("IN (HAVING)", "SELECT g FROM t GROUP BY g HAVING MAX(big) IN ('1', '1.0')", "1L"),
        ("arithmetic (INT)", "SELECT n + '1.5' AS v FROM t", "+ 2L"),
        ("arithmetic (INT, literal first)", "SELECT '1.0' * n AS v FROM t", "1L *"),
        ("arithmetic (BIGINT)", "SELECT big - '1.0' AS v FROM t", "- 1L"),
        ("arithmetic (INT)", "SELECT n - '0.4' AS v FROM t", "- 0L"),
        ("a literal on the left (A-42)", "SELECT id FROM t WHERE '1.5' < COALESCE(n, 0)", "2L <"),
        ("a date part", "SELECT id FROM t WHERE YEAR(d) = '2024.4'", "== 2024L")
      )
    ) { (site, sql, rendered) =>
      withClue(s"[$site] [$sql] ") {
        parsed(sql)
        resolved(sql) shouldBe Right(())
        renderedOf(sql) should include(rendered)
      }
    }
    // per group: the bucket script reads the metric, the type rule the whole number
    resolved("SELECT g, SUM(n) + '1.5' AS v FROM t GROUP BY g") shouldBe Right(())
    forAll(
      Table(
        ("site", "sql", "refusal"),
        (
          "past BIGINT",
          "SELECT id FROM t WHERE COALESCE(big, 0) = '99999999999999999999'",
          "'BIGINT' is not compatible with 'VARCHAR'"
        ),
        (
          "past BIGINT after rounding",
          "SELECT id FROM t WHERE COALESCE(big, 0) = '9223372036854775807.5'",
          "'BIGINT' is not compatible with 'VARCHAR'"
        ),
        (
          "an exponent past BIGINT",
          "SELECT id FROM t WHERE NULLIF(n, 0) = '1e999999999'",
          "'INT' is not compatible with 'VARCHAR'"
        ),
        ("no number", "SELECT id FROM t WHERE NULLIF(n, 0) = 'NaN'", "'INT' is not compatible"),
        ("no number", "SELECT n + 'abc' AS v FROM t", "is not compatible with input 'VARCHAR'")
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

  /** `ABS` is typed by its argument, as PostgreSQL's `abs(integer) -> integer` and DuckDB's: a
    * quoted fraction beside `ABS` of an integer is the whole number it rounds to (R-54), a text
    * that spells no number is refused by name; its value is that integer.
    */
  "ABS" should "be typed by its argument" in {
    reported("SELECT ABS(n) AS a, ABS(big) AS b, ABS(x) AS c FROM t") shouldBe Map(
      "a" -> "INT",
      "b" -> "BIGINT",
      "c" -> "DOUBLE"
    )
    forAll(
      Table(
        ("sql", "rendered"),
        ("SELECT id FROM t WHERE ABS(n) = '1.5'", "== 2L"),
        ("SELECT id FROM t WHERE ABS(n) = '1.0'", "== 1L"),
        ("SELECT id FROM t WHERE ABS(big) = '1.5'", "== 2L"),
        ("SELECT id FROM t WHERE ABS(n) IN ('1', '1.5')", "2L"),
        ("SELECT id FROM t WHERE ABS(n) BETWEEN '1' AND '1.5'", "2L"),
        ("SELECT NULLIF(ABS(n), '1.5') AS v FROM t", "== 2L"),
        ("SELECT CASE WHEN ABS(n) = '1.5' THEN 1 ELSE 0 END AS v FROM t", "== 2L")
      )
    ) { (sql, rendered) =>
      withClue(s"[$sql] ") {
        parsed(sql)
        resolved(sql) shouldBe Right(())
        renderedOf(sql) should include(rendered)
      }
    }
    forAll(
      Table(
        ("sql", "refusal"),
        ("SELECT id FROM t WHERE ABS(n) = 'abc'", "'INT' is not compatible with 'VARCHAR'"),
        ("SELECT ABS(n) + 'abc' AS v FROM t", "is not compatible with input 'VARCHAR'")
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

  /** The whole number a decimal text rounds to beside a whole type, as DuckDB 1.5.5 casts it (T0-5,
    * RUN 2026-11-02: half away from zero), BIGINT-bounded, and computed without a division by a
    * power of ten whatever the exponent (D16).
    */
  "ComparisonRule.roundedWhole" should "round half away from zero within BIGINT, in bounded time" in {
    import app.softnetwork.elastic.sql.ComparisonRule.roundedWhole
    forAll(
      Table(
        ("text", "rounded"),
        ("1.5", Some(2L)),
        ("2.5", Some(3L)),
        ("-1.5", Some(-2L)),
        ("-2.5", Some(-3L)),
        ("1.4", Some(1L)),
        (" 1.5 ", Some(2L)),
        ("1e2", Some(100L)),
        ("1.5e1", Some(15L)),
        ("1.45e1", Some(15L)),
        ("1.0", Some(1L)),
        ("0.5", Some(1L)),
        ("-0.5", Some(-1L)),
        (".5", Some(1L)),
        ("-0.4", Some(0L)),
        ("2.5e-1", Some(0L)),
        ("1e-400", Some(0L)),
        ("1e-999999999", Some(0L)),
        ("-1e-999999999", Some(0L)),
        ("9223372036854775807.4", Some(Long.MaxValue)),
        ("9223372036854775807.5", None),
        ("-9223372036854775808.4", Some(Long.MinValue)),
        ("-9223372036854775808.5", None),
        ("12345678901234567890123456789e-10", Some(1234567890123456789L)),
        ("1e999999999", None),
        ("NaN", None),
        ("Infinity", None),
        ("0x10", None),
        ("1_000", None),
        ("abc", None)
      )
    ) { (text, rounded) =>
      withClue(s"['$text'] ")(roundedWhole(text) shouldBe rounded)
    }
  }
}
