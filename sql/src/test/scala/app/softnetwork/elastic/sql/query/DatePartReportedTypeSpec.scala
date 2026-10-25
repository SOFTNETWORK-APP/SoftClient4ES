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

import app.softnetwork.elastic.sql.{Identifier, PainlessContext, PainlessContextType}
import app.softnetwork.elastic.sql.function.time.Extract
import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.schema.{Column, Table => SchemaTable}
import app.softnetwork.elastic.sql.`type`.{SQLType, SQLTypeUtils, SQLTypes}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** A date part REPORTS a BIGINT, the whole number it produces.
  *
  * 🔴 `reportedType` is what a consumer that DECLARES a column reads: `Table.mergeWithSearch`
  * writes it into a CTAS target's `_meta.columns.<c>.data_type` and, through
  * `SQLTypeUtils.elasticType`, into its mapping; a materialized view declares its columns, its
  * GROUP BY keys included, from it. A date part reported its `outputType`, NUMERIC, which a CTAS
  * maps to a `scaled_float` without a `scaling_factor`: `CREATE TABLE t2 AS SELECT YEAR(d) AS y
  * FROM t` failed at index creation.
  *
  * The oracle is DuckDB 1.5.5, which answers BIGINT for every member over a DATE and over a
  * TIMESTAMP (`typeof(year(d))`, …, `typeof(extract(hour FROM ts))`, `typeof(extract(timezone FROM
  * …))`), for MIN / MAX over one and for ABS over one, and INTEGER for `CAST(year(d) AS INT)`. Two
  * members have no DuckDB counterpart and rest on being a whole number: NANOSECOND (DuckDB has no
  * such part) and EPOCHDAY (days since 1970-01-01, a long; DuckDB's `epoch` counts seconds, a
  * different quantity).
  *
  * `outputType` stays NUMERIC: it is what `coerce` and the date part's own Painless read. GREATEST
  * / LEAST, which take the common type of what their arguments report, are the one Painless that
  * follows: they cast to BIGINT beside an INT and to REAL beside a REAL (pinned below).
  */
class DatePartReportedTypeSpec extends AnyFlatSpec with Matchers {

  private val schema: SchemaTable = SchemaTable(
    "t",
    columns = List(
      Column("d", SQLTypes.Date),
      Column("ts", SQLTypes.Timestamp),
      Column("g", SQLTypes.Keyword),
      Column("n", SQLTypes.Int),
      Column("l", SQLTypes.BigInt),
      Column("r", SQLTypes.Real),
      Column("x", SQLTypes.Double)
    )
  )

  /** Every member of the date-part family, in each spelling the parser accepts for it. */
  private val members: List[String] = List(
    "YEAR(%s)",
    "MONTH(%s)",
    "DAY(%s)",
    "DAYOFMONTH(%s)",
    "QUARTER(%s)",
    "WEEK(%s)",
    "WEEKOFYEAR(%s)",
    "WEEKDAY(%s)",
    "DAYOFWEEK(%s)",
    "YEARDAY(%s)",
    "DAYOFYEAR(%s)",
    "HOUR(%s)",
    "MINUTE(%s)",
    "SECOND(%s)",
    "MILLISECOND(%s)",
    "MICROSECOND(%s)",
    "NANOSECOND(%s)",
    "EPOCHDAY(%s)",
    "OFFSET_SECONDS(%s)",
    "EXTRACT(YEAR FROM %s)",
    "EXTRACT(MONTH FROM %s)",
    "EXTRACT(HOUR FROM %s)"
  )

  /** The date and the timestamp column every member is asked over. */
  private val temporals: List[String] = List("d", "ts")

  /** The statement, and the identifier of its column `a`, resolved against the schema. */
  private def resolved(sql: String): (SingleSearch, Identifier) =
    Parser(sql) match {
      case Right(search: SingleSearch) =>
        val field = search
          .update(Some(schema))
          .select
          .fields
          .find(_.fieldAlias.map(_.alias).contains("a"))
          .getOrElse(fail(s"no column `a` in [$sql]"))
        (search, field.identifier)
      case other => fail(s"[$sql] is not one SELECT: $other")
    }

  /** The column `a` of `CREATE TABLE … AS <search>`. */
  private def ctas(search: SingleSearch): Column =
    schema
      .mergeWithSearch(search)
      .columns
      .find(_.name == "a")
      .getOrElse(fail(s"no CTAS column `a` for [${search.sql}]"))

  /** `a` is reported, declared by a CTAS and mapped as `expected`. */
  private def assertDeclared(sql: String, expected: SQLType, es: String): Unit = {
    val (search, id) = resolved(sql)
    withClue(s"[$sql] ") {
      id.reportedType shouldBe expected
      val column = ctas(search)
      column.dataType shouldBe expected
      SQLTypeUtils.elasticType(column.dataType) shouldBe es
    }
  }

  behavior of "a date part's reported type"

  for (member <- members) {
    it should s"be BIGINT for ${member.format("<d | ts>")}, mapped long, its outputType unchanged" in {
      for (c <- temporals) {
        val sql = s"SELECT ${member.format(c)} AS a FROM t"
        assertDeclared(sql, SQLTypes.BigInt, "long")
        val (_, id) = resolved(sql)
        withClue(s"[$sql] ") {
          id.functions.headOption.collect { case e: Extract => e.outputType } shouldBe Some(
            SQLTypes.Numeric
          )
        }
      }
    }
  }

  it should "be BIGINT for a date part that is a GROUP BY key" in {
    for (c <- temporals)
      assertDeclared(
        s"SELECT YEAR($c) AS a, COUNT(*) AS n FROM t GROUP BY YEAR($c)",
        SQLTypes.BigInt,
        "long"
      )
  }

  // MIN / MAX return one of the values they aggregate, so they report what the date part reports
  // (`AggregateFunction.outputTypeOf`). DuckDB: BIGINT.
  it should "be BIGINT for MIN / MAX over YEAR and MONTH" in {
    for (c <- temporals; aggregate <- List("MIN", "MAX"); part <- List("YEAR", "MONTH"))
      assertDeclared(s"SELECT $aggregate($part($c)) AS a FROM t", SQLTypes.BigInt, "long")
  }

  // ABS is typed by its argument, which is now a BIGINT. DuckDB: BIGINT.
  it should "be BIGINT for ABS over a date part" in {
    for (c <- temporals)
      assertDeclared(s"SELECT ABS(YEAR($c)) AS a FROM t", SQLTypes.BigInt, "long")
  }

  /** The Painless of the column `a`, as a query's script field renders it. */
  private def painlessOf(sql: String): String = {
    val (search, _) = resolved(sql)
    val fields = search.update(Some(schema)).scriptFields.filterNot(_.isAggregation)
    withClue(s"[$sql] ") { fields.size shouldBe 1 }
    fields.head.painless(Some(PainlessContext(context = PainlessContextType.Query)))
  }

  // 🔴 GREATEST / LEAST take the common type of what their arguments REPORT, and render the cast
  // of that type: the one Painless that follows a date part's type. A date part was NUMERIC, so
  // they were DOUBLE and rendered `(double)`. DuckDB 1.5.5: `typeof(greatest(n, year(d)))` and
  // `typeof(greatest(l, year(d)))` are BIGINT, `typeof(greatest(r, year(d)))` FLOAT,
  // `typeof(greatest(x, year(d)))` DOUBLE, `typeof(greatest(year(d), 2020))` BIGINT.
  it should "make GREATEST / LEAST of a date part and a column the common type, and cast to it" in {
    for (
      (other, expected, es, cast) <- List(
        ("n", SQLTypes.BigInt, "long", "(def) ((long) lv1)"),
        ("l", SQLTypes.BigInt, "long", "(def) ((long) lv1)"),
        ("r", SQLTypes.Real, "float", "(def) ((float) lv1)"),
        ("x", SQLTypes.Double, "double", "(def) ((double) lv1)")
      );
      fn <- List("GREATEST", "LEAST");
      sql <- List(
        s"SELECT $fn($other, YEAR(d)) AS a FROM t",
        s"SELECT $fn(YEAR(d), $other) AS a FROM t"
      )
    ) {
      assertDeclared(sql, expected, es)
      withClue(s"[$sql] ") { painlessOf(sql) should include(cast) }
    }
  }

  it should "make GREATEST / LEAST of a date part and a literal BIGINT, compared as whole numbers" in {
    for ((fn, op) <- List("GREATEST" -> ">=", "LEAST" -> "<=")) {
      val sql = s"SELECT $fn(YEAR(d), 2020) AS a FROM t"
      assertDeclared(sql, SQLTypes.BigInt, "long")
      val painless = painlessOf(sql)
      withClue(s"[$sql] ") {
        painless should startWith("((long) (")
        painless should include(s"(param1 $op 2020 ? param1 : 2020)")
      }
    }
  }

  // The control: a cast decides the type itself, before and after. DuckDB: INTEGER.
  it should "leave CAST(YEAR(d) AS INT) an INT" in {
    for (c <- temporals)
      assertDeclared(s"SELECT CAST(YEAR($c) AS INT) AS a FROM t", SQLTypes.Int, "integer")
  }
}
