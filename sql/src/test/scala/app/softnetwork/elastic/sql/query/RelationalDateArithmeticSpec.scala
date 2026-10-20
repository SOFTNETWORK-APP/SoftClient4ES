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

import app.softnetwork.elastic.sql.operator.math.ArithmeticExpression
import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.schema.{Column, Table => Schema}
import app.softnetwork.elastic.sql.`type`.SQLTypes
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** Date arithmetic as the relational engine (DuckDB, which runs a JOIN) is handed it: core's rules
  * -- the days between two dates, the fractional days between two instants, a date moved by days
  * -- in the engine's SQL, inside `ArithmeticExpression.renderingRelational`, and core's refusals
  * by name (`SingleSearch.dateArithmeticErrors`).
  *
  * The expectation is written HERE, from the rules, and never read from the implementation:
  *
  * | Expression                                   | Relational SQL                                                |
  * |:---------------------------------------------|:--------------------------------------------------------------|
  * | DATE - DATE                                  | `(CAST(l AS DATE) - CAST(r AS DATE))`                         |
  * | an instant on one side at least              | `((epoch_ms(l) - epoch_ms(r)) / 86400000.0)`                  |
  * | DATE +/- a whole number n (either side of +) | `(CAST(t AS DATE) +/- CAST(n AS INTEGER))`                    |
  * | any other temporal +/- n                     | `epoch_ms(CAST(epoch_ms(t) +/- <ms> AS BIGINT))`              |
  * | ... <ms>, n whole                            | `CAST(n AS BIGINT) * 86400000`                                |
  * | ... <ms>, n fractional (Java's half up)      | `CAST(floor(CAST(n AS DOUBLE) * 86400000.0 + 0.5) AS BIGINT)` |
  * | a literal spelling a date beside a DATE      | `CAST('<its UTC day>' AS DATE)`                               |
  * | a literal spelling an instant beside one     | its epoch milliseconds                                        |
  * | a literal spelling a number                  | that number of days                                           |
  * | a NULL operand, anything without a temporal  | as written                                                    |
  *
  * Outside the scope every rendering is the one core has always produced.
  */
class RelationalDateArithmeticSpec extends AnyFlatSpec with Matchers {

  private val ev: Schema = Schema(
    name = "ev",
    columns = List(
      Column("k", SQLTypes.Keyword),
      Column("d", SQLTypes.Date),
      Column("ts", SQLTypes.Timestamp),
      Column("n", SQLTypes.Int),
      Column("x", SQLTypes.Double),
      Column("r", SQLTypes.Real)
    )
  )

  private val ev2: Schema = Schema(
    name = "ev2",
    columns = List(
      Column("k", SQLTypes.Keyword),
      Column("d2", SQLTypes.Date),
      Column("ts2", SQLTypes.Timestamp)
    )
  )

  private val schemas: Map[String, Schema] = Map("ev" -> ev, "ev2" -> ev2)

  private val join = "FROM ev a JOIN ev2 b ON a.k = b.k"

  private def parsed(sql: String): SingleSearch =
    Parser(sql) match {
      case Right(s: SingleSearch) => s
      case other                  => fail(s"[$sql] rejected: $other")
    }

  private def attached(sql: String): SingleSearch = parsed(sql).updateAll(schemas)

  /** The fragments the relational engine is handed: the SELECT expressions and the clauses. */
  private def fragments(ss: SingleSearch): Seq[String] =
    ss.select.fields.map(_.identifier.sql) ++
    ss.where.map(_.sql).toSeq ++ ss.groupBy.map(_.sql).toSeq ++
    ss.having.map(_.sql).toSeq ++ ss.orderBy.map(_.sql).toSeq

  private def selected(ss: SingleSearch): String = ss.select.fields.last.identifier.sql

  private val Days = "86400000.0"

  private def between(l: String, r: String): String = s"(CAST($l AS DATE) - CAST($r AS DATE))"
  private def fractional(l: String, r: String): String = s"((epoch_ms($l) - epoch_ms($r)) / $Days)"
  private def dateShift(t: String, op: String, n: String): String =
    s"(CAST($t AS DATE) $op CAST($n AS INTEGER))"
  private def wholeShift(t: String, op: String, n: String): String =
    s"epoch_ms(CAST(epoch_ms($t) $op CAST($n AS BIGINT) * 86400000 AS BIGINT))"
  private def fractionalShift(t: String, op: String, n: String): String =
    s"epoch_ms(CAST(epoch_ms($t) $op CAST(floor(CAST($n AS DOUBLE) * $Days + 0.5) AS BIGINT) AS BIGINT))"

  /** (the SELECT expression, the relational SQL the rules give it). */
  private val forms: Seq[(String, String)] = Seq(
    // DATE - DATE
    "a.d - b.d2" -> between("a.d", "b.d2"),
    // an instant on one side at least
    "a.ts - b.ts2"             -> fractional("a.ts", "b.ts2"),
    "a.ts - b.d2"              -> fractional("a.ts", "b.d2"),
    "a.d - b.ts2"              -> fractional("a.d", "b.ts2"),
    "CURRENT_TIMESTAMP - a.ts" -> fractional("CURRENT_TIMESTAMP", "a.ts"),
    // a DATE moved by a whole number: INT, a BIGINT literal, on either side of +
    "a.d + a.n" -> dateShift("a.d", "+", "a.n"),
    "a.d - a.n" -> dateShift("a.d", "-", "a.n"),
    "a.n + a.d" -> dateShift("a.d", "+", "a.n"),
    "a.d + 2"   -> dateShift("a.d", "+", "2"),
    "a.d - 2"   -> dateShift("a.d", "-", "2"),
    // a DATE moved by a fractional number: a time of day
    "a.d + a.x" -> fractionalShift("a.d", "+", "a.x"),
    "a.d - 1.5" -> fractionalShift("a.d", "-", "1.5"),
    // an instant moved by a whole or a fractional number, on either side of +
    "a.ts + a.n"  -> wholeShift("a.ts", "+", "a.n"),
    "a.ts - 2"    -> wholeShift("a.ts", "-", "2"),
    "a.n + b.ts2" -> wholeShift("b.ts2", "+", "a.n"),
    "a.ts - 1.5"  -> fractionalShift("a.ts", "-", "1.5"),
    "a.x + b.ts2" -> fractionalShift("b.ts2", "+", "a.x"),
    // a numeric string is that number of days
    "a.d + '1'"    -> dateShift("a.d", "+", "1"),
    "a.d - '1.5'"  -> fractionalShift("a.d", "-", "1.5"),
    "a.ts + '1.5'" -> fractionalShift("a.ts", "+", "1.5"),
    // a string spelling a temporal is that temporal, read as the other operand's type
    "a.d - '2024-01-31'"            -> "(CAST(a.d AS DATE) - CAST('2024-01-31' AS DATE))",
    "'2024-01-31' - a.d"            -> "(CAST('2024-01-31' AS DATE) - CAST(a.d AS DATE))",
    "a.ts - '2024-01-31T12:00:00Z'" -> s"((epoch_ms(a.ts) - 1706702400000) / $Days)",
    // a NULL operand: NULL, as written
    "a.d + NULL"  -> "a.d + NULL",
    "a.ts - NULL" -> "a.ts - NULL",
    "NULL - a.d"  -> "NULL - a.d",
    // nesting: every operand renders in the scope, at any depth
    "(a.d + 1) - b.d2"    -> between(dateShift("a.d", "+", "1"), "b.d2"),
    "(a.ts - b.ts2) * 24" -> s"${fractional("a.ts", "b.ts2")} * 24",
    "a.x + 1 + b.ts2"     -> fractionalShift("b.ts2", "+", "a.x + 1"),
    // a REAL is multiplied as a DOUBLE, as Painless does
    "a.d + a.r" -> fractionalShift("a.d", "+", "a.r"),
    // no temporal: as written, in the scope too
    "a.n + a.x" -> "a.n + a.x"
  )

  /** Statements whose clauses carry date arithmetic, and what each clause renders in the scope. */
  private val clauses: Seq[(String, Seq[String])] = Seq(
    s"SELECT a.k, MAX(a.ts) - MIN(b.ts2) AS r $join GROUP BY a.k" ->
    Seq("a.k", fractional("MAX(a.ts)", "MIN(b.ts2)"), " GROUP BY a.k"),
    s"SELECT a.k AS r $join WHERE a.ts - b.ts2 > 2 ORDER BY a.d - b.d2 DESC" ->
    Seq(
      "a.k",
      s" WHERE ${fractional("a.ts", "b.ts2")} > 2",
      s" ORDER BY ${between("a.d", "b.d2")} DESC"
    ),
    s"SELECT a.k, MAX(a.d) - MIN(b.d2) AS r $join GROUP BY a.k HAVING r > 1" ->
    Seq(
      "a.k",
      between("MAX(a.d)", "MIN(b.d2)"),
      " GROUP BY a.k",
      // core resolves the alias to the expression it names
      s" HAVING ${between("MAX(a.d)", "MIN(b.d2)")} > 1"
    )
  )

  "date arithmetic in a JOIN" should
  "render core's rules in the relational engine's SQL inside the scope" in {
    val mismatches = forms.flatMap { case (expression, expected) =>
      val ss = attached(s"SELECT $expression AS r $join")
      val rendered = ArithmeticExpression.renderingRelational(selected(ss))
      if (rendered == expected) None
      else Some(s"[$expression] rendered [$rendered], expected [$expected]")
    } ++ clauses.flatMap { case (sql, expected) =>
      val rendered = ArithmeticExpression.renderingRelational(fragments(attached(sql)))
      if (rendered == expected) None
      else Some(s"[$sql] rendered $rendered, expected $expected")
    }
    withClue(mismatches.mkString("\n", "\n", "\n")) {
      mismatches shouldBe empty
    }
  }

  private def refusal(operator: String, l: String, r: String, expression: String): String =
    s"Type mismatch: operator $operator cannot be applied to $l and $r in expression: " +
    s"$expression; subtract two dates for the days between them, or add or subtract a number of days"

  "dateArithmeticErrors" should "be core's date-arithmetic refusals by name, and no other rule" in {
    val refused = Seq(
      s"SELECT a.n - a.d AS r $join"  -> refusal("-", "INT", "DATE", "a.n - a.d"),
      s"SELECT a.d + b.d2 AS r $join" -> refusal("+", "DATE", "DATE", "a.d + b.d2"),
      s"SELECT a.d * 2 AS r $join"    -> refusal("*", "DATE", "BIGINT", "a.d * 2"),
      s"SELECT a.k AS r $join WHERE a.ts * 2 > 1" -> refusal(
        "*",
        "TIMESTAMP",
        "BIGINT",
        "a.ts * 2"
      ),
      // inside a CASE condition, as core's type rules walk it
      s"SELECT CASE WHEN a.d * 2 > 1 THEN 1 ELSE 0 END AS r $join" ->
      refusal("*", "DATE", "BIGINT", "a.d * 2")
    )
    refused.foreach { case (sql, message) =>
      val ss = attached(sql)
      withClue(s"[$sql] ") {
        ss.dateArithmeticErrors shouldBe Seq(message)
        // the same message core itself refuses the statement with
        ss.validateResolved() shouldBe Left(message)
      }
    }

    // a refusal first classified inside the scope, where its operand renders relationally, names
    // the expression in core's syntax
    val nested = attached(s"SELECT (a.d + 1) * 2 AS r $join")
    ArithmeticExpression.renderingRelational(selected(nested)) shouldBe
    s"${dateShift("a.d", "+", "1")} * 2"
    nested.dateArithmeticErrors shouldBe Seq(refusal("*", "DATE", "BIGINT", "(a.d + 1) * 2"))

    // no date arithmetic: nothing, even where another type rule refuses the statement
    attached(s"SELECT a.n + a.x AS r $join").dateArithmeticErrors shouldBe empty
    val other = attached(s"SELECT a.n + a.k AS r $join")
    other.dateArithmeticErrors shouldBe empty
    other.validateResolved().isLeft shouldBe true
  }

  "outside the scope, every rendering" should "be core's, before and after the scope" in {
    val statements =
      forms.map { case (expression, _) => s"SELECT $expression AS r $join" } ++ clauses.map(_._1)
    val drifts = statements.flatMap { sql =>
      val unattached = parsed(sql)
      val ss = attached(sql)
      val before = (ss.sql, fragments(ss))
      val inside = ArithmeticExpression.renderingRelational((ss.sql, fragments(ss)))
      val after = (ss.sql, fragments(ss))
      Seq(
        if (after == before) None else Some(s"[$sql] after the scope: $after, before: $before"),
        // a statement renders itself in core's syntax, in the scope too
        if (inside._1 == before._1) None else Some(s"[$sql] statement in the scope: ${inside._1}"),
        if (before == (unattached.sql, fragments(unattached))) None
        else Some(s"[$sql] attached $before, unattached ${(unattached.sql, fragments(unattached))}")
      ).flatten
    }
    withClue(drifts.mkString("\n", "\n", "\n")) {
      drifts shouldBe empty
    }

    // core's syntax, as written
    selected(attached(s"SELECT a.ts - b.ts2 AS r $join")) shouldBe "a.ts - b.ts2"
    selected(attached(s"SELECT (a.d + 1) - b.d2 AS r $join")) shouldBe "(a.d + 1) - b.d2"
    selected(attached(s"SELECT a.d - '2024-01-31' AS r $join")) shouldBe "a.d - '2024-01-31'"

    // a subquery's body is the statement core runs, even when it is first rendered in the scope
    val sql = s"SELECT a.k AS r $join WHERE a.k IN (SELECT k FROM ev2 WHERE d2 > CURRENT_DATE - 7)"
    val ss = attached(sql)
    val where = ArithmeticExpression.renderingRelational(ss.where.map(_.sql).getOrElse(""))
    where should include("CURRENT_DATE - 7")
    ss.where.map(_.sql).getOrElse("") shouldBe where
    ss.sql shouldBe parsed(sql).sql
  }
}
