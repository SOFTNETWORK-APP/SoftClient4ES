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

package app.softnetwork.elastic.sql.perf

import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query.{Criteria, SingleSearch}
import app.softnetwork.elastic.sql.schema.{Column, Table}
import app.softnetwork.elastic.sql.`type`.SQLTypes
import app.softnetwork.elastic.sql.{PainlessContext, PainlessContextType}

/** Per-statement cost of TRANSLATING a predicate to Painless — the companion of [[ParseCostProbe]],
  * which measures parsing only.
  *
  * Parsing is one half of what a request pays before Elasticsearch sees it; the other half is
  * schema resolution (`update`) and script EMISSION, and that is the half a change to
  * `Expression.painless` moves. This probe times three things per statement:
  *
  *   - `parse` — `Parser(sql)`, the same work `ParseCostProbe` reports;
  *   - `parse+emit` — a FRESH parse, then `update(schema)` + `criteria.painless(context)`;
  *   - `emit (Δ)` — the difference, i.e. what translation costs on top of parsing.
  *
  * 🔴 The parse is re-done inside the second measurement on purpose, and the emission cost is
  * reported as a DIFFERENCE rather than timed alone: the AST carries mutable rendering state
  * (`PainlessParam.painlessMethods`), so translating the SAME tree twice is not the same work twice
  * and does not model a request. One parse per translation is what production does.
  *
  * Same protocol as `ParseCostProbe`: a global warm-up so the JIT has compiled the hot path and
  * every `object` is initialised, per-statement discarded warm-up, then `runs` timed iterations,
  * reported as a median (and p95) in microseconds.
  *
  * {{{
  * sbt "sql/Test/runMain app.softnetwork.elastic.sql.perf.EmissionCostProbe"
  * sbt "sql/Test/runMain app.softnetwork.elastic.sql.perf.EmissionCostProbe 200 600 out.tsv"
  * }}}
  *
  * ==This probe NEVER asserts==
  * It prints and exits, for the reason issues #269/#270 recorded: a timing assertion in the suite
  * is a CI-flake liability. Compare two trees by running it in each, INTERLEAVED, in one session —
  * an absolute number from another session is noise.
  */
object EmissionCostProbe {

  val DefaultWarmup: Int = 200
  val DefaultRuns: Int = 600

  private val schema: Table = Table(
    "t",
    columns = List(
      Column("id", SQLTypes.Keyword),
      Column("d", SQLTypes.Date),
      Column("ts", SQLTypes.Timestamp),
      Column("name", SQLTypes.Keyword),
      Column("other", SQLTypes.Keyword),
      Column("num", SQLTypes.Keyword),
      Column("amount", SQLTypes.Double),
      Column("n", SQLTypes.Int)
    )
  )

  /** Every shape the predicate path can take, so a change that helps one and hurts another is
    * visible rather than averaged away: plain columns (which must not move at all), functions that
    * FOLD into their parameter, functions that render as an EXPRESSION, the conversion family,
    * `IN`, a function on both sides, and a composite.
    */
  private val Statements: List[(String, String)] = List(
    "plain column, numeric"  -> "SELECT id FROM t WHERE n > 1",
    "plain column, keyword"  -> "SELECT id FROM t WHERE name = 'x'",
    "plain column, temporal" -> "SELECT id FROM t WHERE d = CAST('2025-01-31' AS DATE)",
    "folded function (YEAR)" -> "SELECT id FROM t WHERE YEAR(d) = 2025",
    "folded function (DATE_TRUNC)" ->
    "SELECT id FROM t WHERE DATE_TRUNC(d, MONTH) = CAST('2025-01-01' AS DATE)",
    "expression function (WEEKDAY)"     -> "SELECT id FROM t WHERE WEEKDAY(d) = 0",
    "expression function (DATE_FORMAT)" -> "SELECT id FROM t WHERE DATE_FORMAT(d, 'yyyy') = '2025'",
    "expression function (LAST_DAY)" ->
    "SELECT id FROM t WHERE LAST_DAY(d) = CAST('2025-01-31' AS DATE)",
    "conversion (CAST)"       -> "SELECT id FROM t WHERE CAST(num AS INT) = 9",
    "conversion (TRY_CAST)"   -> "SELECT id FROM t WHERE TRY_CAST(num AS INT) = 9",
    "conditional (ISNULL)"    -> "SELECT id FROM t WHERE ISNULL(name) = true",
    "string function (UPPER)" -> "SELECT id FROM t WHERE UPPER(name) = 'A'",
    "math function (ABS)"     -> "SELECT id FROM t WHERE ABS(amount) > 10",
    "IN, numeric"             -> "SELECT id FROM t WHERE CAST(num AS BIGINT) IN (9, 12)",
    "IN, strings"             -> "SELECT id FROM t WHERE UPPER(name) IN ('A', 'B')",
    "both sides"              -> "SELECT id FROM t WHERE YEAR(d) = YEAR(ts)",
    "composite AND/OR" ->
    "SELECT id FROM t WHERE WEEKDAY(d) = 0 AND UPPER(name) = 'A' OR ABS(amount) > 10"
  )

  private def criteriaOf(sql: String): Option[Criteria] =
    Parser(sql) match {
      case Right(ss: SingleSearch) => ss.update(Some(schema)).where.flatMap(_.criteria)
      case _                       => None
    }

  /** A fresh parse, then the translation — one request's worth of work. */
  private def parseAndEmit(sql: String): Int =
    criteriaOf(sql) match {
      case Some(c) =>
        val ctx = PainlessContext(context = PainlessContextType.Query)
        val body = c.painless(Some(ctx))
        (ctx.toString + body).length
      case None => 0
    }

  private def median(sorted: Array[Long]): Long = {
    val n = sorted.length
    if (n % 2 == 1) sorted(n / 2) else (sorted(n / 2 - 1) + sorted(n / 2)) / 2
  }

  private def micros(nanos: Long): String =
    String.format(java.util.Locale.ROOT, "%8.1f", java.lang.Double.valueOf(nanos / 1000.0))

  private def timed(runs: Int)(body: => Int): (Long, Long) = {
    val samples = new Array[Long](runs)
    var sink = 0
    var i = 0
    while (i < runs) {
      val t0 = System.nanoTime()
      sink ^= body
      val t1 = System.nanoTime()
      samples(i) = t1 - t0
      i += 1
    }
    if (sink == Int.MinValue) println("") // keep the sink observable
    java.util.Arrays.sort(samples)
    (median(samples), samples(math.min(runs - 1, (runs.toLong * 95L / 100L).toInt)))
  }

  def main(args: Array[String]): Unit = {
    val warmup = if (args.length > 0) args(0).toInt else DefaultWarmup
    val runs = if (args.length > 1) args(1).toInt else DefaultRuns
    val out = if (args.length > 2) Some(args(2)) else None

    var sink = 0
    for (_ <- 1 to warmup; (_, sql) <- Statements) {
      sink ^= Parser(sql).hashCode()
      sink ^= parseAndEmit(sql)
    }

    val rows = Statements.map { case (label, sql) =>
      for (_ <- 1 to warmup) { sink ^= Parser(sql).hashCode(); sink ^= parseAndEmit(sql) }
      val (parseMed, parseP95) = timed(runs)(Parser(sql).hashCode())
      val (bothMed, bothP95) = timed(runs)(parseAndEmit(sql))
      (label, parseMed, parseP95, bothMed - parseMed, bothP95 - parseP95, bothMed, bothP95)
    }

    println(
      f"${"statement"}%-36s ${"parse µs"}%9s ${"emit Δ µs"}%10s ${"parse+emit"}%11s   ${"p95 parse+emit"}%14s"
    )
    rows.foreach { case (label, pm, _, em, _, bm, bp) =>
      println(f"$label%-36s ${micros(pm)} ${micros(em)}  ${micros(bm)}    ${micros(bp)}")
    }
    val totalParse = rows.map(_._2).sum
    val totalEmit = rows.map(_._4).sum
    val totalBoth = rows.map(_._6).sum
    println(
      f"${"SUM of medians"}%-36s ${micros(totalParse)} ${micros(totalEmit)}  ${micros(totalBoth)}"
    )

    out.foreach { path =>
      val w = new java.io.PrintWriter(path)
      try {
        w.println("statement\tparse_ns\tparse_p95\temit_ns\temit_p95\ttotal_ns\ttotal_p95")
        rows.foreach { case (l, pm, pp, em, ep, bm, bp) =>
          w.println(s"$l\t$pm\t$pp\t$em\t$ep\t$bm\t$bp")
        }
      } finally w.close()
    }
    if (sink == Int.MinValue) println("")
  }
}
