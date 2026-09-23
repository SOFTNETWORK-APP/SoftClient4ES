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

package app.softnetwork.elastic.client.perf

import app.softnetwork.elastic.client.{NopeClientApi, SearchApi}
import app.softnetwork.elastic.client.result._
import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query.{SingleSearch, TemporalLiterals}
import app.softnetwork.elastic.sql.schema.{Column, Schema, Table}
import app.softnetwork.elastic.sql.`type`.SQLTypes
import org.slf4j.{Logger, LoggerFactory}

/** Per-statement latency of the SEARCH RESOLUTION path -- the work `SearchApi.resolveWithSchema`
  * does between "the statement is parsed" and "Elasticsearch is called".
  *
  * ==Why this probe exists==
  * Parse cost and render cost have been measured on every story since the rule was written
  * (`feedback_measure_sql_parsing_cost_every_time`), and emission cost has `EmissionCostProbe`. The
  * resolution path had NOTHING, and it is where release 0.24.0 put most of its new work: the
  * relational-closure guard (22.1), uncorrelated WHERE-subquery execution (22.2), temporal-literal
  * normalisation against the mapping (BIDC-4 / #276), the schema ATTACH itself (#306), and the
  * post-resolution rules -- `Case.conditionsOf` (#384) and `NullIf.mismatchesOf` (#382). Every one
  * of those runs on EVERY search, and none of them was ever timed. Asked for by the lead on
  * 2026-09-23 while reviewing #382.
  *
  * ==What it reports==
  * For each statement, the median of:
  *   - `parse` -- `Parser.apply`, the yardstick the project already tracks, for scale;
  *   - `resolve` -- the whole `resolveWithSchema`, which is what a request actually pays;
  *   - `attach` -- `update(Some(schema))` alone, the full AST rebuild;
  *   - `temporal` -- `TemporalLiterals` alone;
  *   - `validate` -- `validateResolved()` alone;
  *   - `resolve x2` -- resolution run TWICE.
  *
  * 🔴 `resolve x2` is not a curiosity. `SearchApi.search` resolves, then routes a row query with no
  * LIMIT through `scrollRows` -> `ScrollApi.scroll`, which resolves AGAIN -- stated in
  * `resolveWithSchema`'s own comment and pinned by `SchemaAttachSpec`'s idempotence test. So for an
  * un-LIMITed row query, the number a request pays is the `resolve x2` column, not `resolve`.
  *
  * ==No assertion==
  * Deliberately a `main`, never a test with a ceiling: #269/#270 spent a round on a timing
  * assertion that flunked unrelated PRs on loaded CI runners. Quote it in a PR body; do not gate on
  * it. To attribute a change, run it in THIS tree and in a clone at the control commit, interleaved
  * (`feedback_attribute_only_against_a_control`) -- a remembered number is noise.
  *
  * {{{
  * sbt "core/Test/runMain app.softnetwork.elastic.client.perf.ResolveCostProbe"
  * sbt "core/Test/runMain app.softnetwork.elastic.client.perf.ResolveCostProbe 200 1000 out.tsv"
  * }}}
  */
object ResolveCostProbe {

  private val DefaultWarmup = 200
  private val DefaultRuns = 1000

  private val testLogger: Logger = LoggerFactory.getLogger(getClass)

  private val schema: Schema = Table(
    "events",
    columns = List(
      Column("id", SQLTypes.Keyword),
      Column("code", SQLTypes.Keyword),
      Column("descr", SQLTypes.Text),
      Column("amount", SQLTypes.Double),
      Column("qty", SQLTypes.BigInt),
      Column("flag", SQLTypes.Boolean),
      Column("event_ts", SQLTypes.Date)
    )
  )

  /** Shapes chosen so each column ISOLATES a stage: a bare projection pays the attach and nothing
    * else, a temporal WHERE pays `TemporalLiterals`, a CASE and a NULLIF pay the post-resolution
    * rules, and the window / GROUP BY rows are the largest ASTs the attach has to rebuild.
    */
  private val statements: List[(String, String)] = List(
    "bare projection"          -> "SELECT id FROM events LIMIT 10",
    "star"                     -> "SELECT * FROM events LIMIT 10",
    "several columns"          -> "SELECT id, code, descr, amount, qty FROM events LIMIT 10",
    "no limit (scroll-routed)" -> "SELECT id, code FROM events",
    "simple where"             -> "SELECT id FROM events WHERE code = 'x' LIMIT 10",
    "temporal where"           -> "SELECT id FROM events WHERE event_ts > '2025-01-01' LIMIT 10",
    "temporal where, space form" ->
    "SELECT id FROM events WHERE event_ts > '2025-01-01 10:00:00' LIMIT 10",
    "arithmetic"      -> "SELECT amount / qty AS r FROM events LIMIT 10",
    "cast arithmetic" -> "SELECT CAST(code AS BIGINT) + qty AS r FROM events LIMIT 10",
    "nullif"          -> "SELECT NULLIF(amount, 0) AS r FROM events LIMIT 10",
    "case" ->
    "SELECT CASE WHEN amount > 1 THEN 'hi' ELSE 'lo' END AS r FROM events LIMIT 10",
    "case over comparison" ->
    "SELECT CASE WHEN event_ts > '2025-01-01' THEN 1 ELSE 0 END AS r FROM events LIMIT 10",
    "group by" -> "SELECT code, COUNT(*) AS n FROM events GROUP BY code",
    "group by + having" ->
    "SELECT code, COUNT(*) AS n FROM events GROUP BY code HAVING COUNT(*) > 1",
    "order by" -> "SELECT id FROM events ORDER BY event_ts DESC LIMIT 10",
    "window" ->
    "SELECT id, ROW_NUMBER() OVER (PARTITION BY code ORDER BY amount DESC) AS rn FROM events",
    "wide where" ->
    "SELECT id FROM events WHERE code = 'a' AND amount > 1 AND qty < 9 AND flag = true LIMIT 10"
  )

  private class ProbeClient extends NopeClientApi {
    override protected def logger: Logger = testLogger
  }

  private def median(xs: Array[Long]): Double = {
    val sorted = xs.sorted
    val n = sorted.length
    if (n == 0) 0d
    else if (n % 2 == 1) sorted(n / 2).toDouble
    else (sorted(n / 2 - 1) + sorted(n / 2)) / 2.0
  }

  private def timeMedian(warmup: Int, runs: Int)(body: => Any): Double = {
    var i = 0
    while (i < warmup) { body; i += 1 }
    val samples = new Array[Long](runs)
    i = 0
    while (i < runs) {
      val t0 = System.nanoTime()
      body
      samples(i) = System.nanoTime() - t0
      i += 1
    }
    median(samples) / 1000.0 // microseconds
  }

  def main(args: Array[String]): Unit = {
    val warmup = args.lift(0).map(_.toInt).getOrElse(DefaultWarmup)
    val runs = args.lift(1).map(_.toInt).getOrElse(DefaultRuns)
    val out = args.lift(2)

    val client = new ProbeClient
    client.updateSchema("events", schema)

    // Global warm-up: `object Parser` and the whole combinator hot path must be initialised before
    // ANY timing starts, or the first statement measures class loading.
    statements.foreach { case (_, sql) =>
      var i = 0
      while (i < 50) {
        Parser(sql).foreach {
          case s: SingleSearch => client.resolveWithSchema(s)
          case _               => ()
        }
        i += 1
      }
    }

    val header =
      f"${"shape"}%-28s ${"parse"}%9s ${"resolve"}%9s ${"attach"}%9s ${"temporal"}%9s ${"validate"}%9s ${"resolve x2"}%11s"
    val lines = scala.collection.mutable.ListBuffer[String](header)
    println(header)

    statements.foreach { case (label, sql) =>
      val parsed = Parser(sql) match {
        case Right(s: SingleSearch) => s
        case other =>
          println(f"$label%-28s  SKIPPED ($other)")
          return
      }
      val resolvedOnce = client.resolveWithSchema(parsed) match {
        case ElasticSuccess(s) => s
        case ElasticFailure(e) =>
          println(f"$label%-28s  REFUSED (${e.message})")
          parsed
      }

      val tParse = timeMedian(warmup, runs)(Parser(sql))
      val tResolve = timeMedian(warmup, runs)(client.resolveWithSchema(parsed))
      val tAttach = timeMedian(warmup, runs)(parsed.update(Some(schema)))
      val tTemporal = timeMedian(warmup, runs)(TemporalLiterals(parsed, schema))
      val tValidate = timeMedian(warmup, runs)(resolvedOnce.validateResolved())
      val tTwice = timeMedian(warmup, runs) {
        client.resolveWithSchema(parsed) match {
          case ElasticSuccess(s) => client.resolveWithSchema(s)
          case f                 => f
        }
      }

      val line =
        f"$label%-28s $tParse%9.1f $tResolve%9.1f $tAttach%9.1f $tTemporal%9.1f $tValidate%9.1f $tTwice%11.1f"
      lines += line
      println(line)
    }

    println()
    println("microseconds, median of " + runs + " runs after " + warmup + " discarded")
    println(
      "`resolve x2` is what an un-LIMITed row query really pays (search -> scrollRows -> scroll)"
    )

    out.foreach { path =>
      val w = new java.io.PrintWriter(path)
      try lines.foreach(l => w.println(l.trim.replaceAll(" +", "\t")))
      finally w.close()
      println(s"written: $path")
    }
  }
}
