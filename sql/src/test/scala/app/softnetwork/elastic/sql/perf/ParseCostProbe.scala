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

/** Single-statement parse-latency probe for the SQL grammar.
  *
  * Every query the engine answers is parsed first, so per-statement parse latency is a cost the
  * user pays on every single request - which is what this probe measures. Suite wall time
  * (`sql/testOnly *ParserSpec`) is a useful secondary instrument but it aggregates thousands of
  * parses behind fixture construction and ScalaTest overhead.
  *
  * Run it with:
  *
  * {{{
  * sbt "sql/Test/runMain app.softnetwork.elastic.sql.perf.ParseCostProbe"
  * sbt "sql/Test/runMain app.softnetwork.elastic.sql.perf.ParseCostProbe 300 2000 out.tsv"
  * }}}
  *
  * Arguments, all optional and positional: `warmup` (default [[DefaultWarmup]] = 300 discarded
  * iterations per statement), `runs` (default [[DefaultRuns]] = 1000 timed iterations per
  * statement) and an output path - when given, the same table is also written there as TSV so two
  * runs (e.g. one per git tree) can be diffed mechanically.
  *
  * ==Warm-up==
  * The JIT needs a few hundred iterations to compile the combinator hot path; an unwarmed first
  * parse of `SELECT a FROM t` measures the interpreter and the `Parser` object's own lazy
  * initialisation, not the grammar. The probe therefore runs a **global** warm-up over the whole
  * statement set (so `object Parser` is fully initialised before any timing starts) and then
  * [[DefaultWarmup]] further **discarded** iterations per statement immediately before that
  * statement's timed loop. Those warm-up iterations are never included in any statistic.
  *
  * ==This probe NEVER asserts==
  * It prints and exits. A timing assertion in the test suite is a CI-flake liability - the older
  * `ParseCostProbeSpec` had to have its 1 ms ceiling relaxed to 10 ms (issues #269/#270) after it
  * reddened two PRs whose diffs touched no parse path, on loaded GitHub runners. Nothing here is
  * collected by `sbt test`: this is a `main`-bearing object, not a ScalaTest suite.
  *
  * A failure (bad argument, unparseable probe statement that was expected to parse) is raised as a
  * thrown exception, never `sys.exit`: this build sets no `fork` key on `sql`, so `runMain`
  * executes in the sbt JVM where `sys.exit` would go through sbt's `TrapExit` SecurityManager -
  * deprecated for removal from JDK 17.
  */
object ParseCostProbe {

  /** Discarded iterations per statement, before its timed loop. */
  val DefaultWarmup: Int = 300

  /** Timed iterations per statement. */
  val DefaultRuns: Int = 1000

  /** label -> statement. One entry per grammar shape a user actually sends, plus a deliberate
    * rejection: refusing a malformed statement is a cost too, and it is the one shape where the
    * parser explores every alternative before giving up.
    */
  val Statements: List[(String, String)] = List(
    "bare SELECT" ->
    "SELECT a FROM t",
    "SELECT list + LIMIT" ->
    "SELECT id, name, category, amount, created_at FROM orders LIMIT 100",
    "WHERE heavy" ->
    ("SELECT * FROM orders WHERE (status = 'OPEN' AND amount > 10.5) OR " +
    "(country <> 'USA' AND city LIKE '%berlin%') AND id IN (1,2,3) AND " +
    "created_at BETWEEN '2024-01-01' AND '2024-12-31' AND label IS NOT NULL"),
    "GROUP BY + HAVING" ->
    ("SELECT country, city, COUNT(customer_id) AS ct, MAX(amount) AS mx FROM orders " +
    "WHERE amount > 0 GROUP BY country, city " +
    "HAVING Country <> 'USA' AND COUNT(customer_id) > 1 ORDER BY ct DESC LIMIT 10"),
    "window function" ->
    ("SELECT name, salary, ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) AS rn " +
    "FROM emp LIMIT 100"),
    "JOIN" ->
    "SELECT o.id, c.name FROM orders o INNER JOIN customers c ON o.customer_id = c.id LIMIT 50",
    "derived table" ->
    "SELECT d.cid FROM (SELECT cid FROM customers WHERE region = 'EU') d WHERE d.cid > 10",
    "WHERE subquery" ->
    ("SELECT id FROM orders WHERE customer_id IN " +
    "(SELECT id FROM customers WHERE region = 'EU') AND amount > 100"),
    "DDL CREATE TABLE" ->
    ("CREATE TABLE IF NOT EXISTS users (id INT NOT NULL, name VARCHAR " +
    "FIELDS(raw Keyword) DEFAULT 'anonymous', birthdate DATE, " +
    "age INT SCRIPT AS (DATEDIFF(birthdate, CURRENT_DATE, YEAR)), PRIMARY KEY (id))"),
    "rejection (malformed)" ->
    "SELECT a, FROM WHERE t GROUP 'x' HAVING"
  )

  private def median(sorted: Array[Long]): Long = {
    val n = sorted.length
    if (n % 2 == 1) sorted(n / 2) else (sorted(n / 2 - 1) + sorted(n / 2)) / 2
  }

  /** Locale-independent on purpose: this machine's default locale renders a decimal comma, which
    * makes the printed table unparseable by anything downstream.
    */
  private def micros(nanos: Long): String =
    String.format(java.util.Locale.ROOT, "%9.1f", java.lang.Double.valueOf(nanos / 1000.0))

  def main(args: Array[String]): Unit = {
    val warmup = if (args.length > 0) args(0).toInt else DefaultWarmup
    val runs = if (args.length > 1) args(1).toInt else DefaultRuns
    val out = if (args.length > 2) Some(args(2)) else None
    if (warmup < 0 || runs < 1) {
      throw new RuntimeException(
        s"ParseCostProbe: warmup must be >= 0 and runs >= 1 (got warmup=$warmup runs=$runs)"
      )
    }

    // Global warm-up: forces `object Parser` initialisation and lets the JIT compile the
    // combinator hot path before ANY statement is timed. Without it the first statement in the
    // list absorbs the whole initialisation cost and looks pathological.
    var sink = 0
    for (_ <- 1 to warmup; (_, sql) <- Statements) sink ^= Parser(sql).hashCode()

    val lines = Statements.map { case (label, sql) =>
      for (_ <- 1 to warmup) sink ^= Parser(sql).hashCode()
      val samples = new Array[Long](runs)
      var i = 0
      while (i < runs) {
        val t0 = System.nanoTime()
        val r = Parser(sql)
        val t1 = System.nanoTime()
        sink ^= r.hashCode()
        samples(i) = t1 - t0
        i += 1
      }
      java.util.Arrays.sort(samples)
      val verdict = if (Parser(sql).isRight) "parses" else "rejected"
      (
        label,
        verdict,
        runs,
        median(samples),
        samples(0),
        samples(runs - 1),
        samples(math.min(runs - 1, (runs.toLong * 95L / 100L).toInt))
      )
    }

    val header =
      f"${"statement"}%-24s ${"verdict"}%-9s ${"runs"}%6s ${"median us"}%10s ${"min us"}%10s ${"p95 us"}%10s ${"max us"}%10s"
    println()
    println(s"ParseCostProbe - warmup=$warmup runs=$runs (warm-up iterations are discarded)")
    println(header)
    println("-" * header.length)
    lines.foreach { case (label, verdict, n, med, min, max, p95) =>
      println(
        f"$label%-24s $verdict%-9s $n%6d ${micros(med)}%10s ${micros(min)}%10s ${micros(p95)}%10s ${micros(max)}%10s"
      )
    }
    val total = lines.map(_._4).sum
    println("-" * header.length)
    println(f"sum of medians: ${micros(total)}%s us")
    println()

    out.foreach { path =>
      val sb = new StringBuilder
      sb.append("statement\tverdict\truns\tmedian_ns\tmin_ns\tp95_ns\tmax_ns\n")
      lines.foreach { case (label, verdict, n, med, min, max, p95) =>
        sb.append(s"$label\t$verdict\t$n\t$med\t$min\t$p95\t$max\n")
      }
      java.nio.file.Files.write(
        java.nio.file.Paths.get(path),
        sb.toString.getBytes(java.nio.charset.StandardCharsets.UTF_8)
      )
      println(s"ParseCostProbe: wrote $path")
    }

    // Keep `sink` observable so the JIT cannot eliminate the parses as dead code.
    if (sink == Int.MinValue) println("unreachable sink guard")
  }
}
