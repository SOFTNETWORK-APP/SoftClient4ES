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

import app.softnetwork.elastic.sql.function.aggregate.BucketScriptAggregation
import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query.{
  CreateTable,
  MetricSelectorScript,
  MultiSearch,
  SingleSearch
}
import app.softnetwork.elastic.sql.schema.{Column, Table}
import app.softnetwork.elastic.sql.`type`.{SQLType, SQLTypes}
import app.softnetwork.elastic.sql.{PainlessContext, PainlessContextType}

import scala.util.{Failure, Success, Try}

/** Per-statement cost of everything a statement pays before Elasticsearch sees it, STAGE BY STAGE
  * -- the companion of [[ParseCostProbe]] (parsing only) and [[EmissionCostProbe]] (parsing and one
  * predicate's translation).
  *
  * Each iteration is one request's worth of work on a FRESH parse, timed per stage:
  *
  *   - `parse` -- `Parser(sql)`, syntax and structure (`validate()`);
  *   - `attach` -- `updateAll` with the corpus schemas, what `SearchApi.resolveWithSchema` does
  *     once the mapping is read;
  *   - `validate` -- `validateResolved()`, the rules that need the column types (for a set
  *     operation, every branch's, then the branch type check);
  *   - `emit` -- the Painless the bridge renders from the statement: every script field, the WHERE
  *     criteria (query context), every per-group calculation (`bucket_script`) and the HAVING
  *     selector;
  *
  * and the `total` of the four. A statement refused at a stage stops there, as a request does, and
  * says so in its verdict; a DDL statement is parsed, and its computed columns' scripts read.
  *
  * Corpus: [[ParseCostProbe]] 's statements, [[EmissionCostProbe]] 's, the TYPE rules that are
  * asked once the column types are known (comparisons, IN, BETWEEN, NULLIF, CASE, GREATEST / LEAST,
  * intervals, arithmetic, set operations -- their true mismatches and the statements they used to
  * refuse although well typed), and the date-arithmetic request bodies. Then, as one block, the
  * date-arithmetic POPULATION: every operand against every operand for `+ - * / %`, a few
  * iterations each, reported as the sum of the per-statement medians per stage.
  *
  * Same protocol as the other probes: a global warm-up, a per-statement discarded warm-up, then
  * `runs` timed iterations, reported as medians in microseconds. It uses only what the base already
  * has, so the SAME file runs in a control tree.
  *
  * {{{
  * sbt "sql/Test/runMain app.softnetwork.elastic.sql.perf.ResolutionCostProbe"
  * sbt "sql/Test/runMain app.softnetwork.elastic.sql.perf.ResolutionCostProbe 100 300 out.tsv"
  * }}}
  *
  * With an output path, the per-statement table is written there as TSV and the population block
  * beside it (`<path>.population.tsv`), so two trees can be compared mechanically.
  *
  * ==This probe NEVER asserts==
  * It prints and exits (issues #269/#270: a timing assertion is a CI-flake liability). Compare two
  * trees by running it in each, INTERLEAVED, in one session -- an absolute number from another
  * session is noise.
  */
object ResolutionCostProbe {

  val DefaultWarmup: Int = 100
  val DefaultRuns: Int = 300
  val PopulationWarmup: Int = 2
  val PopulationRuns: Int = 5

  private def table(name: String, columns: (String, SQLType)*): Table =
    Table(name, columns = columns.map { case (n, t) => Column(n, t) }.toList)

  private val schemas: Map[String, Table] = Map(
    "t" -> table(
      "t",
      "id"     -> SQLTypes.Keyword,
      "g"      -> SQLTypes.Keyword,
      "s"      -> SQLTypes.Keyword,
      "name"   -> SQLTypes.Keyword,
      "other"  -> SQLTypes.Keyword,
      "num"    -> SQLTypes.Keyword,
      "n"      -> SQLTypes.Int,
      "m"      -> SQLTypes.Int,
      "x"      -> SQLTypes.Double,
      "amount" -> SQLTypes.Double,
      "d"      -> SQLTypes.Date,
      "d2"     -> SQLTypes.Date,
      "ts"     -> SQLTypes.Timestamp,
      "ts2"    -> SQLTypes.Timestamp,
      "b"      -> SQLTypes.Boolean
    ),
    "orders" -> table(
      "orders",
      "id"          -> SQLTypes.Keyword,
      "name"        -> SQLTypes.Keyword,
      "category"    -> SQLTypes.Keyword,
      "amount"      -> SQLTypes.Double,
      "created_at"  -> SQLTypes.Timestamp,
      "status"      -> SQLTypes.Keyword,
      "country"     -> SQLTypes.Keyword,
      "city"        -> SQLTypes.Keyword,
      "label"       -> SQLTypes.Keyword,
      "customer_id" -> SQLTypes.Keyword
    ),
    "customers" -> table(
      "customers",
      "id"     -> SQLTypes.Keyword,
      "cid"    -> SQLTypes.Int,
      "region" -> SQLTypes.Keyword,
      "name"   -> SQLTypes.Keyword
    ),
    "emp" -> table(
      "emp",
      "name"       -> SQLTypes.Keyword,
      "salary"     -> SQLTypes.Double,
      "department" -> SQLTypes.Keyword
    )
  )

  /** [[EmissionCostProbe]]'s predicate shapes (its list is private to it). */
  private val EmissionStatements: List[(String, String)] = List(
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
    "conversion (CAST)"              -> "SELECT id FROM t WHERE CAST(num AS INT) = 9",
    "conversion (TRY_CAST)"          -> "SELECT id FROM t WHERE TRY_CAST(num AS INT) = 9",
    "conditional (ISNULL)"           -> "SELECT id FROM t WHERE ISNULL(name) = true",
    "conditional (NULLIF, cast)"     -> "SELECT id FROM t WHERE NULLIF(CAST(num AS BIGINT), 0) > 1",
    "conditional (NULLIF, function)" -> "SELECT id FROM t WHERE NULLIF(UPPER(name), 'X') = 'A'",
    "string function (UPPER)"        -> "SELECT id FROM t WHERE UPPER(name) = 'A'",
    "math function (ABS)"            -> "SELECT id FROM t WHERE ABS(amount) > 10",
    "division, two integer columns"  -> "SELECT id FROM t WHERE n / m > 1",
    "division, literal divisor"      -> "SELECT id FROM t WHERE amount / 2 > 1",
    "IN, numeric"                    -> "SELECT id FROM t WHERE CAST(num AS BIGINT) IN (9, 12)",
    "IN, strings"                    -> "SELECT id FROM t WHERE UPPER(name) IN ('A', 'B')",
    "both sides"                     -> "SELECT id FROM t WHERE YEAR(d) = YEAR(ts)",
    "composite AND/OR" ->
    "SELECT id FROM t WHERE WEEKDAY(d) = 0 AND UPPER(name) = 'A' OR ABS(amount) > 10"
  )

  /** The TYPE rules asked once the column types are known: one true mismatch per rule, then the
    * statements that are well typed -- the ones parse-time rules used to refuse, a literal against
    * a bare column, functions over dates.
    */
  private val TypeRuleStatements: List[(String, String)] = List(
    "comparison, literal mismatch" -> "SELECT id FROM t WHERE n + 1 > 'abc'",
    "comparison, column mismatch"  -> "SELECT id FROM t WHERE s = n",
    "comparison, TIME"             -> "SELECT id FROM t WHERE CAST(ts AS TIME) > ts",
    "IN, mismatch"                 -> "SELECT id FROM t WHERE CAST(n AS INT) IN ('a', 'b')",
    "IN (HAVING), mismatch"        -> "SELECT g FROM t GROUP BY g HAVING COUNT(n) IN ('a', 'b')",
    "BETWEEN bounds, mismatch"     -> "SELECT id FROM t WHERE n BETWEEN 1 AND 'a'",
    "BETWEEN operand, mismatch"    -> "SELECT id FROM t WHERE CAST(s AS INT) BETWEEN 'a' AND 'b'",
    "NULLIF, mismatch"             -> "SELECT NULLIF(s, 1) AS v FROM t",
    "NULLIF, kinds"          -> "SELECT NULLIF(CAST(d AS DATE), CAST(ts AS TIMESTAMP)) AS v FROM t",
    "CASE WHEN, non-boolean" -> "SELECT CASE WHEN n THEN 1 ELSE 0 END AS v FROM t",
    "CASE <expr>, mismatch"  -> "SELECT CASE n WHEN 'a' THEN 1 ELSE 0 END AS v FROM t",
    "GREATEST, strings"      -> "SELECT GREATEST('a', 'b') AS v FROM t",
    "LEAST, dates"           -> "SELECT LEAST(d, d2) AS v FROM t",
    "date arithmetic, clock" -> "SELECT CURRENT_DATE * 2 AS v FROM t",
    "date arithmetic, columns"     -> "SELECT d + d2 AS v FROM t",
    "arithmetic, column mismatch"  -> "SELECT n + s AS v FROM t",
    "arithmetic, literal mismatch" -> "SELECT 1 + 'abc' AS v FROM t",
    "set operation, mismatch"      -> "SELECT 1 AS n FROM t UNION ALL SELECT 'a' AS n FROM t",
    "set operation, agreeing"      -> "SELECT id FROM t UNION ALL SELECT g FROM t",
    "well typed, CURRENT_DATE - d" -> "SELECT id FROM t WHERE CURRENT_DATE - d > 30",
    "well typed, d + 7"            -> "SELECT id FROM t WHERE d + 7 > CURRENT_DATE",
    "well typed, NOW() - ts"       -> "SELECT id FROM t WHERE NOW() - ts > 1",
    "well typed, CAST - d"         -> "SELECT id FROM t WHERE CAST(ts AS DATE) - d = 0",
    "well typed, d + 1 = DATE"     -> "SELECT id FROM t WHERE d + 1 = CAST('2024-02-01' AS DATE)",
    "well typed, d + 1 > literal"  -> "SELECT id FROM t WHERE d + 1 > '2024-01-01'",
    "well typed, HAVING alias" ->
    "SELECT g, MAX(d) + 1 AS v FROM t GROUP BY g HAVING v > CAST('2024-01-01' AS DATE)",
    "well typed, CASE WHEN b"    -> "SELECT CASE WHEN b THEN 1 ELSE 0 END AS v FROM t",
    "literal vs column, ="       -> "SELECT id FROM t WHERE n = '5'",
    "literal vs column, date"    -> "SELECT id FROM t WHERE d >= '2024-01-01'",
    "literal vs column, IN"      -> "SELECT id FROM t WHERE n IN ('1', '2')",
    "literal vs column, BETWEEN" -> "SELECT id FROM t WHERE n BETWEEN '1' AND '9'",
    "interval over a date"       -> "SELECT id FROM t WHERE DATE_ADD(d, INTERVAL 1 DAY) > ts",
    "COALESCE against the clock" -> "SELECT id FROM t WHERE COALESCE(d, d2) > CURRENT_DATE",
    "CASE, many branches" ->
    ("SELECT CASE WHEN n > 10 THEN 'a' WHEN n > 5 THEN 'b' WHEN b THEN 'c' ELSE 'd' END AS v " +
    "FROM t WHERE x > 1 AND s IN ('p', 'q') AND d BETWEEN '2024-01-01' AND '2024-12-31'")
  )

  /** The request bodies date arithmetic renders: rows, comparisons against an instant, the clock in
    * a per-group calculation, a cast over arithmetic, an aggregate's own type, a DDL computed
    * column. The typed NULLs come last: on a tree where `CAST(NULL AS ..)` retypes the shared NULL
    * literal, nothing after them can be affected.
    */
  private val DateArithmeticStatements: List[(String, String)] = List(
    "row, d + 1"                   -> "SELECT d + 1 AS v FROM t",
    "row, ts - 1.5"                -> "SELECT ts - 1.5 AS v FROM t",
    "row, d - d2"                  -> "SELECT d - d2 AS v FROM t",
    "row, ts - ts2"                -> "SELECT ts - ts2 AS v FROM t",
    "row, d + '1'"                 -> "SELECT d + '1' AS v FROM t",
    "row, COALESCE(d, d2) - d"     -> "SELECT COALESCE(d, d2) - d AS v FROM t",
    "row, NULLIF(d, d2) + 1"       -> "SELECT NULLIF(d, d2) + 1 AS v FROM t",
    "row, CASE - d"                -> "SELECT CASE WHEN n > 0 THEN d ELSE d2 END - d AS v FROM t",
    "where, d + 1 > ts"            -> "SELECT id FROM t WHERE d + 1 > ts",
    "where, ts > d + 1"            -> "SELECT id FROM t WHERE ts > d + 1",
    "where, CAST(d) >= ts"         -> "SELECT id FROM t WHERE CAST(d AS TIMESTAMP) >= ts",
    "where, LAST_DAY(d) = ts"      -> "SELECT id FROM t WHERE LAST_DAY(d) = ts",
    "where, d - d2 > 3"            -> "SELECT id FROM t WHERE d - d2 > 3",
    "group, CURRENT_DATE - MAX(d)" -> "SELECT g, CURRENT_DATE - MAX(d) AS v FROM t GROUP BY g",
    "group, NOW() - MAX(ts)"       -> "SELECT g, NOW() - MAX(ts) AS v FROM t GROUP BY g",
    "group, DATEDIFF(clock, MAX)" -> "SELECT g, DATEDIFF(CURRENT_DATE, MAX(d)) AS v FROM t GROUP BY g",
    "group, MAX(d) - MIN(d)"     -> "SELECT g, MAX(d) - MIN(d) AS v FROM t GROUP BY g",
    "group, COUNT(d) - 1"        -> "SELECT g, COUNT(d) - 1 AS v FROM t GROUP BY g",
    "group, MAX(d) - AVG(n)"     -> "SELECT g, MAX(d) - AVG(n) AS v FROM t GROUP BY g",
    "having, alias of MAX - MIN" -> "SELECT g, MAX(d) - MIN(d) AS k FROM t GROUP BY g HAVING k > 1",
    "cast, (n + x)::BIGINT"      -> "SELECT (n + x)::BIGINT AS v FROM t",
    "cast, (d + 1)::TIMESTAMP"   -> "SELECT (d + 1)::TIMESTAMP AS v FROM t",
    "cast, (x * 2)::INT"         -> "SELECT (x * 2)::INT AS v FROM t",
    "DDL, computed date columns" ->
    ("CREATE TABLE ev2 (id INT, d DATE, d2 DATE, gap INT SCRIPT AS (d - d2), " +
    "next DATE SCRIPT AS (COALESCE(d, d2) + 1), PRIMARY KEY (id))"),
    "typed NULL, CAST(NULL AS DATE) - d" -> "SELECT CAST(NULL AS DATE) - d AS v FROM t",
    "typed NULL, d + CAST(NULL AS INT)"  -> "SELECT d + CAST(NULL AS INT) AS v FROM t"
  )

  val Statements: List[(String, String, String)] =
    ParseCostProbe.Statements.map { case (l, s) => ("parse", l, s) } ++
    EmissionStatements.map { case (l, s) => ("emission", l, s) } ++
    TypeRuleStatements.map { case (l, s) => ("type rules", l, s) } ++
    DateArithmeticStatements.map { case (l, s) => ("date arithmetic", l, s) }

  /** The date-arithmetic population: every operand on both sides of every operator. */
  private val PopulationOperands: List[(String, Boolean)] = List(
    "d"                                  -> false,
    "ts"                                 -> false,
    "MAX(d)"                             -> true,
    "MAX(ts)"                            -> true,
    "2"                                  -> false,
    "1.5"                                -> false,
    "n"                                  -> false,
    "x"                                  -> false,
    "CURRENT_DATE"                       -> false,
    "NOW()"                              -> false,
    "NULL"                               -> false,
    "CASE WHEN n > 0 THEN d ELSE d2 END" -> false,
    "COALESCE(d, d2)"                    -> false,
    "COALESCE(ts, d)"                    -> false,
    "NULLIF(d, d2)"                      -> false,
    "GREATEST(n, 2)"                     -> false,
    "LEAST(x, 1.5)"                      -> false,
    "AVG(n)"                             -> true,
    "SUM(n)"                             -> true,
    "COUNT(d)"                           -> true,
    "'1'"                                -> false,
    "'1.5'"                              -> false,
    "'x'"                                -> false,
    // typed NULLs last, for the reason given on `DateArithmeticStatements`
    "CAST(NULL AS DATE)" -> false,
    "CAST(NULL AS INT)"  -> false
  )

  val Population: List[String] =
    for {
      (l, la) <- PopulationOperands
      (r, ra) <- PopulationOperands
      op      <- List("+", "-", "*", "/", "%")
    } yield
      if (la || ra) s"SELECT g, $l $op $r AS x FROM t GROUP BY g"
      else s"SELECT $l $op $r AS x FROM t"

  private def queryContext(): PainlessContext = PainlessContext(context = PainlessContextType.Query)

  /** What the bridge renders from a resolved statement, as Painless text. */
  private def emission(s: SingleSearch): Int = {
    var n = 0
    s.scriptFields.filterNot(_.isAggregation).foreach { f =>
      val ctx = queryContext()
      val body = f.painless(Some(ctx))
      n += (ctx.toString + body).length
    }
    s.where.flatMap(_.criteria).foreach { c =>
      val ctx = queryContext()
      val body = c.painless(Some(ctx))
      n += (ctx.toString + body).length
    }
    s.select.fields.filter(_.isBucketScript).foreach { f =>
      n += BucketScriptAggregation(f.identifier).update(s).identifier.painless(None).length
    }
    s.having.flatMap(_.criteria).foreach(c => n += MetricSelectorScript.metricSelector(c).length)
    n
  }

  /** One timed iteration: nanoseconds per stage (`-1` = not reached) and the verdict. */
  private final case class Iteration(
    parse: Long,
    attach: Long,
    validate: Long,
    emit: Long,
    verdict: String,
    sink: Int
  ) {
    def total: Long = parse + math.max(attach, 0L) + math.max(validate, 0L) + math.max(emit, 0L)
  }

  private def iteration(sql: String): Iteration = {
    val t0 = System.nanoTime()
    val parsed = Parser(sql)
    val t1 = System.nanoTime()
    parsed match {
      case Left(_) => Iteration(t1 - t0, -1, -1, -1, "parse-refused", 0)
      case Right(single: SingleSearch) =>
        val attached = Try(single.updateAll(schemas))
        val t2 = System.nanoTime()
        attached match {
          case Failure(e) => Iteration(t1 - t0, t2 - t1, -1, -1, s"attach-throws", e.hashCode())
          case Success(resolved) =>
            val verdict = Try(resolved.validateResolved())
            val t3 = System.nanoTime()
            verdict match {
              case Success(Right(())) =>
                val emitted = Try(emission(resolved))
                val t4 = System.nanoTime()
                Iteration(
                  t1 - t0,
                  t2 - t1,
                  t3 - t2,
                  t4 - t3,
                  if (emitted.isSuccess) "ok" else "emit-throws",
                  emitted.getOrElse(0)
                )
              case Success(Left(_)) =>
                Iteration(t1 - t0, t2 - t1, t3 - t2, -1, "resolve-refused", 0)
              case Failure(_) => Iteration(t1 - t0, t2 - t1, t3 - t2, -1, "validate-throws", 0)
            }
        }
      case Right(multi: MultiSearch) =>
        val attached = Try(multi.requests.map(_.updateAll(schemas)))
        val t2 = System.nanoTime()
        attached match {
          case Failure(e) => Iteration(t1 - t0, t2 - t1, -1, -1, "attach-throws", e.hashCode())
          case Success(branches) =>
            val verdict = Try(
              branches
                .map(_.validateResolved())
                .collectFirst { case l @ Left(_) => l }
                .getOrElse(MultiSearch.branchTypes(branches))
            )
            val t3 = System.nanoTime()
            verdict match {
              case Success(Right(())) =>
                val emitted = Try(branches.map(emission).sum)
                val t4 = System.nanoTime()
                Iteration(
                  t1 - t0,
                  t2 - t1,
                  t3 - t2,
                  t4 - t3,
                  if (emitted.isSuccess) "ok" else "emit-throws",
                  emitted.getOrElse(0)
                )
              case Success(Left(_)) =>
                Iteration(t1 - t0, t2 - t1, t3 - t2, -1, "resolve-refused", 0)
              case Failure(_) => Iteration(t1 - t0, t2 - t1, t3 - t2, -1, "validate-throws", 0)
            }
        }
      case Right(ddl: CreateTable) =>
        val scripts = Try(ddl.schema.columns.flatMap(_.script).map(_.source.length).sum)
        val t2 = System.nanoTime()
        Iteration(t1 - t0, -1, -1, t2 - t1, "ddl", scripts.getOrElse(0))
      case Right(other) => Iteration(t1 - t0, -1, -1, -1, "other", other.hashCode())
    }
  }

  private def median(values: Seq[Long]): Long =
    if (values.isEmpty || values.forall(_ < 0)) -1L
    else {
      val sorted = values.filter(_ >= 0).sorted.toArray
      val n = sorted.length
      if (n % 2 == 1) sorted(n / 2) else (sorted(n / 2 - 1) + sorted(n / 2)) / 2
    }

  private def p95(values: Seq[Long]): Long = {
    val sorted = values.sorted.toArray
    sorted(math.min(sorted.length - 1, (sorted.length.toLong * 95L / 100L).toInt))
  }

  /** Locale-independent: this machine's default locale renders a decimal comma. */
  private def micros(nanos: Long): String =
    if (nanos < 0) "-"
    else String.format(java.util.Locale.ROOT, "%.1f", java.lang.Double.valueOf(nanos / 1000.0))

  private final case class Row(
    group: String,
    label: String,
    verdict: String,
    parse: Long,
    attach: Long,
    validate: Long,
    emit: Long,
    total: Long,
    totalP95: Long
  )

  private def measure(
    group: String,
    label: String,
    sql: String,
    warmup: Int,
    runs: Int
  ): (Row, Int) = {
    var sink = 0
    for (_ <- 1 to warmup) sink ^= iteration(sql).sink
    val samples = (1 to runs).map(_ => iteration(sql))
    samples.foreach(s => sink ^= s.sink)
    val totals = samples.map(_.total)
    (
      Row(
        group,
        label,
        samples.head.verdict,
        median(samples.map(_.parse)),
        median(samples.map(_.attach)),
        median(samples.map(_.validate)),
        median(samples.map(_.emit)),
        median(totals),
        p95(totals)
      ),
      sink
    )
  }

  def main(args: Array[String]): Unit = {
    val warmup = if (args.length > 0) args(0).toInt else DefaultWarmup
    val runs = if (args.length > 1) args(1).toInt else DefaultRuns
    val out = if (args.length > 2) Some(args(2)) else None
    if (warmup < 0 || runs < 1) {
      throw new RuntimeException(
        s"ResolutionCostProbe: warmup must be >= 0 and runs >= 1 (got warmup=$warmup runs=$runs)"
      )
    }

    // Global warm-up: every object initialised and the hot paths compiled before any timing.
    var sink = 0
    for (_ <- 1 to warmup; (_, _, sql) <- Statements) sink ^= iteration(sql).sink

    val rows = Statements.map { case (group, label, sql) =>
      val (row, s) = measure(group, label, sql, warmup, runs)
      sink ^= s
      row
    }

    val header =
      f"${"group"}%-16s ${"statement"}%-36s ${"verdict"}%-16s ${"parse"}%8s ${"attach"}%8s ${"validate"}%9s ${"emit"}%8s ${"total"}%9s ${"p95"}%9s"
    println()
    println(s"ResolutionCostProbe - warmup=$warmup runs=$runs, medians in microseconds")
    println(header)
    println("-" * header.length)
    rows.foreach { r =>
      println(
        f"${r.group}%-16s ${r.label.take(36)}%-36s ${r.verdict}%-16s ${micros(r.parse)}%8s ${micros(
          r.attach
        )}%8s ${micros(r.validate)}%9s ${micros(r.emit)}%8s ${micros(r.total)}%9s ${micros(r.totalP95)}%9s"
      )
    }
    println("-" * header.length)
    def sum(f: Row => Long): Long = rows.map(f).filter(_ >= 0).sum
    println(
      f"${"SUM of medians"}%-70s ${micros(sum(_.parse))}%8s ${micros(sum(_.attach))}%8s ${micros(
        sum(_.validate)
      )}%9s ${micros(sum(_.emit))}%8s ${micros(sum(_.total))}%9s"
    )

    // The population block: a few iterations per statement, summed per stage.
    val population = Population.map { sql =>
      val (row, s) = measure("population", sql, sql, PopulationWarmup, PopulationRuns)
      sink ^= s
      row
    }
    def popSum(f: Row => Long): Long = population.map(f).filter(_ >= 0).sum
    val verdicts =
      population.groupBy(_.verdict).map { case (v, rs) => s"$v=${rs.size}" }.toSeq.sorted
    println()
    println(
      s"population: ${population.size} statements (${verdicts.mkString(", ")}), " +
      s"$PopulationRuns runs each, SUM of medians in microseconds: parse ${micros(popSum(_.parse))}, " +
      s"attach ${micros(popSum(_.attach))}, validate ${micros(popSum(_.validate))}, " +
      s"emit ${micros(popSum(_.emit))}, total ${micros(popSum(_.total))}"
    )
    println()

    out.foreach { path =>
      def write(file: String, lines: Seq[Row]): Unit = {
        val sb = new StringBuilder
        sb.append(
          "group\tstatement\tverdict\tparse_ns\tattach_ns\tvalidate_ns\temit_ns\ttotal_ns\ttotal_p95_ns\n"
        )
        lines.foreach { r =>
          sb.append(
            s"${r.group}\t${r.label}\t${r.verdict}\t${r.parse}\t${r.attach}\t${r.validate}\t${r.emit}\t${r.total}\t${r.totalP95}\n"
          )
        }
        java.nio.file.Files.write(
          java.nio.file.Paths.get(file),
          sb.toString.getBytes(java.nio.charset.StandardCharsets.UTF_8)
        )
      }
      write(path, rows)
      write(s"$path.population.tsv", population)
      println(s"ResolutionCostProbe: wrote $path and $path.population.tsv")
    }

    // Keep `sink` observable so the JIT cannot eliminate the work as dead code.
    if (sink == Int.MinValue) println("unreachable sink guard")
  }
}
