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

package app.softnetwork.elastic.sql.census

/** Epic 19 Story 19.1 - the hand-authored half of the census: one entry per SYNTAX FORM (AD-3),
  * every `ownerAnchor` a verbatim source substring (AD-2, resolved and uniqueness-checked by
  * DialectCensusSpec), every `exampleSql` proven parseable (AC-6), every `standard` tag carrying a
  * Rule-R-shaped citation (PD-2/PD-3). All engine-doc URLs below were FETCHED on 2026-08-31 and the
  * cited construct confirmed present on the cited page.
  */
object DialectCensus {

  import EsConstruct._
  import Kind._
  import Standard._

  private val S = "sql/src/main/scala/app/softnetwork/elastic/sql"
  private val FA = s"$S/function/aggregate/package.scala"
  private val FC = s"$S/function/cond/package.scala"
  private val FV = s"$S/function/convert/package.scala"
  private val FG = s"$S/function/geo/package.scala"
  private val FM = s"$S/function/math/package.scala"
  private val FS = s"$S/function/string/package.scala"
  private val FT = s"$S/function/time/package.scala"
  private val TP = s"$S/time/package.scala"
  private val OP = s"$S/operator/package.scala"
  private val OM = s"$S/operator/math/package.scala"
  private val OT = s"$S/operator/time/package.scala"
  private val QS = s"$S/query/Select.scala"
  private val QF = s"$S/query/From.scala"
  private val QW = s"$S/query/Where.scala"
  private val QG = s"$S/query/GroupBy.scala"
  private val QO = s"$S/query/OrderBy.scala"
  private val QL = s"$S/query/Limit.scala"
  private val QH = s"$S/query/Having.scala"
  private val RP = s"$S/package.scala"
  private val PFa = s"$S/parser/function/aggregate/package.scala"
  private val PFs = s"$S/parser/function/string/package.scala"
  private val PFt = s"$S/parser/function/time/package.scala"
  private val PFv = s"$S/parser/function/convert/package.scala"

  // Engine-doc URLs, each fetched 2026-08-31 with the cited construct confirmed present.
  private val PgStr = "https://www.postgresql.org/docs/16/functions-string.html"
  private val PgMath = "https://www.postgresql.org/docs/16/functions-math.html"
  private val PgDt = "https://www.postgresql.org/docs/16/functions-datetime.html"
  private val PgAgg = "https://www.postgresql.org/docs/16/functions-aggregate.html"
  private val PgCond = "https://www.postgresql.org/docs/16/functions-conditional.html"
  private val PgCmp = "https://www.postgresql.org/docs/16/functions-comparison.html"
  private val PgLogic = "https://www.postgresql.org/docs/16/functions-logical.html"
  private val PgSel = "https://www.postgresql.org/docs/16/sql-select.html"
  private val PgLim = "https://www.postgresql.org/docs/16/queries-limit.html"
  private val PgOrd = "https://www.postgresql.org/docs/16/queries-order.html"
  private val PgExpr = "https://www.postgresql.org/docs/16/sql-expressions.html"
  private val PgBool = "https://www.postgresql.org/docs/16/datatype-boolean.html"
  private val MyStr = "https://dev.mysql.com/doc/refman/8.4/en/string-functions.html"
  private val MyMath = "https://dev.mysql.com/doc/refman/8.4/en/mathematical-functions.html"
  private val MyDt = "https://dev.mysql.com/doc/refman/8.4/en/date-and-time-functions.html"
  private val MyAgg = "https://dev.mysql.com/doc/refman/8.4/en/aggregate-functions.html"
  private val MyCmp = "https://dev.mysql.com/doc/refman/8.4/en/comparison-operators.html"
  private val MyLogic = "https://dev.mysql.com/doc/refman/8.4/en/logical-operators.html"
  private val MySel = "https://dev.mysql.com/doc/refman/8.4/en/select.html"
  private val MyBool = "https://dev.mysql.com/doc/refman/8.4/en/boolean-literals.html"
  private val DkStr = "https://duckdb.org/docs/current/sql/functions/text.html"
  private val DkNum = "https://duckdb.org/docs/current/sql/functions/numeric.html"
  private val DkDate = "https://duckdb.org/docs/current/sql/functions/date.html"
  private val DkPart = "https://duckdb.org/docs/current/sql/functions/datepart.html"
  private val DkAgg = "https://duckdb.org/docs/current/sql/functions/aggregates.html"
  private val DkCast = "https://duckdb.org/docs/current/sql/expressions/cast.html"
  private val DkOrd = "https://duckdb.org/docs/current/sql/query_syntax/orderby.html"

  private def e(
    id: String,
    kind: Kind,
    token: String,
    spelling: String,
    file: String,
    anchor: String,
    ex: String,
    ar: String,
    std: Standard,
    ev: String,
    es: EsConstruct,
    notes: String = "",
    aliases: Option[List[String]] = None
  ): CensusEntry =
    CensusEntry(id, kind, token, spelling, file, anchor, ex, ar, std, ev, Some(es), notes, aliases)

  // ---- functions: aggregate (19 tokens; percentile forms per AD-3) ------------------------
  val aggregate: List[CensusEntry] = List(
    e(
      "fn.agg.count",
      Fn,
      "COUNT",
      "COUNT",
      FA,
      """case object COUNT extends Expr("COUNT") with AggregateFunction with Window""",
      "SELECT COUNT(*) AS c FROM emp",
      "1",
      Ansi,
      "SQL:2016 Part 2 (Foundation) Feature E091-02 COUNT",
      NativeAgg,
      "value_count / hits.total; COUNT(DISTINCT x) becomes a cardinality aggregation"
    ),
    e(
      "fn.agg.min",
      Fn,
      "MIN",
      "MIN",
      FA,
      """case object MIN extends Expr("MIN") with AggregateFunction with Window""",
      "SELECT MIN(salary) AS m FROM emp",
      "1",
      Ansi,
      "SQL:2016 Part 2 (Foundation) Feature E091-04 MIN",
      NativeAgg
    ),
    e(
      "fn.agg.max",
      Fn,
      "MAX",
      "MAX",
      FA,
      """case object MAX extends Expr("MAX") with AggregateFunction with Window""",
      "SELECT MAX(salary) AS m FROM emp",
      "1",
      Ansi,
      "SQL:2016 Part 2 (Foundation) Feature E091-03 MAX",
      NativeAgg
    ),
    e(
      "fn.agg.avg",
      Fn,
      "AVG",
      "AVG",
      FA,
      """case object AVG extends Expr("AVG") with AggregateFunction with Window""",
      "SELECT AVG(salary) AS a FROM emp",
      "1",
      Ansi,
      "SQL:2016 Part 2 (Foundation) Feature E091-01 AVG",
      NativeAgg
    ),
    e(
      "fn.agg.sum",
      Fn,
      "SUM",
      "SUM",
      FA,
      """case object SUM extends Expr("SUM") with AggregateFunction with Window""",
      "SELECT SUM(salary) AS s FROM emp",
      "1",
      Ansi,
      "SQL:2016 Part 2 (Foundation) Feature E091-05 SUM",
      NativeAgg
    ),
    e(
      "fn.agg.stddev",
      Fn,
      "STDDEV",
      "STDDEV",
      FA,
      """case object STDDEV extends Expr("STDDEV") with AggregateFunction with Window""",
      "SELECT STDDEV(salary) AS sd FROM emp",
      "1",
      AnsiAdjacent,
      s"PostgreSQL 16: stddev - $PgAgg ; MySQL 8.4: STDDEV - $MyAgg",
      NativeAgg,
      "ES extended_stats; sample key std_deviation_sampling requires ES 7.7+ (STDDEV = STDDEV_SAMP)"
    ),
    e(
      "fn.agg.stddev-pop",
      Fn,
      "STDDEV_POP",
      "STDDEV_POP",
      FA,
      """case object STDDEV_POP extends Expr("STDDEV_POP") with AggregateFunction with Window""",
      "SELECT STDDEV_POP(salary) AS sd FROM emp",
      "1",
      AnsiAdjacent,
      s"PostgreSQL 16: stddev_pop - $PgAgg ; MySQL 8.4: STDDEV_POP - $MyAgg",
      NativeAgg,
      "ES extended_stats un-suffixed std_deviation key (population), ES 6+"
    ),
    e(
      "fn.agg.stddev-samp",
      Fn,
      "STDDEV_SAMP",
      "STDDEV_SAMP",
      FA,
      """case object STDDEV_SAMP extends Expr("STDDEV_SAMP") with AggregateFunction with Window""",
      "SELECT STDDEV_SAMP(salary) AS sd FROM emp",
      "1",
      AnsiAdjacent,
      s"PostgreSQL 16: stddev_samp - $PgAgg ; MySQL 8.4: STDDEV_SAMP - $MyAgg",
      NativeAgg,
      "ES extended_stats std_deviation_sampling key requires ES 7.7+"
    ),
    e(
      "fn.agg.variance",
      Fn,
      "VARIANCE",
      "VARIANCE",
      FA,
      """case object VARIANCE extends Expr("VARIANCE") with AggregateFunction with Window""",
      "SELECT VARIANCE(salary) AS v FROM emp",
      "1",
      AnsiAdjacent,
      s"PostgreSQL 16: variance - $PgAgg ; MySQL 8.4: VARIANCE - $MyAgg",
      NativeAgg,
      "VARIANCE = VAR_SAMP here (ANSI/PostgreSQL sample default); MySQL VARIANCE is population (T3)"
    ),
    e(
      "fn.agg.var-pop",
      Fn,
      "VAR_POP",
      "VAR_POP",
      FA,
      """case object VAR_POP extends Expr("VAR_POP") with AggregateFunction with Window""",
      "SELECT VAR_POP(salary) AS v FROM emp",
      "1",
      AnsiAdjacent,
      s"PostgreSQL 16: var_pop - $PgAgg ; MySQL 8.4: VAR_POP - $MyAgg",
      NativeAgg
    ),
    e(
      "fn.agg.var-samp",
      Fn,
      "VAR_SAMP",
      "VAR_SAMP",
      FA,
      """case object VAR_SAMP extends Expr("VAR_SAMP") with AggregateFunction with Window""",
      "SELECT VAR_SAMP(salary) AS v FROM emp",
      "1",
      AnsiAdjacent,
      s"PostgreSQL 16: var_samp - $PgAgg ; MySQL 8.4: VAR_SAMP - $MyAgg",
      NativeAgg,
      "ES extended_stats variance_sampling key requires ES 7.7+"
    )
  )

  // ---- functions: percentiles (5 forms each, AD-3) + window functions ---------------------
  private val pctlEv =
    "ES percentiles aggregation (TDigest, approximate). T3 blocks ansi (shard approximation " +
    "differs from the exact ISO inverse-distribution semantics) and only PostgreSQL of the " +
    "PD-3 trio documents the WITHIN GROUP form, so Rule R lands on es_specific."

  val aggregateWindows: List[CensusEntry] = List(
    e(
      "fn.agg.percentile-cont.shorthand",
      Fn,
      "PERCENTILE_CONT",
      "PERCENTILE_CONT",
      PFa,
      "private[this] def percentile_args: PackratParser[(Option[Identifier], Double)] =",
      "SELECT PERCENTILE_CONT(salary, 0.5) AS med FROM emp",
      "2",
      EsSpecific,
      pctlEv,
      NativeAgg,
      "(col, p) shorthand form; p is a literal in [0,1]"
    ),
    e(
      "fn.agg.percentile-cont.within-group",
      Fn,
      "PERCENTILE_CONT",
      "PERCENTILE_CONT",
      PFa,
      "private[this] def percentile_within_group: PackratParser[Seq[Identifier]] =",
      "SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY salary) AS med FROM emp",
      "2",
      EsSpecific,
      pctlEv,
      NativeAgg,
      "ISO/PostgreSQL WITHIN GROUP form (PostgreSQL 16 documents this exact shape)"
    ),
    e(
      "fn.agg.percentile-cont.over-orderby",
      Fn,
      "PERCENTILE_CONT",
      "PERCENTILE_CONT",
      PFa,
      "def percentile_agg: PackratParser[WindowFunction] =",
      "SELECT PERCENTILE_CONT(0.5) OVER (ORDER BY salary) AS med FROM emp",
      "2",
      EsSpecific,
      pctlEv,
      NativeAgg,
      "value column taken from the OVER clause ORDER BY"
    ),
    e(
      "fn.agg.percentile-cont.shorthand-partition",
      Fn,
      "PERCENTILE_CONT",
      "PERCENTILE_CONT",
      PFa,
      "def percentile_agg: PackratParser[WindowFunction] =",
      "SELECT PERCENTILE_CONT(salary, 0.5) OVER (PARTITION BY dept) AS med FROM emp",
      "2",
      EsSpecific,
      pctlEv,
      NativeAgg,
      "shorthand value column + OVER (PARTITION BY ...) partitioning"
    ),
    e(
      "fn.agg.percentile-cont.within-group-partition",
      Fn,
      "PERCENTILE_CONT",
      "PERCENTILE_CONT",
      PFa,
      "def percentile_agg: PackratParser[WindowFunction] =",
      "SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY salary) " +
      "OVER (PARTITION BY dept) AS med FROM emp",
      "2",
      EsSpecific,
      pctlEv,
      NativeAgg,
      "WITHIN GROUP value column + OVER (PARTITION BY ...) partitioning"
    ),
    e(
      "fn.agg.percentile-disc.shorthand",
      Fn,
      "PERCENTILE_DISC",
      "PERCENTILE_DISC",
      PFa,
      "private[this] def percentile_args: PackratParser[(Option[Identifier], Double)] =",
      "SELECT PERCENTILE_DISC(salary, 0.9) AS p90 FROM emp",
      "2",
      EsSpecific,
      pctlEv,
      NativeAgg,
      "DISC is continuous-backed here (same TDigest digest as CONT) - a T3 semantics difference"
    ),
    e(
      "fn.agg.percentile-disc.within-group",
      Fn,
      "PERCENTILE_DISC",
      "PERCENTILE_DISC",
      PFa,
      "private[this] def percentile_within_group: PackratParser[Seq[Identifier]] =",
      "SELECT PERCENTILE_DISC(0.9) WITHIN GROUP (ORDER BY salary) AS p90 FROM emp",
      "2",
      EsSpecific,
      pctlEv,
      NativeAgg,
      "DISC continuous-backed; WITHIN GROUP form"
    ),
    e(
      "fn.agg.percentile-disc.over-orderby",
      Fn,
      "PERCENTILE_DISC",
      "PERCENTILE_DISC",
      PFa,
      "def percentile_agg: PackratParser[WindowFunction] =",
      "SELECT PERCENTILE_DISC(0.9) OVER (ORDER BY salary) AS p90 FROM emp",
      "2",
      EsSpecific,
      pctlEv,
      NativeAgg,
      "DISC continuous-backed; value column from OVER ORDER BY"
    ),
    e(
      "fn.agg.percentile-disc.shorthand-partition",
      Fn,
      "PERCENTILE_DISC",
      "PERCENTILE_DISC",
      PFa,
      "def percentile_agg: PackratParser[WindowFunction] =",
      "SELECT PERCENTILE_DISC(salary, 0.9) OVER (PARTITION BY dept) AS p90 FROM emp",
      "2",
      EsSpecific,
      pctlEv,
      NativeAgg,
      "DISC continuous-backed; shorthand + partition"
    ),
    e(
      "fn.agg.percentile-disc.within-group-partition",
      Fn,
      "PERCENTILE_DISC",
      "PERCENTILE_DISC",
      PFa,
      "def percentile_agg: PackratParser[WindowFunction] =",
      "SELECT PERCENTILE_DISC(0.9) WITHIN GROUP (ORDER BY salary) " +
      "OVER (PARTITION BY dept) AS p90 FROM emp",
      "2",
      EsSpecific,
      pctlEv,
      NativeAgg,
      "DISC continuous-backed; WITHIN GROUP + partition"
    ),
    e(
      "fn.agg.first-value",
      Fn,
      "FIRST_VALUE",
      "FIRST_VALUE",
      FA,
      """case object FIRST_VALUE extends Expr("FIRST_VALUE") with Window""",
      "SELECT FIRST_VALUE(name) OVER (PARTITION BY dept ORDER BY salary DESC) AS top1 FROM emp",
      "1",
      Ansi,
      "SQL:2016 Part 2 (Foundation) Feature T617 FIRST_VALUE and LAST_VALUE functions",
      NativeAgg,
      "top_hits-based window execution; the AST reports native_agg (Task 4.2 empirical check)"
    ),
    e(
      "fn.agg.first-value.first-alias",
      Fn,
      "FIRST_VALUE",
      "FIRST",
      FA,
      """override val words: List[String] = List(sql, "FIRST")""",
      "SELECT FIRST(name) OVER (ORDER BY salary DESC) AS top1 FROM emp",
      "1",
      EsSpecific,
      "ES top_hits window execution; of the PD-3 trio only DuckDB documents a first(...) " +
      "aggregate, so the alias lands on es_specific (T1)",
      NativeAgg,
      "alias spelling of FIRST_VALUE"
    ),
    e(
      "fn.agg.last-value",
      Fn,
      "LAST_VALUE",
      "LAST_VALUE",
      FA,
      """case object LAST_VALUE extends Expr("LAST_VALUE") with Window""",
      "SELECT LAST_VALUE(name) OVER (PARTITION BY dept ORDER BY salary DESC) AS low1 FROM emp",
      "1",
      Ansi,
      "SQL:2016 Part 2 (Foundation) Feature T617 FIRST_VALUE and LAST_VALUE functions",
      NativeAgg,
      "top_hits-based window execution; AST reports native_agg (Task 4.2)"
    ),
    e(
      "fn.agg.last-value.last-alias",
      Fn,
      "LAST_VALUE",
      "LAST",
      FA,
      """override val words: List[String] = List(sql, "LAST")""",
      "SELECT LAST(name) OVER (ORDER BY salary DESC) AS low1 FROM emp",
      "1",
      EsSpecific,
      "ES top_hits window execution; of the PD-3 trio only DuckDB documents a last(...) " +
      "aggregate, so the alias lands on es_specific (T1)",
      NativeAgg,
      "alias spelling of LAST_VALUE"
    ),
    e(
      "fn.agg.array-agg",
      Fn,
      "ARRAY_AGG",
      "ARRAY_AGG",
      FA,
      """case object ARRAY_AGG extends Expr("ARRAY_AGG") with Window""",
      "SELECT ARRAY_AGG(name) AS names FROM emp",
      "1",
      AnsiAdjacent,
      s"PostgreSQL 16: array_agg - $PgAgg ; DuckDB: array_agg (alias for list) - $DkAgg",
      NativeAgg,
      "multivalued top_hits collection"
    ),
    e(
      "fn.agg.array-agg.array-alias",
      Fn,
      "ARRAY_AGG",
      "ARRAY",
      FA,
      """override val words: List[String] = List(sql, "ARRAY")""",
      "SELECT ARRAY(name) AS names FROM emp",
      "1",
      EsSpecific,
      "ES top_hits multivalued collection; no PD-3 trio engine documents an ARRAY(col) " +
      "aggregate call (T1)",
      NativeAgg,
      "alias spelling of ARRAY_AGG; collides with the ARRAY<...> type word in other contexts"
    ),
    e(
      "fn.agg.row-number",
      Fn,
      "ROW_NUMBER",
      "ROW_NUMBER",
      FA,
      """case object ROW_NUMBER extends Expr("ROW_NUMBER") with Window""",
      "SELECT ROW_NUMBER() OVER (ORDER BY salary DESC) AS rn FROM emp",
      "0",
      Ansi,
      "SQL:2016 Part 2 (Foundation) Feature T611-01 ROW_NUMBER (Elementary OLAP operations)",
      NativeAgg,
      "top_hits-based ranking; ordinals assigned client-side; ORDER BY inside OVER is REQUIRED " +
      "(parser rejects its absence); AST reports native_agg (Task 4.2)"
    ),
    e(
      "fn.agg.rank",
      Fn,
      "RANK",
      "RANK",
      FA,
      """case object RANK extends Expr("RANK") with Window""",
      "SELECT RANK() OVER (PARTITION BY dept ORDER BY salary DESC) AS r FROM emp",
      "0",
      Ansi,
      "SQL:2016 Part 2 (Foundation) Feature T611-02 RANK (Elementary OLAP operations)",
      NativeAgg,
      "ties share rank, next rank skips; ordinals client-side"
    ),
    e(
      "fn.agg.dense-rank",
      Fn,
      "DENSE_RANK",
      "DENSE_RANK",
      FA,
      """case object DENSE_RANK extends Expr("DENSE_RANK") with Window""",
      "SELECT DENSE_RANK() OVER (PARTITION BY dept ORDER BY salary DESC) AS r FROM emp",
      "0",
      Ansi,
      "SQL:2016 Part 2 (Foundation) Feature T611 Elementary OLAP operations (DENSE_RANK)",
      NativeAgg,
      "ties share rank, next rank does not skip; ordinals client-side"
    )
  )

  // ---- functions: cond (6) ----------------------------------------------------------------
  val cond: List[CensusEntry] = List(
    e(
      "fn.cond.coalesce",
      Fn,
      "COALESCE",
      "COALESCE",
      FC,
      """case object Coalesce extends Expr("COALESCE") with ConditionalOp""",
      "SELECT COALESCE(nickname, name) AS n FROM emp",
      "1..n",
      Ansi,
      "SQL:2016 Part 2 (Foundation) Feature F261-04 COALESCE",
      PainlessField
    ),
    e(
      "fn.cond.nullif",
      Fn,
      "NULLIF",
      "NULLIF",
      FC,
      """case object NullIf extends Expr("NULLIF") with ConditionalOp""",
      "SELECT NULLIF(status, 'N/A') AS s FROM emp",
      "2",
      Ansi,
      "SQL:2016 Part 2 (Foundation) Feature F261-03 NULLIF",
      PainlessField
    ),
    e(
      "fn.cond.isnull",
      Fn,
      "ISNULL",
      "ISNULL",
      FC,
      """case object IsNull extends Expr("ISNULL") with ConditionalOp""",
      "SELECT ISNULL(manager_id) AS b FROM emp",
      "1",
      EsSpecific,
      "ES missing-field check rendered as painless '== null'; the 1-arg boolean ISNULL(x) form " +
      "matches no PD-3 trio engine (MySQL ISNULL exists but T3-differs: it is also 1-arg " +
      "boolean, yet only one trio engine documents it)",
      PainlessField,
      "boolean test function, not the T-SQL 2-arg ISNULL"
    ),
    e(
      "fn.cond.isnotnull",
      Fn,
      "ISNOTNULL",
      "ISNOTNULL",
      FC,
      """case object IsNotNull extends Expr("ISNOTNULL") with ConditionalOp""",
      "SELECT ISNOTNULL(manager_id) AS b FROM emp",
      "1",
      EsSpecific,
      "ES existence check rendered as painless '!= null'; ISNOTNULL(x) exists in no PD-3 trio " +
      "engine",
      PainlessField
    ),
    e(
      "fn.cond.greatest",
      Fn,
      "GREATEST",
      "GREATEST",
      FC,
      """case object Greatest extends Expr("GREATEST") with ConditionalOp""",
      "SELECT GREATEST(q1, q2, q3) AS g FROM scores",
      "1..n",
      AnsiAdjacent,
      s"PostgreSQL 16: GREATEST - $PgCond ; MySQL 8.4: GREATEST - $MyCmp",
      PainlessField,
      "NULLs are skipped (PostgreSQL semantics); MySQL returns NULL when any argument is NULL (T3)"
    ),
    e(
      "fn.cond.least",
      Fn,
      "LEAST",
      "LEAST",
      FC,
      """case object Least extends Expr("LEAST") with ConditionalOp""",
      "SELECT LEAST(q1, q2, q3) AS l FROM scores",
      "1..n",
      AnsiAdjacent,
      s"PostgreSQL 16: LEAST - $PgCond ; MySQL 8.4: LEAST - $MyCmp",
      PainlessField,
      "NULLs are skipped (PostgreSQL semantics); MySQL returns NULL when any argument is NULL (T3)"
    )
  )

  // ---- functions: convert (3 tokens, 5 forms) ---------------------------------------------
  val convert: List[CensusEntry] = List(
    e(
      "fn.convert.cast",
      Fn,
      "CAST",
      "CAST",
      FV,
      """case object Cast extends Expr("CAST") with TokenRegex""",
      "SELECT CAST(age AS BIGINT) AS a FROM emp",
      "2",
      Ansi,
      "SQL:2016 Part 2 (Foundation) Feature F201 CAST function",
      PainlessField
    ),
    e(
      "fn.convert.try-cast",
      Fn,
      "TRY_CAST",
      "TRY_CAST",
      FV,
      """case object TryCast extends Expr("TRY_CAST") with TokenRegex""",
      "SELECT TRY_CAST(age AS BIGINT) AS a FROM emp",
      "2",
      EsSpecific,
      "ES painless safe conversion: try/catch returning null (Conversion.toPainless); of " +
      "the PD-3 trio only DuckDB documents TRY_CAST",
      PainlessField,
      "DuckDB and T-SQL both spell it TRY_CAST"
    ),
    e(
      "fn.convert.try-cast.safe-cast-alias",
      Fn,
      "TRY_CAST",
      "SAFE_CAST",
      FV,
      """override def words: List[String] = List(sql, "SAFE_CAST")""",
      "SELECT SAFE_CAST(age AS BIGINT) AS a FROM emp",
      "2",
      EsSpecific,
      "ES painless safe conversion (try/catch null); SAFE_CAST is BigQuery's spelling, in " +
      "no PD-3 trio engine (T1)",
      PainlessField,
      "alias spelling of TRY_CAST"
    ),
    e(
      "fn.convert.convert",
      Fn,
      "CONVERT",
      "CONVERT",
      FV,
      """case object Convert extends Expr("CONVERT") with TokenRegex""",
      "SELECT CONVERT(age, BIGINT) AS a FROM emp",
      "2",
      EsSpecific,
      "ES painless type coercion; only MySQL of the PD-3 trio documents the same " +
      "CONVERT(expr, type) form",
      PainlessField,
      "MySQL 8.4 documents CONVERT(expr, type) as equivalent to CAST (cast-functions page, " +
      "fetched); one engine is below the Rule-R adjacency bar"
    ),
    e(
      "fn.convert.convert.tsql",
      Fn,
      "CONVERT",
      "CONVERT",
      PFv,
      "def convert_transact_sql_identifier: PackratParser[Identifier] =",
      "SELECT CONVERT(BIGINT, age) AS a FROM emp",
      "2",
      EsSpecific,
      "ES painless type coercion; the type-first argument order is T-SQL's, in no PD-3 " +
      "trio engine (T2)",
      PainlessField,
      "T-SQL argument order variant of CONVERT"
    )
  )

  // ---- functions: math (19 tokens, +CEILING/POWER aliases) --------------------------------
  val math: List[CensusEntry] = List(
    e(
      "fn.math.abs",
      Fn,
      "ABS",
      "ABS",
      FM,
      """case object Abs extends Expr("ABS") with MathOp""",
      "SELECT ABS(delta) AS a FROM sales",
      "1",
      Ansi,
      "SQL:2016 Part 2 (Foundation) Feature T441 ABS and MOD functions",
      PainlessField
    ),
    e(
      "fn.math.ceil",
      Fn,
      "CEIL",
      "CEIL",
      FM,
      """case object Ceil extends Expr("CEIL") with MathOp""",
      "SELECT CEIL(price) AS c FROM sales",
      "1",
      AnsiAdjacent,
      s"PostgreSQL 16: ceil - $PgMath ; MySQL 8.4: CEIL - $MyMath",
      PainlessField
    ),
    e(
      "fn.math.ceil.ceiling-alias",
      Fn,
      "CEIL",
      "CEILING",
      FM,
      """override def words: List[String] = List("CEILING", sql)""",
      "SELECT CEILING(price) AS c FROM sales",
      "1",
      Ansi,
      "SQL:2016 Part 2 (Foundation) Feature T621 Enhanced numeric functions (CEILING)",
      PainlessField,
      "CEILING is the standard's name; CEIL is the shorthand carried as the canonical token (T1)"
    ),
    e(
      "fn.math.floor",
      Fn,
      "FLOOR",
      "FLOOR",
      FM,
      """case object Floor extends Expr("FLOOR") with MathOp""",
      "SELECT FLOOR(price) AS f FROM sales",
      "1",
      Ansi,
      "SQL:2016 Part 2 (Foundation) Feature T621 Enhanced numeric functions (FLOOR)",
      PainlessField
    ),
    e(
      "fn.math.round",
      Fn,
      "ROUND",
      "ROUND",
      FM,
      """case object Round extends Expr("ROUND") with MathOp""",
      "SELECT ROUND(price, 2) AS r FROM sales",
      "1..2",
      AnsiAdjacent,
      s"PostgreSQL 16: round(v, s) - $PgMath ; MySQL 8.4: ROUND(X,D) - $MyMath",
      PainlessField
    ),
    e(
      "fn.math.exp",
      Fn,
      "EXP",
      "EXP",
      FM,
      """case object Exp extends Expr("EXP") with MathOp""",
      "SELECT EXP(rate) AS x FROM sales",
      "1",
      Ansi,
      "SQL:2016 Part 2 (Foundation) Feature T621 Enhanced numeric functions (EXP)",
      PainlessField
    ),
    e(
      "fn.math.log",
      Fn,
      "LOG",
      "LOG",
      FM,
      """case object Log extends Expr("LOG") with MathOp""",
      "SELECT LOG(price) AS l FROM sales",
      "1",
      EsSpecific,
      "ES painless Math.log, which is the NATURAL logarithm. TRAP: PostgreSQL 16 and DuckDB " +
      "log(x) are BASE-10; only MySQL LOG(X) shares the natural-log meaning, one engine (T3)",
      PainlessField,
      "silent-semantics trap for any renderer that maps LOG to a base-10 log"
    ),
    e(
      "fn.math.log10",
      Fn,
      "LOG10",
      "LOG10",
      FM,
      """case object Log10 extends Expr("LOG10") with MathOp""",
      "SELECT LOG10(price) AS l FROM sales",
      "1",
      Ansi,
      "SQL:2016 Part 2 (Foundation) Feature T624 Common logarithm functions (LOG10)",
      PainlessField
    ),
    e(
      "fn.math.pow",
      Fn,
      "POW",
      "POW",
      FM,
      """case object Pow extends Expr("POW") with MathOp""",
      "SELECT POW(price, 2) AS p FROM sales",
      "2",
      AnsiAdjacent,
      s"MySQL 8.4: POW - $MyMath ; DuckDB: pow - $DkNum",
      PainlessField,
      "exponent must be an integer literal in this dialect (parser production takes a long)"
    ),
    e(
      "fn.math.pow.power-alias",
      Fn,
      "POW",
      "POWER",
      FM,
      """override def words: List[String] = List("POWER", sql)""",
      "SELECT POWER(price, 2) AS p FROM sales",
      "2",
      Ansi,
      "SQL:2016 Part 2 (Foundation) Feature T621 Enhanced numeric functions (POWER)",
      PainlessField,
      "POWER is the standard's name; POW is the canonical token here (T1)"
    ),
    e(
      "fn.math.sqrt",
      Fn,
      "SQRT",
      "SQRT",
      FM,
      """case object Sqrt extends Expr("SQRT") with MathOp""",
      "SELECT SQRT(price) AS s FROM sales",
      "1",
      Ansi,
      "SQL:2016 Part 2 (Foundation) Feature T621 Enhanced numeric functions (SQRT)",
      PainlessField
    ),
    e(
      "fn.math.sign",
      Fn,
      "SIGN",
      "SIGN",
      FM,
      """case object Sign extends Expr("SIGN") with MathOp""",
      "SELECT SIGN(delta) AS s FROM sales",
      "1",
      AnsiAdjacent,
      s"PostgreSQL 16: sign - $PgMath ; MySQL 8.4: SIGN - $MyMath",
      PainlessField
    ),
    e(
      "fn.math.sin",
      Fn,
      "SIN",
      "SIN",
      FM,
      """case object Sin extends Expr("SIN") with Trigonometric""",
      "SELECT SIN(angle) AS s FROM shapes",
      "1",
      Ansi,
      "SQL:2016 Part 2 (Foundation) Feature T622 Trigonometric functions (SIN)",
      PainlessField
    ),
    e(
      "fn.math.asin",
      Fn,
      "ASIN",
      "ASIN",
      FM,
      """case object Asin extends Expr("ASIN") with Trigonometric""",
      "SELECT ASIN(ratio) AS a FROM shapes",
      "1",
      Ansi,
      "SQL:2016 Part 2 (Foundation) Feature T622 Trigonometric functions (ASIN)",
      PainlessField
    ),
    e(
      "fn.math.cos",
      Fn,
      "COS",
      "COS",
      FM,
      """case object Cos extends Expr("COS") with Trigonometric""",
      "SELECT COS(angle) AS c FROM shapes",
      "1",
      Ansi,
      "SQL:2016 Part 2 (Foundation) Feature T622 Trigonometric functions (COS)",
      PainlessField
    ),
    e(
      "fn.math.acos",
      Fn,
      "ACOS",
      "ACOS",
      FM,
      """case object Acos extends Expr("ACOS") with Trigonometric""",
      "SELECT ACOS(ratio) AS a FROM shapes",
      "1",
      Ansi,
      "SQL:2016 Part 2 (Foundation) Feature T622 Trigonometric functions (ACOS)",
      PainlessField
    ),
    e(
      "fn.math.tan",
      Fn,
      "TAN",
      "TAN",
      FM,
      """case object Tan extends Expr("TAN") with Trigonometric""",
      "SELECT TAN(angle) AS t FROM shapes",
      "1",
      Ansi,
      "SQL:2016 Part 2 (Foundation) Feature T622 Trigonometric functions (TAN)",
      PainlessField
    ),
    e(
      "fn.math.atan",
      Fn,
      "ATAN",
      "ATAN",
      FM,
      """case object Atan extends Expr("ATAN") with Trigonometric""",
      "SELECT ATAN(slope) AS a FROM shapes",
      "1",
      Ansi,
      "SQL:2016 Part 2 (Foundation) Feature T622 Trigonometric functions (ATAN)",
      PainlessField
    ),
    e(
      "fn.math.atan2",
      Fn,
      "ATAN2",
      "ATAN2",
      FM,
      """case object Atan2 extends Expr("ATAN2") with Trigonometric""",
      "SELECT ATAN2(dy, dx) AS a FROM shapes",
      "2",
      AnsiAdjacent,
      s"PostgreSQL 16: atan2 - $PgMath ; MySQL 8.4: ATAN2 - $MyMath",
      PainlessField
    ),
    e(
      "fn.math.degrees",
      Fn,
      "DEGREES",
      "DEGREES",
      FM,
      """case object Degrees extends Expr("DEGREES") with Trigonometric""",
      "SELECT DEGREES(angle) AS d FROM shapes",
      "1",
      AnsiAdjacent,
      s"PostgreSQL 16: degrees - $PgMath ; MySQL 8.4: DEGREES - $MyMath",
      PainlessField
    ),
    e(
      "fn.math.radians",
      Fn,
      "RADIANS",
      "RADIANS",
      FM,
      """case object Radians extends Expr("RADIANS") with Trigonometric""",
      "SELECT RADIANS(angle) AS r FROM shapes",
      "1",
      AnsiAdjacent,
      s"PostgreSQL 16: radians - $PgMath ; MySQL 8.4: RADIANS - $MyMath",
      PainlessField
    )
  )

  // ---- functions: string (14 tokens, 26 forms incl. aliases) ------------------------------
  val string: List[CensusEntry] = List(
    e(
      "fn.string.concat",
      Fn,
      "CONCAT",
      "CONCAT",
      FS,
      """case object Concat extends Expr("CONCAT") with StringOp""",
      "SELECT CONCAT(first_name, ' ', last_name) AS full_name FROM emp",
      "1..n",
      AnsiAdjacent,
      s"PostgreSQL 16: concat - $PgStr ; MySQL 8.4: CONCAT - $MyStr",
      PainlessField
    ),
    e(
      "fn.string.lower",
      Fn,
      "LOWER",
      "LOWER",
      FS,
      """case object Lower extends Expr("LOWER") with StringOp""",
      "SELECT LOWER(name) AS n FROM emp",
      "1",
      Ansi,
      "SQL:2016 Part 2 (Foundation) Feature E021-08 UPPER and LOWER functions",
      PainlessField
    ),
    e(
      "fn.string.lower.lcase-alias",
      Fn,
      "LOWER",
      "LCASE",
      FS,
      """override lazy val words: List[String] = List(sql, "LCASE")""",
      "SELECT LCASE(name) AS n FROM emp",
      "1",
      AnsiAdjacent,
      s"MySQL 8.4: LCASE - $MyStr ; DuckDB: lcase (alias for lower) - $DkStr",
      PainlessField,
      "alias spelling of LOWER (T1)"
    ),
    e(
      "fn.string.upper",
      Fn,
      "UPPER",
      "UPPER",
      FS,
      """case object Upper extends Expr("UPPER") with StringOp""",
      "SELECT UPPER(name) AS n FROM emp",
      "1",
      Ansi,
      "SQL:2016 Part 2 (Foundation) Feature E021-08 UPPER and LOWER functions",
      PainlessField
    ),
    e(
      "fn.string.upper.ucase-alias",
      Fn,
      "UPPER",
      "UCASE",
      FS,
      """override lazy val words: List[String] = List(sql, "UCASE")""",
      "SELECT UCASE(name) AS n FROM emp",
      "1",
      AnsiAdjacent,
      s"MySQL 8.4: UCASE - $MyStr ; DuckDB: ucase (alias for upper) - $DkStr",
      PainlessField,
      "alias spelling of UPPER (T1)"
    ),
    e(
      "fn.string.trim",
      Fn,
      "TRIM",
      "TRIM",
      FS,
      """case object Trim extends Expr("TRIM") with StringOp""",
      "SELECT TRIM(name) AS n FROM emp",
      "1",
      Ansi,
      "SQL:2016 Part 2 (Foundation) Feature E021-09 TRIM function",
      PainlessField,
      "only the bare TRIM(x) form; the standard's LEADING/TRAILING/BOTH ... FROM options do not " +
      "parse here (T3 noted, tag kept: the accepted subset matches the standard's default)"
    ),
    e(
      "fn.string.ltrim",
      Fn,
      "LTRIM",
      "LTRIM",
      FS,
      """case object Ltrim extends Expr("LTRIM") with StringOp""",
      "SELECT LTRIM(name) AS n FROM emp",
      "1",
      AnsiAdjacent,
      s"PostgreSQL 16: ltrim - $PgStr ; MySQL 8.4: LTRIM - $MyStr",
      PainlessField
    ),
    e(
      "fn.string.rtrim",
      Fn,
      "RTRIM",
      "RTRIM",
      FS,
      """case object Rtrim extends Expr("RTRIM") with StringOp""",
      "SELECT RTRIM(name) AS n FROM emp",
      "1",
      AnsiAdjacent,
      s"PostgreSQL 16: rtrim - $PgStr ; MySQL 8.4: RTRIM - $MyStr",
      PainlessField
    ),
    e(
      "fn.string.substring.ansi-from-for",
      Fn,
      "SUBSTRING",
      "SUBSTRING",
      PFs,
      "def substr: PackratParser[StringFunction[SQLVarchar]] =",
      "SELECT SUBSTRING(name FROM 2 FOR 3) AS c FROM emp",
      "2..3",
      Ansi,
      "SQL:2016 Part 2 (Foundation) Feature E021-06 SUBSTRING function",
      PainlessField,
      "keyword form; anchored on the PRODUCTION - one token, several forms, and only the " +
      "production distinguishes them (Task 3.3). The same production also accepts MIXED " +
      "keyword/comma forms (T2)"
    ),
    e(
      "fn.string.substring.comma",
      Fn,
      "SUBSTRING",
      "SUBSTRING",
      PFs,
      "def substr: PackratParser[StringFunction[SQLVarchar]] =",
      "SELECT SUBSTRING(name, 2, 3) AS c FROM emp",
      "2..3",
      AnsiAdjacent,
      s"PostgreSQL 16: substr(string, start, count) - $PgStr ; MySQL 8.4: SUBSTRING(str,pos," +
      s"len) - $MyStr",
      PainlessField,
      "comma form; SQL is 1-based and the parser enforces start >= 1"
    ),
    e(
      "fn.string.substring.substr-alias",
      Fn,
      "SUBSTRING",
      "SUBSTR",
      FS,
      """override lazy val words: List[String] = List(sql, "SUBSTR")""",
      "SELECT SUBSTR(name, 2, 3) AS c FROM emp",
      "2..3",
      AnsiAdjacent,
      s"PostgreSQL 16: substr() - $PgStr ; MySQL 8.4: SUBSTR() is a synonym for SUBSTRING() - " +
      MyStr,
      PainlessField,
      "alias spelling; NOT ansi - SUBSTR is not in SQL:2016 (T1, T3)"
    ),
    e(
      "fn.string.left.comma",
      Fn,
      "LEFT",
      "LEFT",
      PFs,
      "def left: PackratParser[StringFunction[SQLVarchar]] =",
      "SELECT LEFT(name, 3) AS l FROM emp",
      "2",
      AnsiAdjacent,
      s"PostgreSQL 16: left(string, n) - $PgStr ; MySQL 8.4: LEFT(str,len) - $MyStr",
      PainlessField
    ),
    e(
      "fn.string.left.for-form",
      Fn,
      "LEFT",
      "LEFT",
      PFs,
      "def left: PackratParser[StringFunction[SQLVarchar]] =",
      "SELECT LEFT(name FOR 3) AS l FROM emp",
      "2",
      EsSpecific,
      "ES painless substring; the LEFT(x FOR n) keyword variant exists in no PD-3 trio engine " +
      "(T2)",
      PainlessField,
      "keyword variant of LEFT sharing SUBSTRING's FOR token"
    ),
    e(
      "fn.string.right.comma",
      Fn,
      "RIGHT",
      "RIGHT",
      PFs,
      "def right: PackratParser[StringFunction[SQLVarchar]] =",
      "SELECT RIGHT(name, 3) AS r FROM emp",
      "2",
      AnsiAdjacent,
      s"PostgreSQL 16: right(string, n) - $PgStr ; MySQL 8.4: RIGHT(str,len) - $MyStr",
      PainlessField
    ),
    e(
      "fn.string.right.for-form",
      Fn,
      "RIGHT",
      "RIGHT",
      PFs,
      "def right: PackratParser[StringFunction[SQLVarchar]] =",
      "SELECT RIGHT(name FOR 3) AS r FROM emp",
      "2",
      EsSpecific,
      "ES painless substring; the RIGHT(x FOR n) keyword variant exists in no PD-3 trio engine " +
      "(T2)",
      PainlessField
    ),
    e(
      "fn.string.length",
      Fn,
      "LENGTH",
      "LENGTH",
      FS,
      """case object Length extends Expr("LENGTH") with StringOp""",
      "SELECT LENGTH(name) AS l FROM emp",
      "1",
      AnsiAdjacent,
      s"PostgreSQL 16: length(text) - $PgStr ; DuckDB: length - $DkStr",
      PainlessField,
      "character count (java String.length). MySQL LENGTH is BYTES - a T3 trap, which is why " +
      "MySQL is not one of the two cited engines; the standard's name is CHAR_LENGTH"
    ),
    e(
      "fn.string.length.len-alias",
      Fn,
      "LENGTH",
      "LEN",
      FS,
      """override lazy val words: List[String] = List(sql, "LEN")""",
      "SELECT LEN(name) AS l FROM emp",
      "1",
      EsSpecific,
      "ES painless String.length; of the PD-3 trio only DuckDB documents len (T-SQL also " +
      "spells it LEN) (T1)",
      PainlessField,
      "alias spelling of LENGTH"
    ),
    e(
      "fn.string.replace",
      Fn,
      "REPLACE",
      "REPLACE",
      FS,
      """case object Replace extends Expr("REPLACE") with StringOp""",
      "SELECT REPLACE(name, 'a', 'o') AS n FROM emp",
      "3",
      AnsiAdjacent,
      s"PostgreSQL 16: replace(string, from, to) - $PgStr ; MySQL 8.4: REPLACE(str,from,to) - " +
      MyStr,
      PainlessField
    ),
    e(
      "fn.string.replace.str-replace-alias",
      Fn,
      "REPLACE",
      "STR_REPLACE",
      FS,
      """override lazy val words: List[String] = List(sql, "STR_REPLACE")""",
      "SELECT STR_REPLACE(name, 'a', 'o') AS n FROM emp",
      "3",
      EsSpecific,
      "ES painless String.replace; STR_REPLACE exists in no PD-3 trio engine (T1)",
      PainlessField,
      "alias spelling of REPLACE"
    ),
    e(
      "fn.string.reverse",
      Fn,
      "REVERSE",
      "REVERSE",
      FS,
      """case object Reverse extends Expr("REVERSE") with StringOp""",
      "SELECT REVERSE(name) AS n FROM emp",
      "1",
      AnsiAdjacent,
      s"PostgreSQL 16: reverse - $PgStr ; MySQL 8.4: REVERSE - $MyStr",
      PainlessField
    ),
    e(
      "fn.string.position.in-form",
      Fn,
      "POSITION",
      "POSITION",
      FS,
      """case object Position extends Expr("POSITION") with StringOp""",
      "SELECT POSITION('a' IN name) AS p FROM emp",
      "2..3",
      Ansi,
      "SQL:2016 Part 2 (Foundation) Feature E021-11 POSITION function",
      PainlessField,
      "POSITION(substring IN string); 1-based result, 0 when absent + 1 per painless indexOf"
    ),
    e(
      "fn.string.position.comma",
      Fn,
      "POSITION",
      "POSITION",
      PFs,
      "def position: PackratParser[StringFunction[SQLBigInt]] =",
      "SELECT POSITION('a', name, 2) AS p FROM emp",
      "2..3",
      EsSpecific,
      "ES painless indexOf with a start offset; the comma form POSITION(sub, str, from) " +
      "matches MySQL's LOCATE argument order, not any trio engine's POSITION (T2)",
      PainlessField
    ),
    e(
      "fn.string.position.strpos-alias",
      Fn,
      "POSITION",
      "STRPOS",
      FS,
      """override lazy val words: List[String] = List(sql, "STRPOS", "LOCATE")""",
      "SELECT STRPOS('a', name) AS p FROM emp",
      "2..3",
      EsSpecific,
      "ES painless indexOf. TRAP: this dialect's STRPOS(search, string) REVERSES PostgreSQL " +
      "and DuckDB strpos(string, search) - same name, swapped arguments (T1, T3)",
      PainlessField,
      "argument-order trap for any renderer that treats STRPOS as PostgreSQL's"
    ),
    e(
      "fn.string.position.locate-alias",
      Fn,
      "POSITION",
      "LOCATE",
      FS,
      """override lazy val words: List[String] = List(sql, "STRPOS", "LOCATE")""",
      "SELECT LOCATE('a', name) AS p FROM emp",
      "2..3",
      EsSpecific,
      "ES painless indexOf; LOCATE(substr, str [, pos]) matches MySQL exactly but only that " +
      "one engine of the PD-3 trio documents it (T1)",
      PainlessField,
      "alias spelling; BI tools emit LOCATE (19.4 input)"
    ),
    e(
      "fn.string.regexp-like",
      Fn,
      "REGEXP_LIKE",
      "REGEXP_LIKE",
      FS,
      """case object RegexpLike extends Expr("REGEXP_LIKE") with StringOp""",
      "SELECT REGEXP_LIKE(name, 'Jo.*') AS b FROM emp",
      "2..3",
      AnsiAdjacent,
      s"PostgreSQL 16: regexp_like(string, pattern, flags) - $PgStr ; MySQL 8.4: " +
      s"REGEXP_LIKE(expr, pat, match_type) - $MyStr",
      PainlessField,
      "optional third argument carries match flags (i/c/n/m)"
    ),
    e(
      "fn.string.regexp-like.regexp-alias",
      Fn,
      "REGEXP_LIKE",
      "REGEXP",
      FS,
      """override lazy val words: List[String] = List(sql, "REGEXP")""",
      "SELECT REGEXP(name, 'Jo.*') AS b FROM emp",
      "2..3",
      EsSpecific,
      "ES painless Pattern.matcher; the function-call spelling REGEXP(str, pat) exists in no " +
      "PD-3 trio engine (MySQL's REGEXP is an infix operator, not a call) (T1)",
      PainlessField
    )
  )

  // ---- functions: time part 1 (current / trunc / extract / last_day) ----------------------
  val timeCurrent: List[CensusEntry] = List(
    e(
      "fn.time.current-date",
      Fn,
      "CURRENT_DATE",
      "CURRENT_DATE",
      FT,
      """case object CurrentDate extends Expr("CURRENT_DATE") with TokenRegex""",
      "SELECT CURRENT_DATE AS d FROM emp",
      "0",
      Ansi,
      "SQL:2016 Part 2 (Foundation) Feature F051-06 CURRENT_DATE",
      PainlessField,
      "bare niladic form (the standard's); resolved from the query timestamp param"
    ),
    e(
      "fn.time.current-date.parens",
      Fn,
      "CURRENT_DATE",
      "CURRENT_DATE",
      PFt,
      "def parens: PackratParser[List[Delimiter]] =",
      "SELECT CURRENT_DATE() AS d FROM emp",
      "0",
      EsSpecific,
      "ES query-timestamp param; the parenthesised CURRENT_DATE() variant is MySQL's, one " +
      "engine of the PD-3 trio (T2)",
      PainlessField,
      "empty-parens variant of CURRENT_DATE"
    ),
    e(
      "fn.time.current-date.curdate-alias",
      Fn,
      "CURRENT_DATE",
      "CURDATE",
      FT,
      """override lazy val words: List[String] = List(sql, "CURDATE")""",
      "SELECT CURDATE() AS d FROM emp",
      "0",
      EsSpecific,
      "ES query-timestamp param; CURDATE is MySQL's spelling, one engine of the PD-3 trio (T1)",
      PainlessField,
      "alias spelling; BI tools emit CURDATE (19.4 input)"
    ),
    e(
      "fn.time.current-time",
      Fn,
      "CURRENT_TIME",
      "CURRENT_TIME",
      FT,
      """case object CurrentTime extends Expr("CURRENT_TIME") with TokenRegex""",
      "SELECT CURRENT_TIME AS t FROM emp",
      "0",
      AnsiAdjacent,
      s"PostgreSQL 16: current_time - $PgDt ; MySQL 8.4: CURRENT_TIME - $MyDt",
      PainlessField,
      "returns a local time here (no time zone), unlike PostgreSQL's time with time zone (T3 " +
      "noted in the downgrade from ansi)"
    ),
    e(
      "fn.time.current-time.parens",
      Fn,
      "CURRENT_TIME",
      "CURRENT_TIME",
      PFt,
      "def parens: PackratParser[List[Delimiter]] =",
      "SELECT CURRENT_TIME() AS t FROM emp",
      "0",
      EsSpecific,
      "ES query-timestamp param; parenthesised variant is MySQL's, one trio engine (T2)",
      PainlessField
    ),
    e(
      "fn.time.current-time.curtime-alias",
      Fn,
      "CURRENT_TIME",
      "CURTIME",
      FT,
      """override lazy val words: List[String] = List(sql, "CURTIME")""",
      "SELECT CURTIME() AS t FROM emp",
      "0",
      EsSpecific,
      "ES query-timestamp param; CURTIME is MySQL's spelling, one trio engine (T1)",
      PainlessField
    ),
    e(
      "fn.time.current-timestamp",
      Fn,
      "CURRENT_TIMESTAMP",
      "CURRENT_TIMESTAMP",
      FT,
      """case object CurrentTimestamp extends Expr("CURRENT_TIMESTAMP") with TokenRegex""",
      "SELECT CURRENT_TIMESTAMP AS ts FROM emp",
      "0",
      AnsiAdjacent,
      s"PostgreSQL 16: current_timestamp - $PgDt ; MySQL 8.4: CURRENT_TIMESTAMP - $MyDt",
      PainlessField
    ),
    e(
      "fn.time.current-timestamp.parens",
      Fn,
      "CURRENT_TIMESTAMP",
      "CURRENT_TIMESTAMP",
      PFt,
      "def parens: PackratParser[List[Delimiter]] =",
      "SELECT CURRENT_TIMESTAMP() AS ts FROM emp",
      "0",
      EsSpecific,
      "ES query-timestamp param; parenthesised variant is MySQL's, one trio engine (T2)",
      PainlessField
    ),
    e(
      "fn.time.now",
      Fn,
      "NOW",
      "NOW",
      FT,
      """case object Now extends Expr("NOW") with TokenRegex""",
      "SELECT NOW() AS n FROM emp",
      "0",
      AnsiAdjacent,
      s"PostgreSQL 16: now() - $PgDt ; MySQL 8.4: NOW - $MyDt",
      PainlessField,
      "also accepted without parentheses"
    ),
    e(
      "fn.time.today",
      Fn,
      "TODAY",
      "TODAY",
      FT,
      """case object Today extends Expr("TODAY") with TokenRegex""",
      "SELECT TODAY() AS d FROM emp",
      "0",
      EsSpecific,
      "ES query-timestamp param truncated to a date; of the PD-3 trio only DuckDB documents " +
      "today()",
      PainlessField,
      "also accepted without parentheses"
    ),
    e(
      "fn.time.date-trunc.date-first",
      Fn,
      "DATE_TRUNC",
      "DATE_TRUNC",
      FT,
      """case object DateTrunc extends Expr("DATE_TRUNC") with TokenRegex with PainlessScript""",
      "SELECT DATE_TRUNC(created_at, MONTH) AS m FROM events",
      "2",
      EsSpecific,
      "ES date-math rounding / painless truncatedTo. TRAP: this (date, part) argument order " +
      "REVERSES PostgreSQL and DuckDB date_trunc(part, date) - same name, swapped arguments (T3)",
      PainlessField,
      "argument-order trap for any renderer that treats DATE_TRUNC as PostgreSQL's"
    ),
    e(
      "fn.time.date-trunc.part-first",
      Fn,
      "DATE_TRUNC",
      "DATE_TRUNC",
      PFt,
      "def date_trunc_transact_sql: PackratParser[FunctionWithIdentifier] =",
      "SELECT DATE_TRUNC(MONTH, created_at) AS m FROM events",
      "2",
      AnsiAdjacent,
      s"PostgreSQL 16: date_trunc(text, timestamp) - $PgDt ; DuckDB: date_trunc(part, date) - " +
      DkDate,
      PainlessField,
      "part-first variant matching PostgreSQL/DuckDB argument order (T2)"
    ),
    e(
      "fn.time.date-trunc.datetrunc-alias",
      Fn,
      "DATE_TRUNC",
      "DATETRUNC",
      FT,
      """override lazy val words: List[String] = List(sql, "DATETRUNC")""",
      "SELECT DATETRUNC(MONTH, created_at) AS m FROM events",
      "2",
      EsSpecific,
      "ES date-math rounding; of the PD-3 trio only DuckDB documents the datetrunc spelling " +
      "(T1)",
      PainlessField,
      "alias spelling of DATE_TRUNC"
    ),
    e(
      "fn.time.extract",
      Fn,
      "EXTRACT",
      "EXTRACT",
      FT,
      """case object Extract extends Expr("EXTRACT") with TokenRegex with PainlessScript""",
      "SELECT EXTRACT(YEAR FROM created_at) AS y FROM events",
      "2",
      AnsiAdjacent,
      s"PostgreSQL 16: extract(field from source) - $PgDt ; MySQL 8.4: EXTRACT(unit FROM " +
      s"date) - $MyDt",
      PainlessField,
      "EXTRACT is ISO SQL; tagged adjacent because the fetched conformance table carries no " +
      "dedicated EXTRACT feature row to cite (T4 downgrade)"
    ),
    e(
      "fn.time.last-day",
      Fn,
      "LAST_DAY",
      "LAST_DAY",
      FT,
      """case object LastDayOfMonth extends Expr("LAST_DAY") with TokenRegex with PainlessScript""",
      "SELECT LAST_DAY(created_at) AS d FROM events",
      "1",
      AnsiAdjacent,
      s"MySQL 8.4: LAST_DAY(date) - $MyDt ; DuckDB: last_day(date) - $DkDate",
      PainlessField
    ),
    e(
      "fn.time.last-day.lastday-alias",
      Fn,
      "LAST_DAY",
      "LASTDAY",
      FT,
      """override lazy val words: List[String] = List(sql, "LASTDAY")""",
      "SELECT LASTDAY(created_at) AS d FROM events",
      "1",
      EsSpecific,
      "ES painless withDayOfMonth(lengthOfMonth); the LASTDAY spelling exists in no PD-3 trio " +
      "engine (T1)",
      PainlessField
    )
  )

  // ---- functions: time part 2 (diff / add / sub / parse / format + aliases) ---------------
  val timeArith: List[CensusEntry] = List(
    e(
      "fn.time.date-diff.col-col-unit",
      Fn,
      "DATE_DIFF",
      "DATE_DIFF",
      FT,
      """case object DateDiff extends Expr("DATE_DIFF") with TokenRegex with PainlessScript""",
      "SELECT DATE_DIFF(start_date, end_date, DAY) AS d FROM projects",
      "2..3",
      EsSpecific,
      "ES painless ChronoUnit.between; the (start, end, unit) order is BigQuery's, in no PD-3 " +
      "trio engine; the unit defaults to DAY when omitted",
      PainlessField
    ),
    e(
      "fn.time.date-diff.unit-first",
      Fn,
      "DATE_DIFF",
      "DATE_DIFF",
      PFt,
      "def date_diff_transact_sql: PackratParser[BinaryFunction[_, _, _]] =",
      "SELECT DATE_DIFF(DAY, start_date, end_date) AS d FROM projects",
      "3",
      EsSpecific,
      "ES painless ChronoUnit.between; the unit-first order matches T-SQL DATEDIFF and DuckDB " +
      "date_diff(part, start, end) - one trio engine, below the adjacency bar (T2)",
      PainlessField,
      "DuckDB documents date_diff(part, startdate, enddate) with this exact order (datepart " +
      "page fetched)"
    ),
    e(
      "fn.time.date-diff.datediff-alias",
      Fn,
      "DATE_DIFF",
      "DATEDIFF",
      FT,
      """override lazy val words: List[String] = List(sql, "DATEDIFF")""",
      "SELECT DATEDIFF(start_date, end_date) AS d FROM projects",
      "2..3",
      EsSpecific,
      "ES painless ChronoUnit.between with the DAY default; 2-arg DATEDIFF(d1, d2) matches " +
      "MySQL's day-difference form, one trio engine (T1)",
      PainlessField,
      "alias spelling; MySQL DATEDIFF returns days, which is this form's default unit"
    ),
    e(
      "fn.time.date-add",
      Fn,
      "DATE_ADD",
      "DATE_ADD",
      FT,
      """case object DateAdd extends Expr("DATE_ADD") with TokenRegex""",
      "SELECT DATE_ADD(created_at, INTERVAL 7 DAY) AS d FROM events",
      "2",
      AnsiAdjacent,
      s"MySQL 8.4: DATE_ADD(date, INTERVAL expr unit) - $MyDt ; DuckDB: date_add - $DkDate",
      PainlessField
    ),
    e(
      "fn.time.date-add.tsql",
      Fn,
      "DATE_ADD",
      "DATE_ADD",
      PFt,
      "def date_add_transact_sql : PackratParser[DateFunction with FunctionWithIdentifier " +
      "with DateMathScript]",
      "SELECT DATE_ADD(DAY, 7, created_at) AS d FROM events",
      "3",
      EsSpecific,
      "ES date-math / painless plus; the (unit, n, date) order is T-SQL DATEADD's, in no PD-3 " +
      "trio engine (T2)",
      PainlessField
    ),
    e(
      "fn.time.date-add.dateadd-alias",
      Fn,
      "DATE_ADD",
      "DATEADD",
      FT,
      """override lazy val words: List[String] = List(sql, "DATEADD")""",
      "SELECT DATEADD(created_at, INTERVAL 7 DAY) AS d FROM events",
      "2",
      EsSpecific,
      "ES date-math / painless plus; DATEADD is T-SQL's spelling, in no PD-3 trio engine (T1)",
      PainlessField
    ),
    e(
      "fn.time.date-sub",
      Fn,
      "DATE_SUB",
      "DATE_SUB",
      FT,
      """case object DateSub extends Expr("DATE_SUB") with TokenRegex""",
      "SELECT DATE_SUB(created_at, INTERVAL 7 DAY) AS d FROM events",
      "2",
      AnsiAdjacent,
      s"MySQL 8.4: DATE_SUB(date, INTERVAL expr unit) - $MyDt ; DuckDB: date_sub - $DkDate",
      PainlessField
    ),
    e(
      "fn.time.date-sub.tsql",
      Fn,
      "DATE_SUB",
      "DATE_SUB",
      PFt,
      "def date_sub_transact_sql : PackratParser[DateFunction with FunctionWithIdentifier " +
      "with DateMathScript]",
      "SELECT DATE_SUB(DAY, 7, created_at) AS d FROM events",
      "3",
      EsSpecific,
      "ES date-math / painless minus; unit-first T-SQL order, in no PD-3 trio engine (T2)",
      PainlessField
    ),
    e(
      "fn.time.date-sub.datesub-alias",
      Fn,
      "DATE_SUB",
      "DATESUB",
      FT,
      """override lazy val words: List[String] = List(sql, "DATESUB")""",
      "SELECT DATESUB(created_at, INTERVAL 7 DAY) AS d FROM events",
      "2",
      EsSpecific,
      "ES date-math / painless minus; the DATESUB spelling exists in no PD-3 trio engine (T1)",
      PainlessField
    ),
    e(
      "fn.time.date-parse",
      Fn,
      "DATE_PARSE",
      "DATE_PARSE",
      FT,
      """case object DateParse extends Expr("DATE_PARSE") with TokenRegex with PainlessScript""",
      "SELECT DATE_PARSE(date_str, '%Y-%m-%d') AS d FROM events",
      "2",
      EsSpecific,
      "ES painless LocalDate.parse with MySQL-style % format tokens converted to " +
      "DateTimeFormatter patterns; DATE_PARSE is Trino's name, in no PD-3 trio engine",
      PainlessField
    ),
    e(
      "fn.time.date-parse.dateparse-alias",
      Fn,
      "DATE_PARSE",
      "DATEPARSE",
      FT,
      """override lazy val words: List[String] = List(sql, "DATEPARSE", "TO_DATE", "PARSE_DATE")""",
      "SELECT DATEPARSE(date_str, '%Y-%m-%d') AS d FROM events",
      "2",
      EsSpecific,
      "ES painless LocalDate.parse; the DATEPARSE spelling exists in no PD-3 trio engine (T1)",
      PainlessField
    ),
    e(
      "fn.time.date-parse.to-date-alias",
      Fn,
      "DATE_PARSE",
      "TO_DATE",
      FT,
      """override lazy val words: List[String] = List(sql, "DATEPARSE", "TO_DATE", "PARSE_DATE")""",
      "SELECT TO_DATE(date_str, '%Y-%m-%d') AS d FROM events",
      "2",
      EsSpecific,
      "ES painless LocalDate.parse; PostgreSQL documents to_date(text, format) but with " +
      "TO_CHAR-style patterns, not this dialect's % tokens - same name, different format " +
      "language (T3), and one trio engine either way (T1)",
      PainlessField
    ),
    e(
      "fn.time.date-parse.parse-date-alias",
      Fn,
      "DATE_PARSE",
      "PARSE_DATE",
      FT,
      """override lazy val words: List[String] = List(sql, "DATEPARSE", "TO_DATE", "PARSE_DATE")""",
      "SELECT PARSE_DATE(date_str, '%Y-%m-%d') AS d FROM events",
      "2",
      EsSpecific,
      "ES painless LocalDate.parse; PARSE_DATE is BigQuery's spelling (argument order " +
      "reversed there), in no PD-3 trio engine (T1)",
      PainlessField
    ),
    e(
      "fn.time.date-format",
      Fn,
      "DATE_FORMAT",
      "DATE_FORMAT",
      FT,
      """case object DateFormat extends Expr("DATE_FORMAT") with TokenRegex with PainlessScript""",
      "SELECT DATE_FORMAT(created_at, '%Y-%m-%d') AS s FROM events",
      "2",
      EsSpecific,
      "ES painless DateTimeFormatter.format with MySQL-style % tokens; only MySQL of the PD-3 " +
      "trio documents DATE_FORMAT(date, format), one engine",
      PainlessField
    ),
    e(
      "fn.time.date-format.format-alias",
      Fn,
      "DATE_FORMAT",
      "FORMAT",
      FT,
      """override lazy val words: List[String] = List(sql, "FORMAT", "DATEFORMAT")""",
      "SELECT FORMAT(created_at, '%Y-%m-%d') AS s FROM events",
      "2",
      EsSpecific,
      "ES painless DateTimeFormatter.format; bare FORMAT(date, fmt) is T-SQL-flavoured and " +
      "collides with MySQL's numeric FORMAT function - same name, different semantics (T1, T3)",
      PainlessField
    ),
    e(
      "fn.time.date-format.dateformat-alias",
      Fn,
      "DATE_FORMAT",
      "DATEFORMAT",
      FT,
      """override lazy val words: List[String] = List(sql, "FORMAT", "DATEFORMAT")""",
      "SELECT DATEFORMAT(created_at, '%Y-%m-%d') AS s FROM events",
      "2",
      EsSpecific,
      "ES painless DateTimeFormatter.format; the DATEFORMAT spelling exists in no PD-3 trio " +
      "engine (T1)",
      PainlessField
    ),
    e(
      "fn.time.datetime-add",
      Fn,
      "DATETIME_ADD",
      "DATETIME_ADD",
      FT,
      """case object DateTimeAdd extends Expr("DATETIME_ADD") with TokenRegex""",
      "SELECT DATETIME_ADD(updated_at, INTERVAL 2 HOUR) AS d FROM events",
      "2",
      EsSpecific,
      "ES date-math / painless plus on a datetime; DATETIME_ADD is BigQuery's name, in no " +
      "PD-3 trio engine",
      PainlessField
    ),
    e(
      "fn.time.datetime-add.tsql",
      Fn,
      "DATETIME_ADD",
      "DATETIME_ADD",
      PFt,
      "def datetime_add_transact_sql : PackratParser[DateTimeFunction with " +
      "FunctionWithIdentifier with DateMathScript]",
      "SELECT DATETIME_ADD(HOUR, 2, updated_at) AS d FROM events",
      "3",
      EsSpecific,
      "ES date-math / painless plus; unit-first T-SQL order, in no PD-3 trio engine (T2)",
      PainlessField
    ),
    e(
      "fn.time.datetime-add.datetimeadd-alias",
      Fn,
      "DATETIME_ADD",
      "DATETIMEADD",
      FT,
      """override lazy val words: List[String] = List(sql, "DATETIMEADD")""",
      "SELECT DATETIMEADD(updated_at, INTERVAL 2 HOUR) AS d FROM events",
      "2",
      EsSpecific,
      "ES date-math / painless plus; the DATETIMEADD spelling exists in no PD-3 trio engine " +
      "(T1)",
      PainlessField
    ),
    e(
      "fn.time.datetime-sub",
      Fn,
      "DATETIME_SUB",
      "DATETIME_SUB",
      FT,
      """case object DateTimeSub extends Expr("DATETIME_SUB") with TokenRegex""",
      "SELECT DATETIME_SUB(updated_at, INTERVAL 2 HOUR) AS d FROM events",
      "2",
      EsSpecific,
      "ES date-math / painless minus on a datetime; DATETIME_SUB is BigQuery's name, in no " +
      "PD-3 trio engine",
      PainlessField
    ),
    e(
      "fn.time.datetime-sub.tsql",
      Fn,
      "DATETIME_SUB",
      "DATETIME_SUB",
      PFt,
      "def datetime_sub_transact_sql : PackratParser[DateTimeFunction with " +
      "FunctionWithIdentifier with DateMathScript]",
      "SELECT DATETIME_SUB(HOUR, 2, updated_at) AS d FROM events",
      "3",
      EsSpecific,
      "ES date-math / painless minus; unit-first T-SQL order, in no PD-3 trio engine (T2)",
      PainlessField
    ),
    e(
      "fn.time.datetime-sub.datetimesub-alias",
      Fn,
      "DATETIME_SUB",
      "DATETIMESUB",
      FT,
      """override lazy val words: List[String] = List(sql, "DATETIMESUB")""",
      "SELECT DATETIMESUB(updated_at, INTERVAL 2 HOUR) AS d FROM events",
      "2",
      EsSpecific,
      "ES date-math / painless minus; the DATETIMESUB spelling exists in no PD-3 trio engine " +
      "(T1)",
      PainlessField
    ),
    e(
      "fn.time.datetime-parse",
      Fn,
      "DATETIME_PARSE",
      "DATETIME_PARSE",
      FT,
      """case object DateTimeParse extends Expr("DATETIME_PARSE") with TokenRegex with """ +
      "PainlessScript",
      "SELECT DATETIME_PARSE(ts_str, '%Y-%m-%d %H:%i:%s') AS d FROM events",
      "2",
      EsSpecific,
      "ES painless ZonedDateTime.parse with MySQL-style % tokens; DATETIME_PARSE exists in no " +
      "PD-3 trio engine",
      PainlessField
    ),
    e(
      "fn.time.datetime-parse.datetimeparse-alias",
      Fn,
      "DATETIME_PARSE",
      "DATETIMEPARSE",
      FT,
      """List(sql, "DATETIMEPARSE", "TO_TIMESTAMP", "PARSE_DATETIME")""",
      "SELECT DATETIMEPARSE(ts_str, '%Y-%m-%d %H:%i:%s') AS d FROM events",
      "2",
      EsSpecific,
      "ES painless ZonedDateTime.parse; the DATETIMEPARSE spelling exists in no PD-3 trio " +
      "engine (T1)",
      PainlessField
    ),
    e(
      "fn.time.datetime-parse.to-timestamp-alias",
      Fn,
      "DATETIME_PARSE",
      "TO_TIMESTAMP",
      FT,
      """List(sql, "DATETIMEPARSE", "TO_TIMESTAMP", "PARSE_DATETIME")""",
      "SELECT TO_TIMESTAMP(ts_str, '%Y-%m-%d %H:%i:%s') AS d FROM events",
      "2",
      EsSpecific,
      "ES painless ZonedDateTime.parse; PostgreSQL documents to_timestamp(text, format) but " +
      "with TO_CHAR-style patterns, not % tokens - same name, different format language (T3), " +
      "one trio engine either way (T1)",
      PainlessField
    ),
    e(
      "fn.time.datetime-parse.parse-datetime-alias",
      Fn,
      "DATETIME_PARSE",
      "PARSE_DATETIME",
      FT,
      """List(sql, "DATETIMEPARSE", "TO_TIMESTAMP", "PARSE_DATETIME")""",
      "SELECT PARSE_DATETIME(ts_str, '%Y-%m-%d %H:%i:%s') AS d FROM events",
      "2",
      EsSpecific,
      "ES painless ZonedDateTime.parse; PARSE_DATETIME is BigQuery's spelling, in no PD-3 " +
      "trio engine (T1)",
      PainlessField
    ),
    e(
      "fn.time.datetime-format",
      Fn,
      "DATETIME_FORMAT",
      "DATETIME_FORMAT",
      FT,
      """case object DateTimeFormat extends Expr("DATETIME_FORMAT") with TokenRegex with """ +
      "PainlessScript",
      "SELECT DATETIME_FORMAT(updated_at, '%Y-%m-%d %H:%i:%s') AS s FROM events",
      "2",
      EsSpecific,
      "ES painless DateTimeFormatter.format on a datetime; DATETIME_FORMAT exists in no PD-3 " +
      "trio engine",
      PainlessField,
      "the only DATETIME_* token with no alias spelling"
    )
  )

  // ---- functions: temporal extractors from sql/time (15 tokens, 22 alias spellings) -------
  // All extract to painless ChronoField/IsoFields getters; every row is a 1-arg call on a
  // temporal column, so a compact builder keeps the 37 rows reviewable.
  private def xr(
    id: String,
    token: String,
    spelling: String,
    anchor: String,
    std: Standard,
    ev: String,
    notes: String = ""
  ): CensusEntry =
    e(
      id,
      Fn,
      token,
      spelling,
      TP,
      anchor,
      s"SELECT $spelling(created_at) AS x FROM events",
      "1",
      std,
      ev,
      PainlessField,
      notes
    )

  /** The TimeField trait's COMPUTED words line - the reason 22 alias spellings exist in no per-
    * token source literal and 9 exist in no literal at all (F-2). Alias rows born from it anchor
    * here.
    */
  private val computedWordsAnchor =
    """List(timeField, timeField.replaceAll("_", ""), sql).distinct"""

  private val chronoEv =
    "ES painless ChronoField extraction (java.time); this spelling is carried only by the " +
    "runtime-computed TimeField.words list"

  val extractors: List[CensusEntry] = List(
    xr(
      "fn.time.year",
      "YEAR",
      "YEAR",
      """case object YEAR extends Expr("YEAR") with TimeField""",
      AnsiAdjacent,
      s"MySQL 8.4: YEAR - $MyDt ; DuckDB: year(date) - $DkPart"
    ),
    xr(
      "fn.time.month",
      "MONTH",
      "MONTH",
      """case object MONTH_OF_YEAR extends Expr("MONTH") with TimeField""",
      AnsiAdjacent,
      s"MySQL 8.4: MONTH - $MyDt ; DuckDB: month(date) - $DkPart",
      "token OBJECT is MONTH_OF_YEAR; its sql (the census token) is MONTH"
    ),
    xr(
      "fn.time.month.month-of-year-alias",
      "MONTH",
      "MONTH_OF_YEAR",
      computedWordsAnchor,
      EsSpecific,
      chronoEv + " (T1)"
    ),
    xr(
      "fn.time.month.monthofyear-alias",
      "MONTH",
      "MONTHOFYEAR",
      computedWordsAnchor,
      EsSpecific,
      chronoEv + "; one of the 9 underscore-stripped spellings greppable NOWHERE (F-2) (T1)"
    ),
    xr(
      "fn.time.day",
      "DAY",
      "DAY",
      """case object DAY_OF_MONTH extends Expr("DAY") with TimeField""",
      AnsiAdjacent,
      s"MySQL 8.4: DAY - $MyDt ; DuckDB: day(date) - $DkPart",
      "token OBJECT is DAY_OF_MONTH; its sql is DAY"
    ),
    xr(
      "fn.time.day.dayofmonth-alias",
      "DAY",
      "DAYOFMONTH",
      """override lazy val words: List[String] = List(sql, "DAYOFMONTH")""",
      AnsiAdjacent,
      s"MySQL 8.4: DAYOFMONTH - $MyDt ; DuckDB: dayofmonth - $DkPart",
      "explicit words override (T1)"
    ),
    xr(
      "fn.time.weekday",
      "WEEKDAY",
      "WEEKDAY",
      """case object DAY_OF_WEEK extends Expr("WEEKDAY") with TimeField""",
      EsSpecific,
      "ES painless (get(ChronoField.DAY_OF_WEEK) + 6) % 7, Monday = 0; matches MySQL WEEKDAY " +
      "but only that one trio engine documents the name",
      "token OBJECT is DAY_OF_WEEK; its sql is WEEKDAY"
    ),
    xr(
      "fn.time.weekday.dayofweek-alias",
      "WEEKDAY",
      "DAYOFWEEK",
      """override lazy val words: List[String] = List(sql, "DAYOFWEEK")""",
      EsSpecific,
      "ES painless weekday with Monday = 0. TRAP: MySQL DAYOFWEEK is 1 = Sunday and DuckDB " +
      "dayofweek is 0 = Sunday - same name, three different numberings (T1, T3)",
      "silent-semantics trap: the spelling matches, the numbers do not"
    ),
    xr(
      "fn.time.yearday",
      "YEARDAY",
      "YEARDAY",
      """case object DAY_OF_YEAR extends Expr("YEARDAY") with TimeField""",
      EsSpecific,
      "ES painless ChronoField.DAY_OF_YEAR; the YEARDAY spelling exists in no PD-3 trio engine",
      "token OBJECT is DAY_OF_YEAR; its sql is YEARDAY"
    ),
    xr(
      "fn.time.yearday.dayofyear-alias",
      "YEARDAY",
      "DAYOFYEAR",
      """override lazy val words: List[String] = List(sql, "DAYOFYEAR")""",
      AnsiAdjacent,
      s"MySQL 8.4: DAYOFYEAR - $MyDt ; DuckDB: dayofyear - $DkPart",
      "explicit words override (T1)"
    ),
    xr(
      "fn.time.hour",
      "HOUR",
      "HOUR",
      """case object HOUR_OF_DAY extends Expr("HOUR") with TimeField""",
      AnsiAdjacent,
      s"MySQL 8.4: HOUR - $MyDt ; DuckDB: hour - $DkPart",
      "token OBJECT is HOUR_OF_DAY; its sql is HOUR"
    ),
    xr(
      "fn.time.hour.hour-of-day-alias",
      "HOUR",
      "HOUR_OF_DAY",
      computedWordsAnchor,
      EsSpecific,
      chronoEv + " (T1)"
    ),
    xr(
      "fn.time.hour.hourofday-alias",
      "HOUR",
      "HOUROFDAY",
      computedWordsAnchor,
      EsSpecific,
      chronoEv + "; one of the 9 underscore-stripped spellings greppable NOWHERE (F-2) (T1)"
    ),
    xr(
      "fn.time.minute",
      "MINUTE",
      "MINUTE",
      """case object MINUTE_OF_HOUR extends Expr("MINUTE") with TimeField""",
      AnsiAdjacent,
      s"MySQL 8.4: MINUTE - $MyDt ; DuckDB: minute - $DkPart"
    ),
    xr(
      "fn.time.minute.minute-of-hour-alias",
      "MINUTE",
      "MINUTE_OF_HOUR",
      computedWordsAnchor,
      EsSpecific,
      chronoEv + " (T1)"
    ),
    xr(
      "fn.time.minute.minuteofhour-alias",
      "MINUTE",
      "MINUTEOFHOUR",
      computedWordsAnchor,
      EsSpecific,
      chronoEv + "; one of the 9 underscore-stripped spellings greppable NOWHERE (F-2) (T1)"
    ),
    xr(
      "fn.time.second",
      "SECOND",
      "SECOND",
      """case object SECOND_OF_MINUTE extends Expr("SECOND") with TimeField""",
      AnsiAdjacent,
      s"MySQL 8.4: SECOND - $MyDt ; DuckDB: second - $DkPart"
    ),
    xr(
      "fn.time.second.second-of-minute-alias",
      "SECOND",
      "SECOND_OF_MINUTE",
      computedWordsAnchor,
      EsSpecific,
      chronoEv + " (T1)"
    ),
    xr(
      "fn.time.second.secondofminute-alias",
      "SECOND",
      "SECONDOFMINUTE",
      computedWordsAnchor,
      EsSpecific,
      chronoEv + "; one of the 9 underscore-stripped spellings greppable NOWHERE (F-2) (T1)"
    ),
    xr(
      "fn.time.nanosecond",
      "NANOSECOND",
      "NANOSECOND",
      """case object NANO_OF_SECOND extends Expr("NANOSECOND") with TimeField""",
      EsSpecific,
      "ES painless ChronoField.NANO_OF_SECOND; NANOSECOND exists in no PD-3 trio engine " +
      "(MySQL stops at MICROSECOND)"
    ),
    xr(
      "fn.time.nanosecond.nano-of-second-alias",
      "NANOSECOND",
      "NANO_OF_SECOND",
      computedWordsAnchor,
      EsSpecific,
      chronoEv + " (T1)"
    ),
    xr(
      "fn.time.nanosecond.nanoofsecond-alias",
      "NANOSECOND",
      "NANOOFSECOND",
      computedWordsAnchor,
      EsSpecific,
      chronoEv + "; one of the 9 underscore-stripped spellings greppable NOWHERE (F-2) (T1)"
    ),
    xr(
      "fn.time.microsecond",
      "MICROSECOND",
      "MICROSECOND",
      """case object MICRO_OF_SECOND extends Expr("MICROSECOND") with TimeField""",
      AnsiAdjacent,
      s"MySQL 8.4: MICROSECOND - $MyDt ; DuckDB: microsecond - $DkPart"
    ),
    xr(
      "fn.time.microsecond.micro-of-second-alias",
      "MICROSECOND",
      "MICRO_OF_SECOND",
      computedWordsAnchor,
      EsSpecific,
      chronoEv + " (T1)"
    ),
    xr(
      "fn.time.microsecond.microofsecond-alias",
      "MICROSECOND",
      "MICROOFSECOND",
      computedWordsAnchor,
      EsSpecific,
      chronoEv + "; one of the 9 underscore-stripped spellings greppable NOWHERE (F-2) (T1)"
    ),
    xr(
      "fn.time.millisecond",
      "MILLISECOND",
      "MILLISECOND",
      """case object MILLI_OF_SECOND extends Expr("MILLISECOND") with TimeField""",
      EsSpecific,
      "ES painless ChronoField.MILLI_OF_SECOND; of the PD-3 trio only DuckDB documents " +
      "millisecond"
    ),
    xr(
      "fn.time.millisecond.milli-of-second-alias",
      "MILLISECOND",
      "MILLI_OF_SECOND",
      computedWordsAnchor,
      EsSpecific,
      chronoEv + " (T1)"
    ),
    xr(
      "fn.time.millisecond.milliofsecond-alias",
      "MILLISECOND",
      "MILLIOFSECOND",
      computedWordsAnchor,
      EsSpecific,
      chronoEv + "; one of the 9 underscore-stripped spellings greppable NOWHERE (F-2) (T1)"
    ),
    xr(
      "fn.time.epochday",
      "EPOCHDAY",
      "EPOCHDAY",
      """case object EPOCH_DAY extends Expr("EPOCHDAY") with TimeField""",
      EsSpecific,
      "ES painless ChronoField.EPOCH_DAY (days since 1970-01-01); the EPOCHDAY spelling " +
      "exists in no PD-3 trio engine",
      "token OBJECT is EPOCH_DAY; its sql is EPOCHDAY - its stripped computed form collides " +
      "with its own sql, which is why it yields only ONE alias (F-2)"
    ),
    xr(
      "fn.time.epochday.epoch-day-alias",
      "EPOCHDAY",
      "EPOCH_DAY",
      computedWordsAnchor,
      EsSpecific,
      chronoEv + " (T1)"
    ),
    xr(
      "fn.time.offset-seconds",
      "OFFSET_SECONDS",
      "OFFSET_SECONDS",
      """case object OFFSET_SECONDS extends Expr("OFFSET_SECONDS") with TimeField""",
      EsSpecific,
      "ES painless ChronoField.OFFSET_SECONDS (zone offset in seconds); exists in no PD-3 " +
      "trio engine"
    ),
    xr(
      "fn.time.offset-seconds.offsetseconds-alias",
      "OFFSET_SECONDS",
      "OFFSETSECONDS",
      computedWordsAnchor,
      EsSpecific,
      chronoEv + "; one of the 9 underscore-stripped spellings greppable NOWHERE (F-2) (T1)"
    ),
    xr(
      "fn.time.quarter",
      "QUARTER",
      "QUARTER",
      """case object QUARTER_OF_YEAR extends Expr("QUARTER") with IsoField""",
      AnsiAdjacent,
      s"MySQL 8.4: QUARTER - $MyDt ; DuckDB: quarter - $DkPart",
      "token OBJECT is QUARTER_OF_YEAR (IsoFields.QUARTER_OF_YEAR); its sql is QUARTER"
    ),
    xr(
      "fn.time.quarter.quarter-of-year-alias",
      "QUARTER",
      "QUARTER_OF_YEAR",
      computedWordsAnchor,
      EsSpecific,
      "ES painless java.time.temporal.IsoFields.QUARTER_OF_YEAR; runtime-computed spelling " +
      "(T1)"
    ),
    xr(
      "fn.time.quarter.quarterofyear-alias",
      "QUARTER",
      "QUARTEROFYEAR",
      computedWordsAnchor,
      EsSpecific,
      "ES painless IsoFields.QUARTER_OF_YEAR; one of the 9 underscore-stripped spellings " +
      "greppable NOWHERE (F-2) (T1)"
    ),
    xr(
      "fn.time.week",
      "WEEK",
      "WEEK",
      """case object WEEK_OF_WEEK_BASED_YEAR extends Expr("WEEK") with IsoField""",
      AnsiAdjacent,
      s"MySQL 8.4: WEEK(date) - $MyDt ; DuckDB: week - $DkPart",
      "ISO week-of-week-based-year here; MySQL WEEK's default mode 0 numbers differently (T3 " +
      "noted)"
    ),
    xr(
      "fn.time.week.weekofyear-alias",
      "WEEK",
      "WEEKOFYEAR",
      """override lazy val words: List[String] = List(sql, "WEEKOFYEAR")""",
      AnsiAdjacent,
      s"MySQL 8.4: WEEKOFYEAR - $MyDt ; DuckDB: weekofyear - $DkPart",
      "explicit words override; MySQL WEEKOFYEAR is ISO-equivalent (mode 3) (T1)"
    )
  )

  // ---- functions: geo (2 tokens) ----------------------------------------------------------
  val geo: List[CensusEntry] = List(
    e(
      "fn.geo.st-distance",
      Fn,
      "ST_DISTANCE",
      "ST_DISTANCE",
      FG,
      """case object Distance extends Expr("ST_DISTANCE") with Function with Operator""",
      "SELECT ST_DISTANCE(location, POINT(48.85, 2.35)) AS d FROM shops",
      "2",
      EsSpecific,
      "ES arcDistance / geo_distance query; result in meters. The OGC name is shared by " +
      "PostGIS (an extension, not PostgreSQL core), so no PD-3 trio engine documents it",
      PainlessField,
      "the Distance NODE declares args = Nil (arity comes from this constructor, not " +
      "args.size); also usable as a WHERE-side geo_distance criteria"
    ),
    e(
      "fn.geo.point.constructor",
      Fn,
      "POINT",
      "POINT",
      FG,
      """case object Point extends Expr("POINT") with TokenRegex""",
      "SELECT ST_DISTANCE(location, POINT(48.85, 2.35)) AS d FROM shops",
      "2",
      EsSpecific,
      "ES geo-point literal (lat, lon) feeding geo_distance / painless arcDistance params; " +
      "POINT only parses as an ST_DISTANCE argument",
      QueryClause,
      "unprojectable on its own: the example's projected chain belongs to ST_DISTANCE, so " +
      "this row is pinned (see DialectCensus.unprojectable)"
    ),
    e(
      "fn.geo.st-distance.distance-alias",
      Fn,
      "ST_DISTANCE",
      "DISTANCE",
      FG,
      """override def words: List[String] = List(sql, "DISTANCE")""",
      "SELECT DISTANCE(location, POINT(48.85, 2.35)) AS d FROM shops",
      "2",
      EsSpecific,
      "ES arcDistance / geo_distance; the bare DISTANCE spelling exists in no PD-3 trio " +
      "engine (T1)",
      PainlessField,
      "alias spelling of ST_DISTANCE"
    )
  )

  // ---- operators (comparison / predicates / logical / elastic / arithmetic / cast) --------
  val operators: List[CensusEntry] = List(
    e(
      "op.compare.eq",
      Op,
      "=",
      "=",
      OP,
      """case object EQ extends Expr("=") with ComparisonOperator""",
      "SELECT id FROM emp WHERE status = 'A'",
      "2",
      AnsiAdjacent,
      s"PostgreSQL 16: = - $PgCmp ; MySQL 8.4: = - $MyCmp",
      QueryClause,
      "term / match query depending on field type"
    ),
    e(
      "op.compare.ne",
      Op,
      "<>",
      "<>",
      OP,
      """case object NE extends Expr("<>") with ComparisonOperator""",
      "SELECT id FROM emp WHERE status <> 'A'",
      "2",
      AnsiAdjacent,
      s"PostgreSQL 16: <> - $PgCmp ; MySQL 8.4: <> - $MyCmp",
      QueryClause
    ),
    e(
      "op.compare.diff",
      Op,
      "!=",
      "!=",
      OP,
      """case object DIFF extends Expr("!=") with ComparisonOperator""",
      "SELECT id FROM emp WHERE status != 'A'",
      "2",
      AnsiAdjacent,
      s"PostgreSQL 16: != - $PgCmp ; MySQL 8.4: != - $MyCmp",
      QueryClause,
      "non-standard spelling of <>, documented by both cited engines"
    ),
    e(
      "op.compare.ge",
      Op,
      ">=",
      ">=",
      OP,
      """case object GE extends Expr(">=") with ComparisonOperator""",
      "SELECT id FROM emp WHERE age >= 30",
      "2",
      AnsiAdjacent,
      s"PostgreSQL 16: >= - $PgCmp ; MySQL 8.4: >= - $MyCmp",
      QueryClause,
      "range query gte"
    ),
    e(
      "op.compare.gt",
      Op,
      ">",
      ">",
      OP,
      """case object GT extends Expr(">") with ComparisonOperator""",
      "SELECT id FROM emp WHERE age > 30",
      "2",
      AnsiAdjacent,
      s"PostgreSQL 16: > - $PgCmp ; MySQL 8.4: > - $MyCmp",
      QueryClause
    ),
    e(
      "op.compare.le",
      Op,
      "<=",
      "<=",
      OP,
      """case object LE extends Expr("<=") with ComparisonOperator""",
      "SELECT id FROM emp WHERE age <= 60",
      "2",
      AnsiAdjacent,
      s"PostgreSQL 16: <= - $PgCmp ; MySQL 8.4: <= - $MyCmp",
      QueryClause
    ),
    e(
      "op.compare.lt",
      Op,
      "<",
      "<",
      OP,
      """case object LT extends Expr("<") with ComparisonOperator""",
      "SELECT id FROM emp WHERE age < 60",
      "2",
      AnsiAdjacent,
      s"PostgreSQL 16: < - $PgCmp ; MySQL 8.4: < - $MyCmp",
      QueryClause
    ),
    e(
      "op.predicate.in",
      Op,
      "IN",
      "IN",
      OP,
      """case object IN extends Expr("IN") with ComparisonOperator""",
      "SELECT id FROM emp WHERE status IN ('A', 'B')",
      "1..n",
      Ansi,
      "SQL:2016 Part 2 (Foundation) Feature E061-03 IN predicate with list of values",
      QueryClause,
      "terms query; literal, long and double lists each have their own production"
    ),
    e(
      "op.predicate.like",
      Op,
      "LIKE",
      "LIKE",
      OP,
      """case object LIKE extends Expr("LIKE") with ComparisonOperator""",
      "SELECT id FROM emp WHERE name LIKE 'Jo%'",
      "2",
      Ansi,
      "SQL:2016 Part 2 (Foundation) Feature E061-04 LIKE predicate",
      QueryClause,
      "wildcard query; % maps to *"
    ),
    e(
      "op.predicate.rlike",
      Op,
      "RLIKE",
      "RLIKE",
      OP,
      """case object RLIKE extends Expr("RLIKE") with ComparisonOperator""",
      "SELECT id FROM emp WHERE name RLIKE 'Jo.*'",
      "2",
      EsSpecific,
      "ES regexp query on keyword fields; RLIKE is MySQL's regexp-operator spelling, one " +
      "engine of the PD-3 trio",
      QueryClause
    ),
    e(
      "op.predicate.between",
      Op,
      "BETWEEN",
      "BETWEEN",
      OP,
      """case object BETWEEN extends Expr("BETWEEN") with ComparisonOperator""",
      "SELECT id FROM emp WHERE age BETWEEN 30 AND 40",
      "3",
      Ansi,
      "SQL:2016 Part 2 (Foundation) Feature E061-02 BETWEEN predicate",
      QueryClause,
      "range query gte/lte; literal, long, double, identifier and geo-distance bounds each " +
      "have their own production"
    ),
    e(
      "op.predicate.is-null",
      Op,
      "IS NULL",
      "IS NULL",
      OP,
      """case object IS_NULL extends Expr("IS NULL") with ComparisonOperator""",
      "SELECT id FROM emp WHERE manager_id IS NULL",
      "1",
      Ansi,
      "SQL:2016 Part 2 (Foundation) Feature E061-06 NULL predicate",
      QueryClause,
      "must_not exists query"
    ),
    e(
      "op.predicate.is-not-null",
      Op,
      "IS NOT NULL",
      "IS NOT NULL",
      OP,
      """case object IS_NOT_NULL extends Expr("IS NOT NULL") with ComparisonOperator""",
      "SELECT id FROM emp WHERE manager_id IS NOT NULL",
      "1",
      Ansi,
      "SQL:2016 Part 2 (Foundation) Feature E061-06 NULL predicate",
      QueryClause,
      "exists query"
    ),
    e(
      "op.fulltext.match-against",
      Op,
      "MATCH",
      "MATCH",
      OP,
      """case object MATCH extends Expr("MATCH") with ComparisonOperator""",
      "SELECT id FROM emp WHERE MATCH (name, title) AGAINST ('john')",
      "n+1",
      EsSpecific,
      "ES query DSL: multi_match / match. MySQL spells it MATCH ... AGAINST but with " +
      "full-text-index semantics, not Elasticsearch analyzer semantics (T3)",
      QueryClause,
      "two-token form; production parser/WhereParser.scala matchCriteria (rep1sep of columns, " +
      "then AGAINST ( literal )). Scoring is ES-side and has no relational analogue. MATCH is " +
      "ALSO a DDL statementWord (GEO_MATCH enrich policies) - different surface",
      Some(List("AGAINST"))
    ),
    e(
      "op.logical.not",
      Op,
      "NOT",
      "NOT",
      OP,
      """case object NOT extends Expr("NOT") with LogicalOperator""",
      "SELECT id FROM emp WHERE NOT status = 'X'",
      "1",
      AnsiAdjacent,
      s"PostgreSQL 16: NOT - $PgLogic ; MySQL 8.4: NOT - $MyLogic",
      QueryClause,
      "bool must_not"
    ),
    e(
      "op.logical.and",
      Op,
      "AND",
      "AND",
      OP,
      """case object AND extends Expr("AND") with PredicateOperator""",
      "SELECT id FROM emp WHERE age >= 30 AND status = 'A'",
      "2",
      AnsiAdjacent,
      s"PostgreSQL 16: AND - $PgLogic ; MySQL 8.4: AND - $MyLogic",
      QueryClause,
      "bool must/filter"
    ),
    e(
      "op.logical.or",
      Op,
      "OR",
      "OR",
      OP,
      """case object OR extends Expr("OR") with PredicateOperator""",
      "SELECT id FROM emp WHERE age >= 60 OR status = 'R'",
      "2",
      AnsiAdjacent,
      s"PostgreSQL 16: OR - $PgLogic ; MySQL 8.4: OR - $MyLogic",
      QueryClause,
      "bool should"
    ),
    e(
      "op.elastic.nested",
      Op,
      "NESTED",
      "NESTED",
      OP,
      """case object Nested extends Expr("NESTED") with ElasticOperator""",
      "SELECT id FROM products WHERE NESTED(comments.rating >= 4)",
      "1",
      EsSpecific,
      "ES nested query scoring relation over nested documents",
      QueryClause,
      "no relational analogue; jOOQ has no model for it"
    ),
    e(
      "op.elastic.child",
      Op,
      "CHILD",
      "CHILD",
      OP,
      """case object Child extends Expr("CHILD") with ElasticOperator""",
      "SELECT id FROM docs WHERE CHILD(status = 'published')",
      "1",
      EsSpecific,
      "ES has_child query over a join field",
      QueryClause
    ),
    e(
      "op.elastic.parent",
      Op,
      "PARENT",
      "PARENT",
      OP,
      """case object Parent extends Expr("PARENT") with ElasticOperator""",
      "SELECT id FROM docs WHERE PARENT(category = 'news')",
      "1",
      EsSpecific,
      "ES has_parent query over a join field",
      QueryClause
    ),
    e(
      "op.math.add",
      Op,
      "+",
      "+",
      OM,
      """case object ADD extends Expr("+") with ArithmeticOperator""",
      "SELECT price + 10 AS p FROM sales",
      "2",
      AnsiAdjacent,
      s"PostgreSQL 16: + - $PgMath ; DuckDB: + - $DkNum",
      PainlessField
    ),
    e(
      "op.math.subtract",
      Op,
      "-",
      "-",
      OM,
      """case object SUBTRACT extends Expr("-") with ArithmeticOperator""",
      "SELECT price - 10 AS p FROM sales",
      "2",
      AnsiAdjacent,
      s"PostgreSQL 16: - operator - $PgMath ; DuckDB: - operator - $DkNum",
      PainlessField
    ),
    e(
      "op.math.multiply",
      Op,
      "*",
      "*",
      OM,
      """case object MULTIPLY extends Expr("*") with ArithmeticOperator""",
      "SELECT salary * 12 AS annual FROM emp",
      "2",
      AnsiAdjacent,
      s"PostgreSQL 16: * - $PgMath ; DuckDB: * - $DkNum",
      PainlessField
    ),
    e(
      "op.math.divide",
      Op,
      "/",
      "/",
      OM,
      """case object DIVIDE extends Expr("/") with ArithmeticOperator""",
      "SELECT price / 2 AS half FROM sales",
      "2",
      AnsiAdjacent,
      s"PostgreSQL 16: / - $PgMath ; DuckDB: / - $DkNum",
      PainlessField
    ),
    e(
      "op.math.modulo",
      Op,
      "%",
      "%",
      OM,
      """case object MODULO extends Expr("%") with ArithmeticOperator""",
      "SELECT qty % 2 AS m FROM sales",
      "2",
      AnsiAdjacent,
      s"PostgreSQL 16: % (modulo) - $PgMath ; DuckDB: % (modulo) - $DkNum",
      PainlessField,
      "the operator form; there is no MOD() function in this dialect (its help JSON is a " +
      "recorded phantom)"
    ),
    e(
      "op.time.interval-plus",
      Op,
      "+",
      "+",
      OT,
      """case object PLUS extends Expr("+") with IntervalOperator""",
      "SELECT created_at + INTERVAL 1 DAY AS next_day FROM events",
      "2",
      Ansi,
      "SQL:2016 Part 2 (Foundation) Feature F052 Intervals and datetime arithmetic",
      PainlessField,
      "temporal + INTERVAL form; pushes down to ES date math where the chain allows"
    ),
    e(
      "op.time.interval-minus",
      Op,
      "-",
      "-",
      OT,
      """case object MINUS extends Expr("-") with IntervalOperator""",
      "SELECT created_at - INTERVAL 7 DAY AS prev_week FROM events",
      "2",
      Ansi,
      "SQL:2016 Part 2 (Foundation) Feature F052 Intervals and datetime arithmetic",
      PainlessField
    ),
    e(
      "op.convert.cast-operator",
      Op,
      "::",
      "::",
      FV,
      """case object CastOperator extends Expr("\\:\\:") with TokenRegex""",
      "SELECT age::BIGINT AS a FROM emp",
      "2",
      AnsiAdjacent,
      s"PostgreSQL 16: expression::type - $PgExpr ; DuckDB: expr::TYPENAME - $DkCast",
      PainlessField,
      "postfix cast; the token's sql literal is the ESCAPED regex source, the accepted " +
      "spelling is ::"
    ),
    e(
      "op.string.pipe-concat",
      Op,
      "||",
      "||",
      FS,
      """case object Pipe extends Expr("\\|\\|") with StringOp""",
      "SELECT CONCAT(first_name, last_name) AS full_name FROM emp",
      "2",
      Ansi,
      "SQL:2016 Part 2 (Foundation) Feature E021-07 Character concatenation (||)",
      PainlessField,
      "DEAD TOKEN (19.1 finding): the Pipe token is declared but NO parser production consumes " +
      "it - 'a || b' is REJECTED at this baseline (verified: zero Pipe references under " +
      "sql/src/main/scala/.../parser). The example shows the accepted CONCAT equivalent; a " +
      "jOOQ-rendered || concatenation cannot round-trip (19.2 input)"
    )
  )

  // ---- clauses (walked from SQLKeywords.clauseTokens - Task 5's diff enforces coverage) ----
  val clauses: List[CensusEntry] = List(
    e(
      "clause.select.projection",
      Clause,
      "SELECT",
      "SELECT",
      QS,
      """case object Select extends Expr("SELECT") with TokenRegex""",
      "SELECT id FROM emp",
      "n",
      AnsiAdjacent,
      s"PostgreSQL 16: SELECT - $PgSel ; MySQL 8.4: SELECT - $MySel",
      RequestShape,
      "projection drives _source / docvalue_fields"
    ),
    e(
      "clause.select.distinct",
      Clause,
      "DISTINCT",
      "DISTINCT",
      RP,
      """case object Distinct extends Expr("DISTINCT") with TokenRegex""",
      "SELECT COUNT(DISTINCT city) AS c FROM emp",
      "1",
      AnsiAdjacent,
      s"PostgreSQL 16: SELECT DISTINCT - $PgSel ; MySQL 8.4: DISTINCT - $MySel",
      NativeAgg,
      "COUNT(DISTINCT x) becomes a cardinality aggregation (approximate above the precision " +
      "threshold - a T3 semantics note for 19.3)"
    ),
    e(
      "clause.select.star-except",
      Clause,
      "EXCEPT",
      "EXCEPT",
      QS,
      """case object Except extends Expr("except") with TokenRegex""",
      "SELECT * EXCEPT(salary, ssn) FROM emp",
      "n",
      EsSpecific,
      "Elasticsearch request shaping: the excluded columns never enter _source / " +
      "docvalue_fields. BigQuery and DuckDB spell projection-exclusion differently and this " +
      "is NOT the ANSI EXCEPT set operator - that operator does not exist in this dialect",
      RequestShape,
      "AMBIGUITY TRAP: EXCEPT here is projection exclusion, not a set operator. The token's " +
      "sql is LOWERCASE (\"except\") - normalise before comparing"
    ),
    e(
      "clause.select.as-alias",
      Clause,
      "AS",
      "AS",
      RP,
      """case object Alias extends Expr("AS") with TokenRegex""",
      "SELECT e.name AS n FROM emp AS e",
      "1",
      Ansi,
      "SQL:2016 Part 2 (Foundation) Feature E051-08 Correlation names in the FROM clause",
      RequestShape,
      "AS is optional in both column and table positions; aliases drive response column names"
    ),
    e(
      "clause.from.table",
      Clause,
      "FROM",
      "FROM",
      QF,
      """case object From extends Expr("FROM") with TokenRegex""",
      "SELECT id FROM emp",
      "1..n",
      AnsiAdjacent,
      s"PostgreSQL 16: FROM - $PgSel ; MySQL 8.4: FROM - $MySel",
      RequestShape,
      "table name = index/alias/pattern; a quoted schema prefix is accepted and ignored"
    ),
    e(
      "clause.where.filter",
      Clause,
      "WHERE",
      "WHERE",
      QW,
      """case object Where extends Expr("WHERE") with TokenRegex""",
      "SELECT id FROM emp WHERE age >= 30",
      "1",
      AnsiAdjacent,
      s"PostgreSQL 16: WHERE - $PgSel ; MySQL 8.4: WHERE - $MySel",
      QueryClause,
      "criteria tree renders to the ES bool query DSL"
    ),
    e(
      "clause.groupby",
      Clause,
      "GROUP BY",
      "GROUP BY",
      QG,
      """case object GroupBy extends Expr("GROUP BY") with TokenRegex""",
      "SELECT country, COUNT(*) AS c FROM emp GROUP BY country",
      "1..n",
      Ansi,
      "SQL:2016 Part 2 (Foundation) Feature E051-02 GROUP BY clause",
      NativeAgg,
      "terms bucket aggregation; without LIMIT the bucket size defaults to 65536 (issue #205)"
    ),
    e(
      "clause.groupby.ordinal",
      Clause,
      "GROUP BY",
      "GROUP BY",
      QG,
      """case object GroupBy extends Expr("GROUP BY") with TokenRegex""",
      "SELECT country, COUNT(*) AS c FROM emp GROUP BY 1",
      "1..n",
      EsSpecific,
      "ES terms bucket keyed by the projected expression the ordinal resolves to; of the " +
      "fetched PD-3 pages only MySQL's SELECT syntax documents GROUP BY position (T2)",
      NativeAgg,
      "ordinal variant; the bucket production accepts a bare long"
    ),
    e(
      "clause.having",
      Clause,
      "HAVING",
      "HAVING",
      QH,
      """case object Having extends Expr("HAVING") with TokenRegex""",
      "SELECT country, COUNT(*) AS c FROM emp GROUP BY country HAVING COUNT(*) > 10",
      "1",
      Ansi,
      "SQL:2016 Part 2 (Foundation) Feature E051-06 HAVING clause",
      BucketScript,
      "renders to an ES bucket_selector pipeline aggregation filtering buckets"
    ),
    e(
      "clause.orderby",
      Clause,
      "ORDER BY",
      "ORDER BY",
      QO,
      """case object OrderBy extends Expr("ORDER BY") with TokenRegex""",
      "SELECT id FROM emp ORDER BY name",
      "1..n",
      AnsiAdjacent,
      s"PostgreSQL 16: ORDER BY - $PgOrd ; MySQL 8.4: ORDER BY - $MySel",
      SortClause
    ),
    e(
      "clause.orderby.asc",
      Clause,
      "ASC",
      "ASC",
      QO,
      """case object Asc extends Expr("ASC") with SortOrder""",
      "SELECT id FROM emp ORDER BY name ASC",
      "1",
      AnsiAdjacent,
      s"PostgreSQL 16: ASC - $PgOrd ; MySQL 8.4: ASC - $MySel",
      SortClause
    ),
    e(
      "clause.orderby.desc",
      Clause,
      "DESC",
      "DESC",
      QO,
      """case object Desc extends Expr("DESC") with SortOrder""",
      "SELECT id FROM emp ORDER BY name DESC",
      "1",
      AnsiAdjacent,
      s"PostgreSQL 16: DESC - $PgOrd ; MySQL 8.4: DESC - $MySel",
      SortClause
    ),
    e(
      "clause.orderby.nulls-first",
      Clause,
      "NULLS FIRST",
      "NULLS FIRST",
      QO,
      """case object NullsFirst extends Expr("NULLS FIRST") with NullOrdering""",
      "SELECT id FROM emp ORDER BY name NULLS FIRST",
      "1",
      AnsiAdjacent,
      s"PostgreSQL 16: NULLS FIRST - $PgOrd ; DuckDB: NULLS FIRST - $DkOrd",
      SortClause,
      "maps to ES sort.missing = _first; rejected on aggregation/GROUP BY sorts (issue #99)"
    ),
    e(
      "clause.orderby.nulls-last",
      Clause,
      "NULLS LAST",
      "NULLS LAST",
      QO,
      """case object NullsLast extends Expr("NULLS LAST") with NullOrdering""",
      "SELECT id FROM emp ORDER BY name NULLS LAST",
      "1",
      AnsiAdjacent,
      s"PostgreSQL 16: NULLS LAST - $PgOrd ; DuckDB: NULLS LAST - $DkOrd",
      SortClause,
      "maps to ES sort.missing = _last"
    ),
    e(
      "clause.limit",
      Clause,
      "LIMIT",
      "LIMIT",
      QL,
      """case object Limit extends Expr("LIMIT") with TokenRegex""",
      "SELECT id FROM emp LIMIT 10",
      "1",
      AnsiAdjacent,
      s"PostgreSQL 16: LIMIT - $PgLim ; MySQL 8.4: LIMIT - $MySel",
      RequestShape,
      "maps to size; the standard's FETCH FIRST spelling does not parse here. Above " +
      "max_result_window the engine routes through bounded scroll paging (issue #224)"
    ),
    e(
      "clause.limit.offset",
      Clause,
      "OFFSET",
      "OFFSET",
      QL,
      """case object Offset extends Expr("OFFSET") with TokenRegex""",
      "SELECT id FROM emp LIMIT 10 OFFSET 5",
      "1",
      AnsiAdjacent,
      s"PostgreSQL 16: OFFSET - $PgLim ; MySQL 8.4: LIMIT ... OFFSET - $MySel",
      RequestShape,
      "maps to from; only valid after LIMIT in this grammar"
    ),
    e(
      "clause.union-all",
      Clause,
      "UNION ALL",
      "UNION ALL",
      OP,
      """case object UNION extends Expr("UNION ALL") with Operator with TokenRegex""",
      "SELECT id FROM emp UNION ALL SELECT id FROM contractors",
      "2..n",
      Ansi,
      "SQL:2016 Part 2 (Foundation) Feature E071-02 UNION ALL table operator",
      RequestShape,
      "msearch multi-request; the token is the literal two-word UNION ALL - bare UNION (with " +
      "duplicate elimination) does NOT exist in this dialect"
    ),
    e(
      "clause.join.bare",
      Clause,
      "JOIN",
      "JOIN",
      QF,
      """case object Join extends Expr("JOIN") with TokenRegex""",
      "SELECT e.id FROM emp e JOIN dept d ON e.dept_id = d.id",
      "2",
      Ansi,
      "SQL:2016 Part 2 (Foundation) Feature F041-01 Inner join (but not necessarily the " +
      "INNER keyword)",
      ClientSide,
      "cross-index JOINs execute in a join-capable extension (DuckDB), never in ES; without " +
      "one the core engine cannot combine the legs (issue #157)"
    ),
    e(
      "clause.join.on",
      Clause,
      "ON",
      "ON",
      QF,
      """case object On extends Expr("ON") with TokenRegex""",
      "SELECT e.id FROM emp e JOIN dept d ON e.dept_id = d.id",
      "1",
      Ansi,
      "SQL:2016 Part 2 (Foundation) Feature F041-01 Inner join (join condition)",
      ClientSide,
      "equality-and-AND-only criteria; functions and NOT are rejected in ON"
    ),
    e(
      "clause.join.inner",
      Clause,
      "INNER",
      "INNER",
      QF,
      """case object InnerJoin extends Expr("INNER") with JoinType""",
      "SELECT e.id FROM emp e INNER JOIN dept d ON e.dept_id = d.id",
      "2",
      Ansi,
      "SQL:2016 Part 2 (Foundation) Feature F041-02 INNER keyword",
      ClientSide
    ),
    e(
      "clause.join.left",
      Clause,
      "LEFT",
      "LEFT",
      QF,
      """case object LeftJoin extends Expr("LEFT") with JoinType""",
      "SELECT e.id FROM emp e LEFT JOIN dept d ON e.dept_id = d.id",
      "2",
      Ansi,
      "SQL:2016 Part 2 (Foundation) Feature F041-03 LEFT OUTER JOIN (OUTER optional)",
      ClientSide,
      "the LEFT token's sql collides with the string function LeftOp - a cross-category " +
      "collision the coverage diff must survive"
    ),
    e(
      "clause.join.left-outer",
      Clause,
      "LEFT",
      "LEFT OUTER",
      QF,
      """override def words: List[String] = List("LEFT\\s+OUTER", "LEFT")""",
      "SELECT e.id FROM emp e LEFT OUTER JOIN dept d ON e.dept_id = d.id",
      "2",
      Ansi,
      "SQL:2016 Part 2 (Foundation) Feature F041-03 LEFT OUTER JOIN",
      ClientSide,
      "the only words overrides outside the function packages (T2 keyword variant)"
    ),
    e(
      "clause.join.right",
      Clause,
      "RIGHT",
      "RIGHT",
      QF,
      """case object RightJoin extends Expr("RIGHT") with JoinType""",
      "SELECT e.id FROM emp e RIGHT JOIN dept d ON e.dept_id = d.id",
      "2",
      Ansi,
      "SQL:2016 Part 2 (Foundation) Feature F041-04 RIGHT OUTER JOIN (OUTER optional)",
      ClientSide
    ),
    e(
      "clause.join.right-outer",
      Clause,
      "RIGHT",
      "RIGHT OUTER",
      QF,
      """override def words: List[String] = List("RIGHT\\s+OUTER", "RIGHT")""",
      "SELECT e.id FROM emp e RIGHT OUTER JOIN dept d ON e.dept_id = d.id",
      "2",
      Ansi,
      "SQL:2016 Part 2 (Foundation) Feature F041-04 RIGHT OUTER JOIN",
      ClientSide
    ),
    e(
      "clause.join.full",
      Clause,
      "FULL",
      "FULL",
      QF,
      """case object FullJoin extends Expr("FULL") with JoinType""",
      "SELECT e.id FROM emp e FULL JOIN dept d ON e.dept_id = d.id",
      "2",
      Ansi,
      "SQL:2016 Part 2 (Foundation) Feature F406 FULL OUTER JOIN (OUTER optional)",
      ClientSide,
      "MySQL has no FULL OUTER JOIN at all - a 19.4 corpus note"
    ),
    e(
      "clause.join.full-outer",
      Clause,
      "FULL",
      "FULL OUTER",
      QF,
      """override def words: List[String] = List("FULL\\s+OUTER", "FULL")""",
      "SELECT e.id FROM emp e FULL OUTER JOIN dept d ON e.dept_id = d.id",
      "2",
      Ansi,
      "SQL:2016 Part 2 (Foundation) Feature F406 FULL OUTER JOIN",
      ClientSide
    ),
    e(
      "clause.join.cross",
      Clause,
      "CROSS",
      "CROSS",
      QF,
      """case object CrossJoin extends Expr("CROSS") with JoinType""",
      "SELECT e.id FROM emp e CROSS JOIN dept d ON e.dept_id = d.id",
      "2",
      Ansi,
      "SQL:2016 Part 2 (Foundation) Feature F407 CROSS JOIN",
      ClientSide,
      "19.1 FINDING: a bare CROSS JOIN with no ON is accepted by the grammar but rejected by " +
      "StandardJoin.validate (which demands an ON unconditionally), so Join.validate's " +
      "CrossJoin exemption is unreachable - the accepted FORM requires the ON shown here"
    ),
    e(
      "clause.join.unnest",
      Clause,
      "UNNEST",
      "UNNEST",
      QF,
      """case object Unnest extends Expr("UNNEST") with TokenRegex""",
      "SELECT p.id FROM products p JOIN UNNEST(p.comments) AS c",
      "1",
      EsSpecific,
      "ES nested query + inner_hits over an array-of-struct field; the JOIN UNNEST spelling " +
      "is BigQuery's, in no PD-3 trio engine",
      QueryClause,
      "the UNNEST identifier must be a nested field (table.column); no join type or ON allowed"
    ),
    e(
      "clause.case.searched",
      Clause,
      "CASE",
      "CASE",
      FC,
      """case object Case extends Expr("CASE") with ConditionalOp""",
      "SELECT CASE WHEN age >= 18 THEN 'adult' ELSE 'minor' END AS cat FROM emp",
      "1..n",
      Ansi,
      "SQL:2016 Part 2 (Foundation) Feature F261-02 CASE expression (searched form)",
      PainlessField,
      "WHEN/THEN/ELSE/END are separate clauseTokens carried by this row's aliases",
      Some(List("WHEN", "THEN", "ELSE", "END"))
    ),
    e(
      "clause.case.simple",
      Clause,
      "CASE",
      "CASE",
      s"$S/parser/function/cond/package.scala",
      "def case_when: PackratParser[Case] =",
      "SELECT CASE status WHEN 'A' THEN 1 ELSE 0 END AS s FROM emp",
      "1..n",
      Ansi,
      "SQL:2016 Part 2 (Foundation) Feature F261-01 CASE expression (simple form)",
      PainlessField,
      "same production as the searched form, with a leading comparand expression (T2)"
    ),
    e(
      "clause.window.over",
      Clause,
      "OVER",
      "OVER",
      FA,
      """case object OVER extends Expr("OVER") with TokenRegex""",
      "SELECT MAX(salary) OVER (PARTITION BY dept) AS m FROM emp",
      "1",
      Ansi,
      "SQL:2016 Part 2 (Foundation) Feature T611 Elementary OLAP operations (window OVER " +
      "clause)",
      NativeAgg,
      "windowed aggregates execute as partition buckets + metric/top_hits"
    ),
    e(
      "clause.window.partition-by",
      Clause,
      "PARTITION BY",
      "PARTITION BY",
      FA,
      """case object PARTITION_BY extends Expr("PARTITION BY") with TokenRegex""",
      "SELECT SUM(salary) OVER (PARTITION BY dept) AS t FROM emp",
      "1..n",
      Ansi,
      "SQL:2016 Part 2 (Foundation) Feature T611 Elementary OLAP operations (PARTITION BY)",
      NativeAgg,
      "partition terms buckets sized Bucket.DefaultSize = 65536 (issue #207)"
    ),
    e(
      "clause.window.over-limit",
      Clause,
      "OVER",
      "OVER",
      PFa,
      "private[this] def ranking_over: Parser[(Seq[Identifier], OrderBy, Option[Limit])] =",
      "SELECT ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC LIMIT 3) AS rn FROM emp",
      "1..3",
      EsSpecific,
      "ES top-N push-down: the inline LIMIT bounds each partition's top_hits; no PD-3 trio " +
      "engine accepts LIMIT inside an OVER clause (issue #101)",
      NativeAgg,
      "elasticsql-specific OVER extension; the outer statement LIMIT never bounds partitions"
    ),
    e(
      "clause.interval",
      Clause,
      "INTERVAL",
      "INTERVAL",
      TP,
      """case object Interval extends Expr("INTERVAL") with TokenRegex""",
      "SELECT created_at + INTERVAL 2 DAY AS d FROM events",
      "2",
      Ansi,
      "SQL:2016 Part 2 (Foundation) Feature F052 Intervals and datetime arithmetic",
      PainlessField,
      "INTERVAL <int> <unit>; units also accept a plural s suffix (see the unit rows)"
    ),
    e(
      "clause.cast.target-type",
      Clause,
      "BIGINT",
      "BIGINT",
      s"$S/parser/type/package.scala",
      "def sql_type: PackratParser[SQLType] =",
      "SELECT CAST(age AS BIGINT) AS a FROM emp",
      "1",
      EsSpecific,
      "ES mapping types reachable through CAST/CONVERT/:: - the sql_type production accepts 27 " +
      "curated names (SQLKeywords.typeWords); BIGINT stands for the surface (AC-3)",
      PainlessField,
      "representative row for the SQL type-name surface; the full list is " +
      "SQLKeywords.typeWords (ARRAY BIGINT BINARY BOOLEAN BYTE CHAR DATE DATETIME DOUBLE FLOAT " +
      "GEOPOINT GEO_POINT INT INTEGER KEYWORD LONG REAL SHORT SMALLINT STRING STRUCT TEXT TIME " +
      "TIMESTAMP TINYINT VARBINARY VARCHAR)"
    ),
    e(
      "clause.for",
      Clause,
      "FOR",
      "FOR",
      FS,
      """case object For extends Expr("FOR") with TokenRegex""",
      "SELECT SUBSTRING(name FROM 2 FOR 3) AS c FROM emp",
      "1",
      Ansi,
      "SQL:2016 Part 2 (Foundation) Feature E021-06 SUBSTRING function (FROM ... FOR ...)",
      PainlessField,
      "keyword of the SUBSTRING/LEFT/RIGHT keyword forms; not a loop construct"
    )
  )

  // ---- literals (PD-5: tokens that are NOT functions) -------------------------------------
  val literals: List[CensusEntry] = List(
    e(
      "lit.pi",
      Lit,
      "PI",
      "PI",
      RP,
      """case object PiValue extends Value[Double](Math.PI) with TokenRegex""",
      "SELECT id FROM emp WHERE score > PI",
      "0",
      EsSpecific,
      "Elasticsearch painless literal Math.PI; a BARE WORD - SELECT PI, never PI(). jOOQ " +
      "models PI() as a function, a guaranteed render mismatch for 19.2",
      QueryClause,
      "ambiguity trap: a renderer reading PI as a column reference corrupts silently"
    ),
    e(
      "lit.random",
      Lit,
      "RANDOM",
      "RANDOM",
      RP,
      """case object RandomValue extends Value[Double](Math.random()) with TokenRegex""",
      "SELECT id FROM emp WHERE score >= RANDOM",
      "0",
      EsSpecific,
      "Elasticsearch painless Math.random(); bare-word literal like PI, evaluated once per " +
      "token object",
      QueryClause
    ),
    e(
      "lit.e",
      Lit,
      "E",
      "E",
      RP,
      """case object EValue extends Value[Double](Math.E) with TokenRegex""",
      "SELECT EXP(1) AS euler FROM emp",
      "0",
      EsSpecific,
      "Elasticsearch painless Math.E constant. DEAD TOKEN (19.1 finding): declared and " +
      "registered in SQLKeywords.literalTokens but NO parser production consumes it - a bare " +
      "E never parses as this literal. The example shows the accepted EXP(1) equivalent",
      PainlessField,
      "kept in the census because the registry word E must be accounted for (Task 5 diff)"
    ),
    e(
      "lit.null",
      Lit,
      "NULL",
      "NULL",
      s"$S/parser/type/package.scala",
      "def nullValue: PackratParser[Null.type] =",
      "SELECT COALESCE(nickname, NULL) AS n FROM emp",
      "0",
      EsSpecific,
      "Elasticsearch missing-value semantics; painless null literal in value positions",
      PainlessField,
      "NULL is also a DDL statementWord (DEFAULT NULL) - different surface"
    ),
    e(
      "lit.true",
      Lit,
      "TRUE",
      "TRUE",
      s"$S/parser/type/package.scala",
      "def boolean: PackratParser[BooleanValue] =",
      "SELECT id FROM emp WHERE active = TRUE",
      "0",
      AnsiAdjacent,
      s"PostgreSQL 16: TRUE boolean constant - $PgBool ; MySQL 8.4: TRUE literal - $MyBool",
      QueryClause,
      "term query on a boolean field; parsed by TypeParser.boolean, not a TokenRegex"
    ),
    e(
      "lit.false",
      Lit,
      "FALSE",
      "FALSE",
      s"$S/parser/type/package.scala",
      "def boolean: PackratParser[BooleanValue] =",
      "SELECT id FROM emp WHERE active = FALSE",
      "0",
      AnsiAdjacent,
      s"PostgreSQL 16: FALSE boolean constant - $PgBool ; MySQL 8.4: FALSE literal - $MyBool",
      QueryClause
    )
  )

  // ---- units (PD-5: geo distance units + plural time units) -------------------------------
  private def geoUnit(id: String, unit: String, anchor: String, extra: String = ""): CensusEntry =
    e(
      id,
      Uom,
      unit,
      unit,
      FG,
      anchor,
      s"SELECT id FROM shops WHERE ST_DISTANCE(location, POINT(48.85, 2.35)) <= 5 $unit",
      "0",
      EsSpecific,
      "ES geo_distance unit string appended to the distance literal",
      QueryClause,
      if (extra.isEmpty) "deliberately excluded from SQLKeywords (word-shaped, REPL.1 PD)"
      else extra
    )

  private def timeUnit(
    id: String,
    singular: String,
    plural: String,
    anchor: String
  ): CensusEntry =
    e(
      id,
      Uom,
      singular,
      plural,
      TP,
      anchor,
      s"SELECT created_at + INTERVAL 2 $plural AS d FROM events",
      "0",
      EsSpecific,
      "ES date math unit; the plural form is accepted by the TimeUnit regex (an optional s " +
      "suffix) and carried by NO token words - SQLKeywords.timeUnitPluralWords is the curated " +
      "source",
      PainlessField,
      s"plural spelling of the $singular unit; the singular word is covered by the temporal " +
      "extractor tokens"
    )

  val units: List[CensusEntry] = List(
    geoUnit("uom.geo.km", "km", """case object Kilometers extends Expr("km") with MetricUnit"""),
    geoUnit("uom.geo.m", "m", """case object Meters extends Expr("m") with MetricUnit"""),
    geoUnit("uom.geo.cm", "cm", """case object Centimeters extends Expr("cm") with MetricUnit"""),
    geoUnit("uom.geo.mm", "mm", """case object Millimeters extends Expr("mm") with MetricUnit"""),
    geoUnit("uom.geo.mi", "mi", """case object Miles extends Expr("mi") with ImperialUnit"""),
    geoUnit("uom.geo.yd", "yd", """case object Yards extends Expr("yd") with ImperialUnit"""),
    geoUnit("uom.geo.ft", "ft", """case object Feet extends Expr("ft") with ImperialUnit"""),
    geoUnit(
      "uom.geo.in",
      "in",
      """case object Inches extends Expr("in") with ImperialUnit""",
      "the unit word collides with the IN operator spelling - the registry carries IN via " +
      "operator.IN, which is why this is the one unit absent from nonRegistryWords"
    ),
    geoUnit(
      "uom.geo.nmi",
      "nmi",
      """case object NauticalMiles extends Expr("nmi") with DistanceUnit"""
    ),
    timeUnit(
      "uom.time.years",
      "YEAR",
      "YEARS",
      """case object YEARS extends Expr("YEAR") with CalendarUnit"""
    ),
    timeUnit(
      "uom.time.months",
      "MONTH",
      "MONTHS",
      """case object MONTHS extends Expr("MONTH") with CalendarUnit"""
    ),
    timeUnit(
      "uom.time.quarters",
      "QUARTER",
      "QUARTERS",
      """case object QUARTERS extends Expr("QUARTER") with CalendarUnit"""
    ),
    timeUnit(
      "uom.time.weeks",
      "WEEK",
      "WEEKS",
      """case object WEEKS extends Expr("WEEK") with CalendarUnit"""
    ),
    timeUnit(
      "uom.time.days",
      "DAY",
      "DAYS",
      """case object DAYS extends Expr("DAY") with CalendarUnit with FixedUnit"""
    ),
    timeUnit(
      "uom.time.hours",
      "HOUR",
      "HOURS",
      """case object HOURS extends Expr("HOUR") with FixedUnit"""
    ),
    timeUnit(
      "uom.time.minutes",
      "MINUTE",
      "MINUTES",
      """case object MINUTES extends Expr("MINUTE") with FixedUnit"""
    ),
    timeUnit(
      "uom.time.seconds",
      "SECOND",
      "SECONDS",
      """case object SECONDS extends Expr("SECOND") with FixedUnit"""
    )
  )

  val entries: List[CensusEntry] =
    aggregate ++ aggregateWindows ++ cond ++ convert ++ math ++ string ++
    timeCurrent ++ timeArith ++ extractors ++ geo ++ operators ++ clauses ++ literals ++ units

  /** Phantom help JSON - functions the engine does NOT implement (PD-4, AC-2). NOT census rows.
    * Path -> why it is phantom. All thirteen files (and json/_index.json) are UNTRACKED.
    */
  val phantomHelpFiles: List[(String, String)] = List(
    "core/src/main/resources/help/functions/conditional/if.json"
    -> ("no IF token in function/cond; the SQLKeywords \"IF\" hit is the DDL IF NOT EXISTS " +
    "statementWord"),
    "core/src/main/resources/help/functions/conditional/ifnull.json" -> "no IFNULL token",
    "core/src/main/resources/help/functions/conditional/nvl.json"    -> "no NVL token",
    "core/src/main/resources/help/functions/numeric/ln.json"
    -> "no LN token (LOG and LOG10 exist; LOG is the natural log here)",
    "core/src/main/resources/help/functions/numeric/mod.json"
    -> "no MOD token (the % operator exists)",
    "core/src/main/resources/help/functions/numeric/truncate.json"
    -> ("no TRUNCATE function; the SQLKeywords \"TRUNCATE\" hit is the TRUNCATE TABLE " +
    "statementWord"),
    "core/src/main/resources/help/functions/string/lpad.json"  -> "no LPAD token",
    "core/src/main/resources/help/functions/string/rpad.json"  -> "no RPAD token",
    "core/src/main/resources/help/functions/string/space.json" -> "no SPACE token",
    "core/src/main/resources/help/functions/string/regexp_replace.json"
    -> "no REGEXP_REPLACE token (REGEXP_LIKE/REGEXP exists)",
    // NEW at this baseline - and, unlike the ten above, WIRED into json/_index.json
    "core/src/main/resources/help/functions/json/json_extract.json"
    -> "zero JSON_EXTRACT hits in sql/src/main",
    "core/src/main/resources/help/functions/json/json_object.json"
    -> "zero JSON_OBJECT hits in sql/src/main",
    "core/src/main/resources/help/functions/json/json_array.json"
    -> "JSON_ARRAY exists only as a COPY INTO FILE_FORMAT value (Parser.scala), not a function"
  )

  /** Help documents that are real syntax but NOT functions, so the corpus walk in Task 5 does not
    * mistake them for phantoms. Complete at this baseline - derived by walking the corpus (PD-4).
    */
  val nonFunctionHelpNames: Set[String] = Set("CASE", "INTERVAL", "PI", "RANDOM")

  /** Kind.Fn rows whose function genuinely cannot appear as a single projected field, id -> reason.
    * Their ES construct is PINNED and the emitter marks es_construct_source = pinned.
    */
  val unprojectable: Map[String, String] = Map(
    "fn.geo.point.constructor" -> ("POINT only parses as an argument of ST_DISTANCE " +
    "(parser/function/geo/package.scala point/distance productions); the projected field's " +
    "chain belongs to ST_DISTANCE, so no POINT-specific derivation exists")
  )

  /** Names that must NOT appear as a function token (AC-2). */
  val phantomFunctionNames: Set[String] = Set(
    "IF",
    "IFNULL",
    "NVL",
    "LN",
    "MOD",
    "TRUNCATE",
    "LPAD",
    "RPAD",
    "SPACE",
    "REGEXP_REPLACE",
    "JSON_EXTRACT",
    "JSON_OBJECT",
    "JSON_ARRAY"
  )
}
