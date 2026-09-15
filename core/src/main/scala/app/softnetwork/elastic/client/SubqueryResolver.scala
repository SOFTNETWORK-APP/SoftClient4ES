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

package app.softnetwork.elastic.client

import app.softnetwork.elastic.client.result._
import app.softnetwork.elastic.sql._
// 🔴 RENAMED, deliberately: `app.softnetwork.elastic.client` (this package) declares its OWN
// `BooleanValue` / `StringValue` for aggregate results, and on the 2.12 leg a member of the
// enclosing package outranks BOTH a wildcard and an explicit import — so the two names silently
// resolve to the wrong classes there while the 2.13 leg compiles. Aliasing is the only spelling
// that means the same thing on both legs. Caught only by `+ core/compile`.
import app.softnetwork.elastic.sql.{BooleanValue => SqlBoolean, StringValue => SqlString}
import app.softnetwork.elastic.sql.`type`.{SQLTemporal, SQLType, SQLTypes}
import app.softnetwork.elastic.sql.operator._
import app.softnetwork.elastic.sql.query._

import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneOffset}
import scala.collection.immutable.ListMap

/** Story 22.2 / epic 22 AD-2 — ES-native TWO-PHASE execution of UNCORRELATED WHERE subqueries.
  *
  * Phase one runs each inner statement through the SAME client (`api.search`, so it crosses the
  * seam itself: schema attach, temporal literals, scroll routing, #224 window translation and —
  * recursively — its own subqueries) and rewrites the node into a literal criteria the bridges
  * already emit. Phase two is the outer statement, unchanged.
  *
  * Executed SYNCHRONOUSLY, on the async path too (lead ruling OQ-3, 2026-09-14): a second, async
  * seam would be two places for the subquery phase to go missing (#306's argument, verbatim).
  * THREAD MODEL, read from source rather than assumed: phase one runs on the thread that CONSTRUCTS
  * the outer request — `searchAsync` resolves BEFORE any `Future`, `ScrollApi.scroll` resolves at
  * `Source` construction, i.e. the gateway `ActorSystem`'s dispatcher thread that called
  * `SearchExecutor.execute`, or the caller's own thread on the direct API. An inner ROW-shaped body
  * executes through `search(inner)`, which `Await`s its scroll on `SearchApi.scrollRoutingSystem` —
  * a DIFFERENT system, so the blocked thread is never one the inner stream needs: no self-deadlock
  * at any nesting depth. COST BOUND per subquery: mode P / scalar / EXISTS = one round trip; mode W
  * row-shaped = at most `ceil(65,537 / scrollSize)` pages (66 at the default 1,000), sliced on ES
  * >= 7.15.
  *
  * 🔴 Inner statements execute under `EntityContext`, NOT `NativeContext`: under `NativeContext`
  * `ElasticConversion.rowNormalizer` NULL-FILLS a requested output name the response does not
  * carry, so a mismatch between `Field.outputName` and the key the converter produced (an
  * un-aliased `AVG(amount)`, a qualified `c.id`) would read as a genuine NULL — a
  * `MatchNoneCriteria`, i.e. a SILENT WRONG ANSWER. Under `EntityContext` the key is ABSENT and
  * [[cell]] / [[collect]] answer a 400 naming the column and the columns that ARE there.
  */
object SubqueryResolver {

  /** Elasticsearch's default `index.max_terms_count` — the ceiling on the values a `terms` query
    * may carry (6.8 -> 9.x, per index). ALSO this resolver's own hard bound: more distinct values
    * than this is a LOUD 400 naming the limit and the JOIN rewrite, never a truncated `terms` (the
    * #205 family). Equal to `Bucket.DefaultSize` by coincidence of Elasticsearch's defaults, not by
    * construction — the two are kept separate deliberately.
    */
  val MaxTerms: Int = 65536

  /** 🔴 `MaxTerms + 1`, never `MaxTerms`. With a bucket size of exactly 65,536 a 70,000-value
    * column would come back as EXACTLY 65,536 buckets — under the limit, silently truncated (#205
    * one story over). One bucket more is what makes the overflow VISIBLE to [[tooMany]].
    */
  private val Bound: Limit = Limit(MaxTerms + 1, None)

  private type Execute = SingleSearch => ElasticResult[ElasticResponse]
  private type ColumnType = (SingleSearch, String) => Option[SQLType]

  /** The rewritten statement (the SAME instance when nothing changed), or the FIRST failure: the
    * inner statement's own error with ITS status (never flattened, #184), or a 400 of this
    * object's.
    */
  def resolve(single: SingleSearch, api: SearchApi): ElasticResult[SingleSearch] =
    resolve(
      single,
      s => api.search(s)(EntityContext), // an absent key stays ABSENT — never null-filled
      api.scopeCorrelation(single, _),
      api.innerColumnType
    )

  /** Test seam: the inner executor, the bare-name correlation check and the inner column's mapped
    * type are injected, so the whole two-phase contract is provable Docker-free. `columnType(inner,
    * name)` answers `None` when no schema is loadable (the #306 skip list) — mode P stays the
    * default then.
    */
  private[client] def resolve(
    single: SingleSearch,
    execute: Execute,
    correlation: SingleSearch => Option[String] = _ => None,
    columnType: ColumnType = (_, _) => None
  ): ElasticResult[SingleSearch] =
    single.where.flatMap(_.criteria) match {
      case None => ElasticResult.success(single)
      case Some(criteria) =>
        rewrite(criteria, execute, correlation, columnType) match {
          case Right(c) if c eq criteria => ElasticResult.success(single)
          case Right(c)    => ElasticResult.success(single.copy(where = Some(Where(Some(c)))))
          case Left(error) => ElasticResult.failure(error)
        }
    }

  /** The SAME walk shape as `TemporalLiterals.rewrite` — predicate / relations / leaf, preserving
    * the instance (`eq`) wherever nothing changed, so a statement with no subquery is returned
    * untouched and the seam's second crossing (`search -> scrollRows -> ScrollApi.scroll`, #306) is
    * a genuine no-op.
    */
  private def rewrite(
    criteria: Criteria,
    execute: Execute,
    correlation: SingleSearch => Option[String],
    columnType: ColumnType
  ): Either[ElasticError, Criteria] = criteria match {
    case p: Predicate =>
      for {
        l <- rewrite(p.leftCriteria, execute, correlation, columnType)
        r <- rewrite(p.rightCriteria, execute, correlation, columnType)
      } yield
        if ((l eq p.leftCriteria) && (r eq p.rightCriteria)) p
        else p.copy(leftCriteria = l, rightCriteria = r)
    case n: ElasticNested =>
      rewrite(n.criteria, execute, correlation, columnType).map(c =>
        if (c eq n.criteria) n else n.copy(criteria = c)
      )
    case n: ElasticChild =>
      rewrite(n.criteria, execute, correlation, columnType).map(c =>
        if (c eq n.criteria) n else n.copy(criteria = c)
      )
    case n: ElasticParent =>
      rewrite(n.criteria, execute, correlation, columnType).map(c =>
        if (c eq n.criteria) n else n.copy(criteria = c)
      )
    case s: SubqueryCriteria => resolveOne(s, execute, correlation, columnType)
    case other               => Right(other)
  }

  private def resolveOne(
    node: SubqueryCriteria,
    execute: Execute,
    correlation: SingleSearch => Option[String],
    columnType: ColumnType
  ): Either[ElasticError, Criteria] =
    node.inner match {
      // Unreachable after `validate()`; `GatewayApi.run(statement)` does not validate a
      // programmatically built statement, so the arm is a named 400 rather than a MatchError.
      case None => Left(bad(s"Unsupported subquery body in ${node.sql}"))
      // Story 22.3b — DEFENSIVE, and UNREACHABLE through every route that exists today: a node with
      // non-empty `correlatedRefs` implies `hasCorrelatedSubqueries`, which both
      // `SearchApi.resolveWithSchema` (before phase one) and `CoreDqlExtension.execute` refuse
      // first. It is kept because the invariant it protects is a SILENT wrong answer if it ever
      // breaks — executing a correlated body as if it were self-contained — and because a future
      // caller of this object need not know the guard exists one layer up. Do not read it as
      // evidence of an open hole.
      case Some(_) if node.correlatedRefs.nonEmpty =>
        Left(RelationalClosureGuard.correlatedRejection)
      case Some(inner) =>
        correlation(inner) match {
          case Some(reason) => Left(bad(reason))
          case None =>
            node match {
              case in: InSubquery     => resolveIn(in, inner, execute, columnType)
              case ex: ExistsSubquery => resolveExists(ex, inner, execute)
              case sc: ScalarSubquery => resolveScalar(sc, inner, execute)
              case qs: QuantifiedSubquery =>
                resolveQuantified(qs, inner, execute, columnType)
            }
        }
    }

  // ── IN ─────────────────────────────────────────────────────────────────────────────────────

  /** MODE P (projection): the body is `SELECT <bare column> FROM … [WHERE …] [ORDER BY …]` with no
    * GROUP BY / HAVING / LIMIT / window / EXCEPT / JOIN. Its DISTINCT values ARE a terms
    * aggregation over the column — one bounded request whatever the row count — so it is executed
    * as `… GROUP BY <column> LIMIT MaxTerms+1` (a LIMIT on a grouped statement IS the bucket size,
    * `Bucket.update`). MODE W (as written): everything else runs unchanged, except that a
    * row-shaped body with no LIMIT (or a LIMIT window above the bound) is bounded at `MaxTerms + 1`
    * rows so the scroll it routes to cannot run away.
    *
    * 🔴 Mode P is a terms AGGREGATION, and a terms aggregation over a `text` column is an
    * Elasticsearch 400 (no fielddata) for a statement the analyst wrote WITHOUT a GROUP BY. When
    * the inner mapping is loadable and says `text`, the body runs AS WRITTEN (mode W); an unknown
    * mapping keeps mode P (the cheap default) and an ES refusal then propagates loudly with the
    * inner statement's own message.
    */
  private def valueSet(
    inner: SingleSearch,
    node: SubqueryCriteria,
    execute: Execute,
    columnType: ColumnType
  ): Either[ElasticError, (Seq[Any], Boolean, Boolean)] = {
    val field = inner.select.fields.head
    val projection = if (declinesModeP(inner, field, columnType)) None else modeP(inner, field)
    // 🔴 The column the CONVERTER produces for the statement ACTUALLY EXECUTED, derived the SAME way
    // `SearchApi.extractOutputFieldNames` derives it — `fieldsWithComputedAliases`, not `fields`.
    // MEASURED on real ES 8.18: an UN-ALIASED aggregate (`SELECT AVG(amount)`, the headline scalar
    // shape) comes back under the synthetic alias `__c1` while `select.fields.head.outputName` is
    // `amount`, so reading the plain field list answered "no column 'amount'" for a statement that
    // had run perfectly. One derivation, two consumers (the 21.3 lesson).
    for {
      statement <- projection
        .map(Right(_): Either[ElasticError, SingleSearch])
        .getOrElse(bounded(inner, node))
      column = columnNameOf(statement)
      rows      <- run(statement, execute, node)
      collected <- collect(rows, column, node, strict = projection.isDefined)
      values = collected._1
      hadNull = collected._2
      // A terms aggregation NEVER yields a null key (documents without the field are skipped), so
      // a null cell on mode P can only mean the column was not produced UNDER `column` — a naming
      // defect between `Field.outputName` and the converter's bucket key. Loud, never MatchNone.
      _ <-
        if (projection.isDefined && hadNull)
          Left(
            bad(
              s"Subquery ${node.sql}: the projected column '$column' was not produced by the " +
              "aggregation (naming defect, report it)"
            )
          )
        else Right(())
      _ <- if (values.size > MaxTerms) Left(tooMany(values.size, node)) else Right(())
    } yield (values, hadNull, projection.isDefined)
  }

  private def resolveIn(
    node: InSubquery,
    inner: SingleSearch,
    execute: Execute,
    columnType: ColumnType
  ): Either[ElasticError, Criteria] =
    for {
      resolved <- valueSet(inner, node, execute, columnType)
      values = resolved._1
      hadNull = resolved._2
      modePUsed = resolved._3
      // ANSI NULL rule (PD-5): a NULL among the inner values is invisible to `IN` (no row can equal
      // it) and makes `NOT IN` UNKNOWN for EVERY row — zero rows. Mode P cannot see NULLs (terms
      // aggregations skip missing values), so `NOT IN` probes for one explicitly.
      nullSeen <-
        if (node.maybeNot.isEmpty || hadNull) Right(hadNull)
        else if (modePUsed) nullProbe(inner, execute, node)
        else Right(false)
    } yield
      if (node.maybeNot.isDefined && nullSeen) MatchNoneCriteria()
      else if (values.isEmpty)
        if (node.maybeNot.isDefined) MatchAllCriteria() else MatchNoneCriteria()
      else TermValues.in(node.identifier, values, node.maybeNot)

  private[client] def columnNameOf(s: SingleSearch): String =
    s.select.fieldsWithComputedAliases.head.outputName

  /** Mode P is a terms AGGREGATION over the projected column, and TWO mapped types cannot carry
    * one:
    *
    *   - `text` has no fielddata, so Elasticsearch answers 400;
    *   - a `date` column buckets as a DATE HISTOGRAM, which needs an interval nobody wrote —
    *     MEASURED on real ES 8.18 as `all shards failed; Invalid interval specified, must be
    *     non-null and non-empty` for `IN (SELECT since FROM customers WHERE region = 'EU')`.
    *
    * Both therefore run the body AS WRITTEN (mode W). An UNKNOWN mapping keeps mode P, the cheap
    * default, and an Elasticsearch refusal then propagates loudly with the inner statement's own
    * message rather than being guessed at here.
    */
  private def declinesModeP(inner: SingleSearch, field: Field, columnType: ColumnType): Boolean =
    columnType(inner, field.identifier.name).exists {
      case SQLTypes.Text  => true
      case _: SQLTemporal => true
      case _              => false
    }

  private def modeP(inner: SingleSearch, field: Field): Option[SingleSearch] = {
    val id = field.identifier
    val bare = id.functions.isEmpty && id.name.nonEmpty && id.name != "*" && !id.nested
    if (
      bare && inner.groupBy.isEmpty && inner.having.isEmpty && inner.limit.isEmpty &&
      inner.windowFunctions.isEmpty && inner.select.except.isEmpty && inner.from.joins.isEmpty
    )
      Some(
        inner
          .copy(
            select = Select(Seq(field), None),
            groupBy = Some(GroupBy(Seq(Bucket(id)))),
            orderBy = None, // order is irrelevant to a SET
            limit = Some(Bound) // = the bucket size, MaxTerms + 1
          )
          .update()
      )
    else None
  }

  /** Mode W with the row bound: a row-shaped body KEEPS its own LIMIT when it has one within the
    * bound (`IN (SELECT id FROM t ORDER BY score DESC LIMIT 100)` means those 100); otherwise it is
    * bounded at `MaxTerms + 1` rows, which `SearchApi.search` routes through scroll with
    * `maxDocuments`. An aggregation-shaped body is bounded by its own bucket sizes.
    */
  private def bounded(
    inner: SingleSearch,
    node: SubqueryCriteria
  ): Either[ElasticError, SingleSearch] =
    if (!inner.returnsRows) Right(inner)
    else
      inner.limit match {
        case Some(l)
            if l.limit.toLong + l.offset.map(_.offset.toLong).getOrElse(0L) <= MaxTerms + 1 =>
          Right(inner)
        // 🔴 REJECTED, never rewritten. Replacing a written `LIMIT 10 OFFSET 100000` with
        // `LIMIT 65537` executes a COMPLETELY DIFFERENT window and, if the rows it reads hold at
        // most MaxTerms distinct values, the count check never fires — a silent wrong answer in the
        // one arm whose whole point is "the body means what it says".
        case Some(l) =>
          Left(
            bad(
              s"Subquery ${node.sql} asks for a window beyond $MaxTerms rows (${l.sql.trim}). " +
              "Narrow the subquery's LIMIT/OFFSET, or rewrite the statement as a JOIN (executed by " +
              "the relational engine, softclient4es-arrow-extensions)."
            )
          )
        case None => Right(inner.copy(limit = Some(Bound)))
      }

  /** `SELECT <col> FROM … WHERE (<inner where>) AND <col> IS NULL LIMIT 1` — one row means the
    * inner set contains a NULL. Built on the AST (`Predicate`, `IsNullExpr`, `Limit`), never by
    * re-parsing a render; its `.sql` is pinned as a parser fixed point in `SubqueryResolverSpec`.
    */
  private def nullProbe(
    inner: SingleSearch,
    execute: Execute,
    node: SubqueryCriteria
  ): Either[ElasticError, Boolean] = {
    val field = inner.select.fields.head
    val isNull: Criteria = IsNullExpr(field.identifier)
    val where = inner.where.flatMap(_.criteria) match {
      // NOT-FOLD: this CONSTRUCTS a predicate rather than reading one — the fourth argument is the
      // literal `None`, i.e. the probe carries no NOT at all, so there is no negation to fold onto
      // either operand. (`PainlessNullSurvivalSpec`'s scan cannot type a receiver and counts the
      // positional `Predicate(_, _, _, x, _)` shape whichever direction it runs in.)
      case Some(c) => Predicate(c, AND, isNull, None, group = true)
      case None    => isNull
    }
    val probe = inner
      .copy(
        select = Select(Seq(field), None),
        where = Some(Where(Some(where))),
        orderBy = None,
        limit = Some(Limit(1, None))
      )
      .update()
    run(probe, execute, node).map(_.nonEmpty)
  }

  // ── quantified (lead ruling OQ-4) ───────────────────────────────────────────────────────────

  /** `<id> <op> ANY|SOME|ALL (<subquery>)` for the ten combinations `IN` / `NOT IN` do not already
    * cover. The body resolves EXACTLY as [[resolveIn]] 's does; the quantifier is reduced HERE,
    * from the resolved value list.
    */
  private def resolveQuantified(
    node: QuantifiedSubquery,
    inner: SingleSearch,
    execute: Execute,
    columnType: ColumnType
  ): Either[ElasticError, Criteria] =
    for {
      resolved <- valueSet(inner, node, execute, columnType)
      values = resolved._1
      hadNull = resolved._2
      modePUsed = resolved._3
      // Which forms need to KNOW whether the inner set carries a NULL:
      //   - every `ALL` form — a NULL makes the universal UNKNOWN for every row;
      //   - 🔴 every NEGATED form, `ANY` included. `NOT (x > ANY S)` is NOT `NOT (x > min S)` once S
      //     holds a NULL: with `S = {1, NULL}` and `x = 0` the inner is `FALSE OR UNKNOWN` =
      //     UNKNOWN, so its negation is UNKNOWN and no row matches — while `must_not(range gt 1)`
      //     would RETURN that row. A plain (un-negated) `ANY` genuinely ignores inner NULLs.
      // Mode P cannot see a NULL (terms aggregations skip missing values), so it probes — exactly
      // as `NOT IN` does, through the SAME helper.
      nullSeen <-
        if (!(node.universal || node.maybeNot.isDefined) || hadNull) Right(hadNull)
        else if (modePUsed) nullProbe(inner, execute, node)
        else Right(false)
      reduced <- reduceQuantified(node, values, nullSeen)
    } yield reduced

  /** 🔴 The ANSI reduction table. Order matters and the two empty-set rows are OPPOSITE — the
    * single most likely thing to ship backwards:
    *
    *   1. `ALL` over a set containing NULL is UNKNOWN for every row (never TRUE, never FALSE), so
    *      it answers no rows and an outer `NOT` does NOT flip it — UNKNOWN negated is still
    *      UNKNOWN. `ANY` simply ignores inner NULLs.
    *   1. Over an EMPTY set an existential (`ANY`/`SOME`) is FALSE and a universal (`ALL`) is TRUE.
    *      Both are genuine truth values, so an outer `NOT` DOES flip them.
    *   1. Otherwise: `> ANY` is `> min`, `> ALL` is `> max`, and the two irreducible forms use both
    *      ends — `= ALL` is `min == max && x = min`, `<> ANY` is its negation.
    *
    * Story 22.3 runs the CORRELATED twins of these forms on DuckDB, which is ANSI; the two venues
    * must agree on the same statement, which is the argument that decided PD-5.
    */
  private[client] def reduceQuantified(
    node: QuantifiedSubquery,
    values: Seq[Any],
    nullSeen: Boolean
  ): Either[ElasticError, Criteria] = {
    val negate = node.maybeNot.isDefined
    def truth(b: Boolean): Criteria =
      if (b != negate) MatchAllCriteria() else MatchNoneCriteria()
    // UNKNOWN, and UNKNOWN negated is still UNKNOWN — so this answer is NOT run through `truth`.
    if ((node.universal || negate) && nullSeen) Right(MatchNoneCriteria())
    else if (values.isEmpty) Right(truth(node.universal))
    else {
      val ordered = TermValues.sorted(values)
      val lo = ordered.head
      val hi = ordered.last
      def cmp(op: ComparisonOperator, v: Any): Criteria =
        GenericExpression(node.identifier, op, TermValues.literal(v), node.maybeNot)
      (node.operator, node.universal) match {
        case (GT, false) => Right(cmp(GT, lo))
        case (GE, false) => Right(cmp(GE, lo))
        case (LT, false) => Right(cmp(LT, hi))
        case (LE, false) => Right(cmp(LE, hi))
        case (GT, true)  => Right(cmp(GT, hi))
        case (GE, true)  => Right(cmp(GE, hi))
        case (LT, true)  => Right(cmp(LT, lo))
        case (LE, true)  => Right(cmp(LE, lo))
        // `x = ALL (S)` — true only when S is a singleton value and x equals it.
        case (EQ, true) => Right(if (lo == hi) cmp(EQ, lo) else truth(false))
        // `x <> ANY (S)` — the negation of the row above: true unless S is a singleton x equals.
        case (NE | DIFF, false) => Right(if (lo == hi) cmp(NE, lo) else truth(true))
        // `= ANY` and `<> ALL` collapse to `InSubquery` at PARSE time, so no PARSED statement can
        // reach this arm — but `GatewayApi.run(statement)` accepts a programmatically built AST, and
        // answering `match_none` there would be a silent wrong answer. Loud, like the sibling arm in
        // `resolveOne`.
        case (op, universal) =>
          Left(
            bad(
              s"Unsupported quantified comparison '$op ${if (universal) "ALL" else "ANY"}' in " +
              s"${node.sql}: this combination is expressed as IN / NOT IN."
            )
          )
      }
    }
  }

  // ── EXISTS ─────────────────────────────────────────────────────────────────────────────────

  /** True iff the body returns >= 1 row. A row-shaped body is capped at `LIMIT 1` (its own `LIMIT
    * 0` is KEPT: zero rows, EXISTS false — DuckDB agrees); an aggregation-shaped body runs as
    * written (a metric-only SELECT always yields one row => EXISTS true, as in every ANSI engine).
    * A GROUP BY body with NO HAVING is capped at one BUCKET; with a HAVING it must run as written,
    * because the `bucket_selector` filters AFTER the size cut.
    */
  private def resolveExists(
    node: ExistsSubquery,
    inner: SingleSearch,
    execute: Execute
  ): Either[ElasticError, Criteria] = {
    val statement =
      if (inner.returnsRows && !inner.limit.exists(_.limit == 0))
        inner.copy(limit = Some(Limit(1, None)))
      else if (inner.groupBy.isDefined && inner.having.isEmpty && inner.limit.isEmpty)
        inner.copy(limit = Some(Limit(1, None)))
      else inner
    run(statement, execute, node).map { rows =>
      if (rows.nonEmpty != node.maybeNot.isDefined) MatchAllCriteria() else MatchNoneCriteria()
    }
  }

  // ── scalar ─────────────────────────────────────────────────────────────────────────────────

  /** 0 rows -> the subquery is NULL -> the comparison is UNKNOWN -> no row (`MatchNoneCriteria`,
    * and NOT negated by an outer `NOT`: `NOT (x > NULL)` is UNKNOWN too — ANSI, and DuckDB); 1 row
    * -> a literal comparison the bridge already emits; > 1 rows -> 400 (ANSI cardinality
    * violation).
    */
  private def resolveScalar(
    node: ScalarSubquery,
    inner: SingleSearch,
    execute: Execute
  ): Either[ElasticError, Criteria] = {
    // 🔴 BOUNDED, like every other inner execution. `ScalarSubquery.validate` keeps a scalar body
    // to "metric-only, or LIMIT 1", but `GatewayApi.run(statement)` does not validate a
    // programmatically built AST — and two rows are all this method ever needs in order to report
    // the ANSI cardinality violation below, so an unbounded row-shaped body can never run away here.
    val statement =
      if (inner.returnsRows && !inner.limit.exists(_.limit <= 2))
        inner.copy(limit = Some(Limit(2, None)))
      else inner
    val column = columnNameOf(statement)
    for {
      rows <- run(statement, execute, node)
      value <- rows match {
        case Seq()    => Right(None)
        case Seq(row) => cell(row, column, node).flatMap(scalarCell(_, node))
        case many =>
          Left(
            bad(s"Scalar subquery returned ${many.size} rows, expected at most one: ${node.sql}")
          )
      }
    } yield value match {
      case None => MatchNoneCriteria()
      case Some(v) =>
        GenericExpression(node.identifier, node.operator, TermValues.literal(v), node.maybeNot)
    }
  }

  /** A scalar cell may arrive as a ONE-element sequence (the row path's array wrap of a script
    * field — `UPPER(a) AS up` -> `List("X")`), so it is unwrapped; a genuinely multi-valued cell is
    * not a scalar (400, the ANSI cardinality violation on the VALUE axis).
    */
  private def scalarCell(v: Any, node: SubqueryCriteria): Either[ElasticError, Option[Any]] =
    v match {
      case null                                      => Right(None)
      case s: scala.collection.Seq[_] if s.size <= 1 => Right(s.headOption.flatMap(Option(_)))
      case s: scala.collection.Seq[_] =>
        Left(bad(s"Scalar subquery ${node.sql} returned a multi-valued cell (${s.size} values)"))
      case other => Right(Some(other))
    }

  // ── plumbing ───────────────────────────────────────────────────────────────────────────────

  private def run(
    s: SingleSearch,
    execute: Execute,
    node: SubqueryCriteria
  ): Either[ElasticError, Seq[ListMap[String, Any]]] =
    execute(s) match {
      case ElasticSuccess(response) => Right(response.results)
      // #184 — the inner failure keeps ITS status and cause; only the message says where it
      // happened. Mode P's ES-side overflow on >= 7.10 is a `too_many_buckets_exception`, which is
      // the SAME bound the resolver's own count check enforces, so the analyst sees ONE message
      // whichever side hit it first.
      case ElasticFailure(error) =>
        if (mentionsTooManyBuckets(error)) Left(tooManyFrom(error, node))
        else Left(error.copy(message = s"Subquery ${node.sql} failed: ${error.message}"))
    }

  private def mentionsTooManyBuckets(error: ElasticError): Boolean = {
    def mentions(message: String): Boolean =
      message != null && message.contains("too_many_buckets")
    def walk(t: Throwable, depth: Int): Boolean =
      t != null && depth > 0 &&
      (mentions(t.getMessage) ||
      t.getSuppressed.exists(s => walk(s, depth - 1)) ||
      walk(t.getCause, depth - 1))
    mentions(error.message) || error.cause.exists(t => walk(t, 10))
  }

  /** The projected column's values, DISTINCT and non-null, plus whether a null was seen.
    *
    * A cell that is a sequence (the row path's array wrap of a script field, or a genuinely
    * multi-valued field) is FLATTENED — SQL-on-arrays semantics: `x IN (SELECT tags FROM t)`
    * matches any tag. A MISSING column is a 400, never guessed.
    */
  private def collect(
    rows: Seq[ListMap[String, Any]],
    column: String,
    node: SubqueryCriteria,
    strict: Boolean
  ): Either[ElasticError, (Seq[Any], Boolean)] = {
    val builder = scala.collection.mutable.LinkedHashSet.empty[Any]
    var hadNull = false
    var failure: Option[ElasticError] = None
    // 🔴 Under `EntityContext` a document carrying NO VALUE for the projected column yields a row
    // WITHOUT the key — which on the row path (mode W) is an ordinary SQL NULL, not a defect.
    // Treating it as one turned `x NOT IN (SELECT <nullable column> FROM t)`, the exact ANSI shape
    // PD-5 exists for, into an HTTP 400 instead of zero rows. `strict` is true only for mode P,
    // where the aggregation contract really does guarantee the key; on the row path the
    // naming-defect 400 is kept for the case that can only BE one — rows came back and NOT ONE of
    // them carries the column.
    if (!strict && rows.nonEmpty && !rows.exists(_.contains(column)))
      return Left(missingColumn(rows.head, column, node))
    rows.foreach { row =>
      if (failure.isEmpty)
        (if (strict) cell(row, column, node)
         else Right(row.getOrElse(column, null))) match {
          case Left(e) => failure = Some(e)
          case Right(v) =>
            val flat: Seq[Any] = v match {
              case null                       => Seq(null)
              case s: scala.collection.Seq[_] => if (s.isEmpty) Seq(null) else s.toList
              case other                      => Seq(other)
            }
            flat.foreach {
              case null => hadNull = true
              // 🔴 CANONICALISED before the set, not after: a `ZonedDateTime` and the `Instant` it
              // converts to are distinct objects that render to the SAME ISO-8601 literal, and the
              // converters produce both shapes for one `date` column. Deduplicating on the raw cell
              // would emit the term twice and — worse — count it twice against MaxTerms.
              case x => builder += TermValues.canonical(x)
            }
        }
    }
    failure.map(Left(_)).getOrElse(Right((builder.toList, hadNull)))
  }

  private def cell(
    row: ListMap[String, Any],
    column: String,
    node: SubqueryCriteria
  ): Either[ElasticError, Any] =
    row.get(column).toRight(missingColumn(row, column, node))

  private def missingColumn(
    row: ListMap[String, Any],
    column: String,
    node: SubqueryCriteria
  ): ElasticError =
    bad(
      s"Subquery ${node.sql} returned no column '$column' " +
      s"(columns: ${row.keys.mkString(", ")})"
    )

  private def tooMany(n: Int, node: SubqueryCriteria): ElasticError =
    bad(
      s"Subquery ${node.sql} returned more than $MaxTerms distinct values ($n or more), the " +
      "maximum a terms query accepts (index.max_terms_count / search.max_buckets). Narrow the " +
      "subquery, or rewrite the statement as a JOIN (executed by the relational engine, " +
      "softclient4es-arrow-extensions)."
    )

  private def tooManyFrom(error: ElasticError, node: SubqueryCriteria): ElasticError =
    tooMany(MaxTerms + 1, node).copy(cause = error.cause)

  private def bad(message: String): ElasticError =
    ElasticError(message = message, statusCode = Some(400), operation = Some("subquery"))
}

/** Typed literal values for the rewritten predicate, built from the cells the CONVERTER returned
  * (Jackson's smallest type per value — `ElasticConversion.jsonNodeToAny`; `extractBucketKey` for a
  * terms key).
  *
  *   - every integral -> `LongValues`;
  *   - ANY floating value -> the WHOLE list becomes `DoubleValues` (a column mixing `1` and `2.5`
  *     must not be typed on its FIRST value, which is how `inToQuery` types the terms query);
  *   - strings -> `StringValues`; booleans -> `BooleanValues` (rendered `true` / `false`, which an
  *     Elasticsearch boolean field accepts).
  *
  * 🔴 TEMPORAL cells are the arm the converters make NECESSARY: `extractBucketKey` prefers
  * `key_as_string` and parses it into a `ZonedDateTime`, turns an integral key in the epoch-millis
  * range into one too, and `jsonNodeToAny` parses date-shaped strings on the row path. So every
  * `java.time.temporal.Temporal` / `java.util.Date` cell is rendered as an ISO-8601 INSTANT string
  * (UTC) inside a `StringValue` — exactly what a hand-written `IN ('2024-01-01T00:00:00Z')` is — so
  * `TemporalLiterals`' `InExpr(StringValues)` / `GenericExpression(StringValue)` arms then
  * normalise it against the OUTER column's mapped format, custom formats included. NEVER `toString`
  * (a `ZonedDateTime` renders `…Z[UTC]`, which no ES date format accepts) and never epoch millis (a
  * custom format without `epoch_millis` rejects them).
  *
  * Documented residual: the converter's millis-range heuristic also turns a `long` id between 1e12
  * and 1e13 into a date — the same value already DISPLAYS as a date at the REPL, so such a column
  * is unusable in mode P until that heuristic is revisited (out of scope here).
  *
  * [[in]] returns the CRITERIA (an `InExpr` built INSIDE, where `R` / `T` are concrete) rather than
  * an existential `Values[_, _ <: Value[_]]`: `InExpr[R, +T <: Value[R]]` will not infer through an
  * existential on the 2.12 leg.
  */
private[client] object TermValues {

  /** 🔴 FIXED-WIDTH, deliberately — NOT `DateTimeFormatter.ISO_INSTANT`.
    *
    * `ISO_INSTANT` emits 0, 3, 6 or 9 fractional digits depending on the value, so
    * `"2024-01-01T00:00:00.500Z"` and `"2024-01-01T00:00:00Z"` differ in LENGTH and compare
    * `'.'(0x2E) < 'Z'(0x5A)` — the LATER instant sorts FIRST. [[sorted]] reads that order to pick
    * the `min`/`max` a quantified comparison reduces to, so a millisecond-bearing `date` column
    * would have produced `> ALL` bounds that are too low: rows Elasticsearch must not return. With
    * three digits always present, lexicographic order IS chronological order, and
    * `strict_date_optional_time` accepts the spelling on every supported major.
    */
  private val Iso: DateTimeFormatter =
    DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(ZoneOffset.UTC)

  def in(identifier: Identifier, values: Seq[Any], maybeNot: Option[NOT.type]): Criteria =
    if (values.forall(isIntegral))
      InExpr(identifier, LongValues(values.map(v => LongValue(asLong(v)))), maybeNot)
    else if (values.forall(isNumeric))
      InExpr(identifier, DoubleValues(values.map(v => DoubleValue(asDouble(v)))), maybeNot)
    else if (values.forall(_.isInstanceOf[java.lang.Boolean]))
      InExpr(
        identifier,
        BooleanValues(values.map(v => SqlBoolean(v.asInstanceOf[java.lang.Boolean]))),
        maybeNot
      )
    else InExpr(identifier, StringValues(values.map(v => SqlString(asString(v)))), maybeNot)

  /** The form a cell is COMPARED and DEDUPLICATED in: the exact literal the predicate will carry.
    * Only temporals move (to their UTC ISO-8601 instant); everything else is its own canonical
    * form.
    */
  def canonical(v: Any): Any = v match {
    case _: java.util.Date | _: java.time.temporal.Temporal => asString(v)
    case other                                              => other
  }

  def literal(v: Any): Value[_] = v match {
    case b: java.lang.Boolean => SqlBoolean(b)
    case x if isIntegral(x)   => LongValue(asLong(x))
    case x if isNumeric(x)    => DoubleValue(asDouble(x))
    case other                => SqlString(asString(other))
  }

  /** The ordering the quantified reduction's `min` / `max` are read from.
    *
    * Numeric values compare numerically; everything else compares as the STRING the predicate will
    * carry — which is what Elasticsearch's own `range` query over a keyword field does, and which
    * is chronological for the ISO-8601 instants temporal cells render as (same length, same `Z`
    * suffix, so lexicographic order IS time order).
    */
  def sorted(values: Seq[Any]): Seq[Any] =
    // Integral values are ordered as LONGS: `asDouble` loses precision above 2^53, which would pick
    // the wrong neighbour as the max of a `long` id column.
    if (values.forall(isIntegral)) values.sortBy(asLong)
    else if (values.forall(isNumeric)) values.sortBy(asDouble)
    else values.sortBy(asString)

  private def isIntegral(v: Any): Boolean = v match {
    case _: java.lang.Byte | _: java.lang.Short | _: java.lang.Integer | _: java.lang.Long => true
    case _: java.math.BigInteger                                                           => true
    case _                                                                                 => false
  }

  private def isNumeric(v: Any): Boolean = isIntegral(v) || (v match {
    case _: java.lang.Float | _: java.lang.Double => true
    case _: java.math.BigDecimal                  => true
    case _                                        => false
  })

  private def asLong(v: Any): Long = v.asInstanceOf[Number].longValue()

  private def asDouble(v: Any): Double = v.asInstanceOf[Number].doubleValue()

  private def asString(v: Any): String = v match {
    case s: String                      => s
    case i: Instant                     => Iso.format(i)
    case d: java.util.Date              => Iso.format(d.toInstant)
    case t: java.time.ZonedDateTime     => Iso.format(t.toInstant)
    case t: java.time.OffsetDateTime    => Iso.format(t.toInstant)
    case t: java.time.LocalDateTime     => Iso.format(t.toInstant(ZoneOffset.UTC))
    case t: java.time.LocalDate         => Iso.format(t.atStartOfDay(ZoneOffset.UTC).toInstant)
    case t: java.time.temporal.Temporal => Iso.format(Instant.from(t))
    case other                          => String.valueOf(other)
  }
}
