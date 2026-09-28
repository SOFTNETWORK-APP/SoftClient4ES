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

import app.softnetwork.elastic.sql.{FromTo, Identifier, PainlessScript, Token, Value}
import app.softnetwork.elastic.sql.function.{FunctionChain, FunctionN}
import app.softnetwork.elastic.sql.function.cond.{Case, Coalesce}
import app.softnetwork.elastic.sql.function.geo.Distance

/** Is a value the engine computes with a Painless script evaluated against a document that holds
  * the columns it reads?
  *
  * An UNNEST column lives in a NESTED document. Elasticsearch evaluates a script against exactly
  * one document, and a nested field has no doc values in its parent (nor a parent field in a nested
  * document), so a script that reads a column of another document reads NOTHING — the null guard
  * turns it into `null`. MEASURED at RUN level on Elasticsearch 8.18.3 and 6.8.23:
  *
  *   - SELECT: every function over an UNNEST column is a top-level `script_fields` entry, evaluated
  *     once per PARENT hit -- beside a window function too, where the rows come from the
  *     window-free statement `SingleSearch.withoutWindows`. When the column itself carries the
  *     chain (`YEAR(i.d)`, `CAST(i.n AS ..)`) the row explosion then writes the column's RAW
  *     inner-hit value over the script's null under the same output name; when the chain has no
  *     named carrier (`UPPER(i.s)`, `i.n + 2`, `COALESCE(..)`, `CASE ..`) no nested query is
  *     emitted at all and the answer is one row per parent with a NULL value. Not one cell answered
  *     correctly on its own merits.
  *   - WHERE: a leaf whose operand is an UNNEST column carrying the chain is wrapped in
  *     `ElasticNested` and evaluated per element — CORRECT; a leaf whose chain has no named carrier
  *     is evaluated against the parent and answers zero rows (or every row, through `COALESCE`) at
  *     HTTP 200; a leaf evaluated per element that also reads a PARENT column reads null for it.
  *   - WINDOW: the value argument is evaluated by the window's aggregation, per element when the
  *     bridge wraps it in a `nested` aggregation (a bare column, a `CAST`, a date part, arithmetic:
  *     `SUM(i.n * 1) OVER (..)` CORRECT), else per parent: `SUM(ABS(i.n)) OVER (PARTITION BY id)`
  *     answered 0.0 on every row at HTTP 200 (Elasticsearch 8.18.3).
  *
  * ONE derivation serves every clause: the scope a column is read in ([[scopeOf]]), the columns an
  * expression reads ([[readsOf]]) and the document the engine evaluates it against — the parent for
  * a SELECT script field, the carrier's element for a WHERE leaf whose carrier is nested (exactly
  * `Expression.nested`, the flag `update` wraps a leaf on), the nested element of a window's
  * aggregation (exactly `Field.nestedElement`, the key the bridge wraps it on). A value is refused
  * when it reads a column outside that document.
  *
  * Deliberately OUT of this rule, each for a stated reason:
  *   - a statement the relational engine plans (`relationalClosureRequired`): softclient4es-arrow
  *     pushes only RAW columns to the Elasticsearch leg and evaluates the SELECT list itself;
  *   - an AGGREGATE -- no partition, no order, `SUM(..) OVER ()` included: the GROUP BY family, not
  *     a `script_fields` entry of the statement that runs. A function projected BESIDE a window is
  *     judged: a window-enriched row query executes `SingleSearch.withoutWindows`, whose
  *     `script_fields` carry it. Beside a whole-table aggregate (`COUNT(*)`, `SUM(..) OVER ()`)
  *     nothing is a script field -- the statement is aggregation-shaped and the function is not
  *     evaluated at all, on a flat index alike;
  *   - a WHERE leaf that carries NO function (`i.n > 0`, `MATCH (i.s) AGAINST ('x')`): a query-DSL
  *     leaf the nested-query machinery places itself;
  *   - in a top-level WHERE leaf only, a bare UNNEST column after a non-null literal in `COALESCE`
  *     (`COALESCE('x', i.s) = 'x'`): never the answer, so not a read ([[whereReadsOf]]);
  *   - GROUP BY / HAVING / ORDER BY: other mechanisms, not part of this rule.
  */
object UnnestScope {

  /** Deeper than any expression a user writes; bounds the walk on a pathological AST. */
  private val MaxDepth: Int = 64

  /** The UNNEST element whose column `id` reads, named by the statement's OWN UNNEST alias; `None`
    * for a column of the parent document.
    *
    * Decided by the FROM's UNNEST aliases, never by a name that merely looks nested: a column is an
    * UNNEST column exactly when `GenericIdentifier.update` resolved its qualifier through
    * `unnestAliases` (its `tableAlias` IS that alias). A copy `update` never reached keeps the
    * qualifier in its NAME, so the qualifier as written is read for it — the same alias, by the
    * same map. Two such copies are MEASURED: a column inside a `BETWEEN` bound
    * (`BetweenExpr.update` updates the operand, never the range: `t.m BETWEEN ABS(i.n) AND 10`),
    * and the receiver of `WEEKDAY` / `DAYOFWEEK` (`Extract.update` returns `this`) until that
    * function resolves its receiver. The bound is the one that keeps this fallback live.
    */
  private[query] def scopeOf(id: Identifier, unnestAliases: Set[String]): Option[String] =
    id.tableAlias match {
      case Some(alias) => Some(alias).filter(unnestAliases.contains)
      case None =>
        val dot = id.name.indexOf('.')
        if (dot <= 0) None else Some(id.name.substring(0, dot)).filter(unnestAliases.contains)
    }

  /** Every column `token` READS: a carrier that names a column, every function argument, the
    * operands of `DISTANCE` (kept outside `args`), a `CASE` expression, its `WHEN` conditions and
    * results, a `BETWEEN` range and both sides of a criterion.
    *
    * 🔴 Not `FunctionUtils.funIdentifiers`: it misses a `CASE`'s `WHEN` conditions (they are not
    * `Case.args`) and `DISTANCE`'s operands (`Distance.args` is empty) — both MEASURED blind spots.
    * A function that holds its column as a receiver (`FunctionWithIdentifier`) needs no arm of its
    * own: the parser makes that receiver the CARRIER (`Parser.identifierWithFunction`), and every
    * such function is a `FunctionN` whose receiver is also an argument or is empty (`COALESCE`,
    * `GREATEST`, `LEAST`, `NULLIF`) — MEASURED over the derived population, no column is read only
    * through it.
    *
    * A criterion is met here only as a `CASE WHEN` condition (a WHERE leaf is judged by
    * [[violations]] itself): a comparison, `AND` / `OR`, a `NESTED` / `CHILD` wrapper or `MATCH`
    * -- each MEASURED reachable. A subquery is not: `Case.validate()` refuses it first ("A subquery
    * is not supported in a CASE WHEN condition"). A nameless carrier (`UPPER(..)`'s, a literal
    * bound's) names no column.
    */
  private[query] def readsOf(token: Token): Seq[Identifier] = reads(token, _.args)

  /** What a top-level WHERE leaf READS: [[readsOf]], less the DEAD arguments of a `COALESCE` -- a
    * bare UNNEST column that follows a non-null literal argument. `COALESCE` answers its first
    * non-null argument, so that column is never the answer: the leaf does not depend on the element
    * and the engine answers it correctly per parent (MEASURED at RUN on Elasticsearch 8.18.3 and
    * 6.8.23: `WHERE COALESCE('x', i.s) = 'x'` returns the rows of the same statement without it).
    *
    * WHERE ONLY. A SELECT or a window argument that reads no live UNNEST column still loses the
    * element -- the statement answers one row per parent, and `SUM(COALESCE(1, i.n)) OVER (..)`
    * counts parents -- and a leaf under an `ElasticNested` is not answered per parent: each is
    * judged by [[readsOf]].
    */
  private def whereReadsOf(token: Token, unnestAliases: Set[String]): Seq[Identifier] =
    reads(
      token,
      {
        case c: Coalesce => liveArguments(c, unnestAliases)
        case n           => n.args
      }
    )

  /** `c`'s arguments less every bare UNNEST column that follows its first non-null literal. A
    * literal only -- `NULL` and a `?` parameter can be NULL (`Value.nullable`) -- and a BARE column
    * only: a chain over a dead column is still evaluated against the parent, so it is not dead.
    */
  private def liveArguments(c: Coalesce, unnestAliases: Set[String]): Seq[PainlessScript] = {
    val literal = c.values.indexWhere(nonNullLiteral)
    c.values.zipWithIndex.collect {
      case (arg, i) if literal < 0 || i <= literal || !bareUnnestColumn(arg, unnestAliases) => arg
    }
  }

  /** A literal that is never NULL: a nameless carrier holding one non-nullable `Value`, or that
    * `Value` itself.
    */
  private def nonNullLiteral(arg: Token): Boolean = arg match {
    case v: Value[_] => !v.nullable
    case id: Identifier if id.name.isEmpty =>
      id.functions match {
        case (v: Value[_]) :: Nil => !v.nullable
        case _                    => false
      }
    case _ => false
  }

  private def bareUnnestColumn(arg: Token, unnestAliases: Set[String]): Boolean = arg match {
    case id: Identifier =>
      id.name.nonEmpty && id.functions.isEmpty && scopeOf(id, unnestAliases).isDefined
    case _ => false
  }

  /** The ONE walk behind [[readsOf]] and [[whereReadsOf]]: `argsOf` names the arguments of a
    * function that are read.
    */
  private def reads(
    token: Token,
    argsOf: FunctionN[_, _] => Seq[PainlessScript]
  ): Seq[Identifier] = {
    def walk(t: Token, depth: Int): Seq[Identifier] =
      if (depth > MaxDepth) Nil
      else
        t match {
          case id: Identifier =>
            (if (id.name.nonEmpty) Seq(id) else Nil) ++
              id.functions.flatMap(walk(_, depth + 1))
          case c: Criteria => criteria(c, depth + 1)
          case c: Case =>
            c.args.flatMap(walk(_, depth + 1)) ++
              c.conditions.flatMap { case (when, _) => walk(when, depth + 1) }
          case d: Distance        => d.identifiers.flatMap(walk(_, depth + 1))
          case n: FunctionN[_, _] => argsOf(n).flatMap(walk(_, depth + 1))
          case fc: FunctionChain  => fc.functions.flatMap(walk(_, depth + 1))
          case ft: FromTo         => walk(ft.from, depth + 1) ++ walk(ft.to, depth + 1)
          case _                  => Nil
        }
    def criteria(c: Criteria, depth: Int): Seq[Identifier] =
      c match {
        case Predicate(l, _, r, _, _) => criteria(l, depth + 1) ++ criteria(r, depth + 1)
        case rel: ElasticRelation     => criteria(rel.criteria, depth + 1)
        case m: MultiMatchCriteria    => m.identifiers.flatMap(walk(_, depth + 1))
        case e: Expression =>
          walk(e.identifier, depth + 1) ++ e.maybeValue.toSeq.flatMap(walk(_, depth + 1))
        case _ => Nil
      }
    walk(token, 0)
  }

  /** A value refused by this rule, named: the clause, the expression as written, the column it
    * cannot see, and where the engine evaluates it (`None` = the parent document).
    */
  private[query] final case class Violation(
    clause: String,
    expression: String,
    column: Identifier,
    evaluatedIn: Option[String]
  ) {

    /** 🔴 The stable phrase FIRST and the remedy LAST: `GatewayApi` relays a parse refusal longer
      * than 200 characters as its first 120 + `...` + its last 77, so both ends must survive a cut.
      * The remedy was MEASURED with a LIMIT: a plain UNNEST projection then answers every element.
      * Without one it currently returns at most three elements per parent (Elasticsearch's default
      * inner-hits size) -- a separate defect, documented beside this refusal.
      */
    def message: String = {
      val where = evaluatedIn match {
        case None        => "per parent document"
        case Some(alias) => s"per element of UNNEST alias '$alias'"
      }
      val remedy =
        if (clause == "SELECT") "compute it from the returned rows" else "filter the returned rows"
      // The COLUMN as written, without the chain a carrier holds (`YEAR(i.d)`'s carrier IS `i.d`).
      val col = column.withFunctions(Nil).sql
      s"$StablePhrase$clause: $expression is evaluated $where, where $col is not visible. " +
      s"Select its columns and $remedy."
    }
  }

  /** The head every refusal of this rule starts with -- what a caller or a test anchors on. */
  val StablePhrase: String = "A function over an UNNEST column is not supported in "

  /** Does `token` carry a FUNCTION -- a transform other than a literal? A literal operand is a
    * nameless carrier holding one `Value` (a `BETWEEN` bound `10`), which is no function. A
    * `BETWEEN` range carries one when either bound does.
    */
  private def carriesFunction(token: Token): Boolean = token match {
    case fc: FunctionChain =>
      fc.functions.exists {
        case _: Value[_] => false
        case _           => true
      }
    case ft: FromTo => carriesFunction(ft.from) || carriesFunction(ft.to)
    case _          => false
  }

  /** Every WHERE leaf, in statement order, with whether it is at the TOP LEVEL -- outside any
    * `ElasticNested` (an explicit `NESTED(..)`, or the wrapper `update` puts around a leaf whose
    * carrier is nested): a query the engine answers per parent. Under one, a leaf is evaluated per
    * element, or DROPPED when the wrapper scopes no nested element (the bridge renders it
    * `match_all`). A join relation (`CHILD` / `PARENT`) reads OTHER documents -- not an UNNEST
    * element -- and falls to the default: not this rule's.
    */
  private def whereLeaves(c: Criteria, topLevel: Boolean): Seq[(Criteria, Boolean)] = c match {
    case Predicate(l, _, r, _, _) => whereLeaves(l, topLevel) ++ whereLeaves(r, topLevel)
    case n: ElasticNested         => whereLeaves(n.criteria, topLevel = false)
    case e: Expression            => Seq(e -> topLevel)
    case s: SubqueryCriteria      => Seq(s -> topLevel)
    case _                        => Nil
  }

  /** The ONE decision. Empty for a statement without UNNEST, and for one the relational engine
    * plans.
    */
  private[query] def violations(ss: SingleSearch): Seq[Violation] = {
    val aliases = ss.unnestAliases.keySet
    if (aliases.isEmpty || ss.relationalClosureRequired) Nil
    else {
      def outside(evaluatedIn: Option[String])(reads: Seq[Identifier]): Option[Identifier] =
        reads.find(scopeOf(_, aliases) != evaluatedIn)
      // SELECT -- a `script_fields` entry is evaluated once per PARENT hit. Judged on the statement
      // that RUNS: a window-enriched row query executes `withoutWindows` (its own `scriptFields` is
      // empty beside a window, which counts as an aggregate).
      val runs = if (ss.windowRowQuery) ss.withoutWindows else ss
      val select = runs.scriptFields.flatMap { f =>
        outside(None)(readsOf(f.identifier)).map(Violation("SELECT", f.identifier.sql, _, None))
      }
      // WINDOW -- its VALUE argument (never its PARTITION BY / ORDER BY) is evaluated by the
      // window's aggregation: per element of the `nested` aggregation the bridge wraps it in, keyed
      // on `Field.nestedElement`, else per parent. Not `nested` of the argument: an arithmetic
      // argument has a nameless carrier and is wrapped all the same (MEASURED at RENDER).
      val window = ss.windowFields.flatMap { f =>
        f.identifier.windows.filter(_.isWindowing).toSeq.flatMap { w =>
          val evaluatedIn = f.nestedElement.map(_.innerHitsName)
          outside(evaluatedIn)(readsOf(w.identifier))
            .map(Violation("SELECT", f.identifier.sql, _, evaluatedIn))
        }
      }
      // WHERE -- a leaf is evaluated against its carrier's element when the carrier is nested
      // (`Expression.nested`: what `update` wraps it in `ElasticNested` on), else the parent. A
      // top-level leaf -- answered per parent -- does not read a `COALESCE`'s dead arguments
      // (`whereReadsOf`); under an `ElasticNested` that premise does not hold: every read counts.
      def readsIn(topLevel: Boolean)(token: Token): Seq[Identifier] =
        if (topLevel) whereReadsOf(token, aliases) else readsOf(token)
      val leaves = ss.where.flatMap(_.criteria).toSeq.flatMap(whereLeaves(_, topLevel = true))
      val where = leaves.flatMap {
        case (e: Expression, topLevel)
            if carriesFunction(e.identifier) || e.maybeValue.exists(carriesFunction) =>
          val evaluatedIn =
            if (e.identifier.nested) scopeOf(e.identifier, aliases) else None
          outside(evaluatedIn)(readsIn(topLevel)(e)).map(Violation("WHERE", e.sql, _, evaluatedIn))
        case (s: SubqueryCriteria, topLevel) if s.outerIdentifiers.exists(carriesFunction) =>
          s.outerIdentifiers.flatMap { outer =>
            val evaluatedIn = if (outer.nested) scopeOf(outer, aliases) else None
            outside(evaluatedIn)(readsIn(topLevel)(outer))
              .map(Violation("WHERE", s.sql, _, evaluatedIn))
          }
        case _ => Nil
      }
      select ++ window ++ where
    }
  }

  /** The refusal `SingleSearch.validate()` returns, if any. */
  def refusal(ss: SingleSearch): Option[String] = violations(ss).headOption.map(_.message)
}
