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

import app.softnetwork.elastic.sql.{renderName, Alias, NamePart, Token}

/** One common table expression of a `WITH` clause (story 22.5): `<name> AS (<query>)`.
  *
  * `name` is the part exactly as written (bare or quoted -- ONE lexeme, never split, #85's rule);
  * `query` is what the grammar's `cteBody` produced (`SearchStatement | FromlessSelect`, already
  * `.update()`-d by `Parser.single`), with every EARLIER CTE of the same WITH list already
  * substituted into it by [[CteSubstitution.resolve]].
  *
  * Non-recursive by construction: a CTE body may reference the CTEs that PRECEDE it and nothing
  * else -- a reference to itself or to a later CTE is rejected at parse time.
  */
case class Cte(name: NamePart, query: DqlStatement) extends Token {

  override def sql: String = s"${renderName(Seq(name), name.value)} AS (${query.sql})"

  override def validate(): Either[String, Unit] = query match {
    case s: SearchStatement => s.validate()
    case f: FromlessSelect  => f.validate()
    case other =>
      Left(s"CTE '${name.value}' body must be a SELECT, got ${other.getClass.getSimpleName}")
  }
}

/** Story 22.5 -- turns `WITH ...` plus a parsed search statement into the statement stories 22.1
  * and 22.4 already know how to validate, route and plan: every bare, single-part FROM/JOIN
  * reference whose name is a CTE name becomes `Table(name = <alias>, derived =
  * Some(DerivedTable(body, alias, cte = Some(part))))`, recursively through literal derived-table
  * bodies and through every statement a WHERE / HAVING / JOIN `ON` criteria embeds.
  *
  * ONE owner of the rule: the parser calls it from the `withQuery` action, ONCE, on the fully
  * parsed statement, and `SingleSearch.validate()` only CHECKS that it ran
  * ([[SingleSearch.unsubstitutedCteReference]]). Substitution deliberately does NOT live in
  * `update()`, which re-runs on every executed statement (`SearchApi.resolveWithSchema` is
  * `copy(schema).update()`, `Table.mergeWithSearch` re-updates too): a rewrite there would need an
  * idempotency latch AND the WITH list would have to be present on every branch and every body at
  * update time. After this pass `ctes` is render/carry only and nothing in `update()` reads it.
  *
  * Rules (SQL-92 Clause 7.12 / 6.3, the non-recursive subset):
  *   1. CTE names are matched EXACTLY (case-sensitive, like every alias map in this AST) against
  *      `Table.name` / `StandardJoin.source.name` when the reference has AT MOST ONE part -- a
  *      qualified reference (`"sch".monthly`) is never a CTE reference (a qualifier names a venue's
  *      namespace, #85). 2. A CTE name SHADOWS an index of the same name for the statement's
  *      duration: the substitution is unconditional, so `FROM bi_events` reads the CTE `bi_events`
  *      if one is declared. Loud, not silent: the render re-emits the WITH list, so the user sees
  *      it. 3. Scope is LEFT-TO-RIGHT and non-recursive: CTE k's body sees CTEs 1..k-1. A body
  *      naming itself or a later CTE is REJECTED, never read as an index of that name. 4. Duplicate
  *      CTE names are rejected. An UNREFERENCED CTE is allowed (its body still validates). A CTE
  *      referenced twice yields two marked derived tables sharing ONE body value (inline semantics
  * -- executed twice by the planner). 5. A reference that carries its own alias keeps it as the
  * derived table's correlation name (`FROM monthly m` => `DerivedTable(body, Alias("m"), cte =
  * Some(monthly))`); a reference whose alias EQUALS the CTE name is normalised to the canonical
  * `Alias(part.value, part.quoted)` so `FROM monthly AS monthly`, `FROM "monthly" AS monthly` and
  * `FROM monthly` are ONE AST and render as `FROM monthly` -- the fixed point needs a canonical
  * form.
  */
object CteSubstitution {

  /** Resolve the WITH list: duplicates and forward/self references rejected, earlier CTEs
    * substituted into later bodies, each rewritten body re-`update()`-d.
    */
  def resolve(ctes: Seq[Cte]): Either[String, Seq[Cte]] = {
    val names = ctes.map(_.name.value)
    names.diff(names.distinct).distinct.headOption match {
      case Some(dup) => Left(s"CTE '$dup' is defined more than once in the WITH clause")
      case None =>
        ctes.zipWithIndex
          .foldLeft(Right(Vector.empty[Cte]): Either[String, Vector[Cte]]) {
            case (left @ Left(_), _) => left
            case (Right(done), (cte, k)) =>
              val laterOrSelf = names.drop(k).toSet
              referencedNames(cte.query).find(laterOrSelf.contains) match {
                case Some(n) if n == cte.name.value =>
                  // 🔴 Say what HAPPENED, not what it resembles. "recursive CTEs are not
                  // supported" is FALSE for the commonest analyst idiom that lands here —
                  // `WITH orders AS (SELECT id FROM orders WHERE id > 1) SELECT * FROM orders`
                  // contains no recursion at all; the author meant the BASE TABLE, which is what
                  // ANSI/PostgreSQL/DuckDB bind it to (a non-recursive CTE is not in scope inside
                  // its own body). This engine takes rule 3 instead and refuses it, so the message
                  // has to name the real cause and the real remedy. The load-bearing terms sit in
                  // the first 120 characters because `GatewayApi.excerpt` elides the MIDDLE.
                  Left(
                    s"A CTE body may not name the CTE itself: '$n' is referenced inside its own " +
                    s"definition. Rename the CTE, e.g. WITH ${n}_f AS (SELECT ... FROM $n ...). " +
                    "(Recursive CTEs, WITH RECURSIVE, are a separate unsupported feature.)"
                  )
                case Some(n) =>
                  Left(
                    s"CTE '${cte.name.value}' references CTE '$n', which is defined later in the " +
                    "WITH clause; a CTE may only reference the CTEs before it"
                  )
                case None =>
                  val scope = done.map(c => c.name.value -> c).toMap
                  val rewritten = substituteStatement(cte.query, scope)
                  Right(
                    done :+ (if (rewritten eq cte.query) cte
                             else cte.copy(query = updated(rewritten)))
                  )
              }
          }
          .map(_.toSeq)
    }
  }

  /** Substitute the (already resolved) WITH list into a top-level search statement. The list is
    * attached to the FIRST branch (a `UNION ALL` statement's WITH clause is textually attached to
    * its first SELECT and `MultiSearch.sql` concatenates branch renders); every branch is rewritten
    * and re-`update()`-d.
    *
    * `requests` is non-empty by construction (`rep1sep`); the `headOption` guard is for a
    * programmatic `MultiSearch(Nil)`, which is returned untouched rather than thrown on.
    */
  def apply(ctes: Seq[Cte], body: SearchStatement): Either[String, SearchStatement] =
    resolve(ctes).flatMap { resolved =>
      val scope = resolved.map(c => c.name.value -> c).toMap
      body match {
        case s: SingleSearch =>
          Right(substituteSingle(s, scope).copy(ctes = resolved).update())
        case m: MultiSearch =>
          val branches = m.requests.map(b => substituteSingle(b, scope))
          branches.headOption match {
            case Some(h) =>
              Right(
                m.copy(requests = h.copy(ctes = resolved).update() +: branches.tail.map(_.update()))
              )
            // `requests` is non-empty by construction (`rep1sep`); this guards a programmatic
            // `MultiSearch(Nil)`, which has no branch 0 to carry the list -- so it is refused for
            // the same reason the arm below is, rather than returned with the list dropped.
            case None =>
              Left("A WITH clause cannot be attached to a MultiSearch with no branches")
          }
        case other =>
          // 🔴 NEVER `case other => other`. This method is PUBLIC, and
          // `SingleSearch.unsubstitutedCteReference` names it in the very message it gives an
          // embedder ("build the statement through Parser or CteSubstitution"), so a caller can
          // and will reach this arm. Returning the statement unchanged DROPS the WITH list BEFORE
          // it is ever attached, and every safety net this story has keys on `ctes.nonEmpty` — the
          // closure guard, `unsubstitutedCteReference`, `RelationalClosureGuard.shapeOf` and the
          // render all go quiet at once. MEASURED on the `case other => other` form:
          // `CteSubstitution(Seq(Cte("a", …)), SelectStatement("SELECT x FROM a"))` answered
          // `Right`, `validate()` passed, the render lost the WITH clause, and `sources` was
          // `List(a)` — i.e. the statement would have executed against an INDEX named after the
          // CTE. Loud beats silent (story 22.3b's rule).
          //
          // `SelectStatement` cannot carry a substitution at all: its `query` is a SQL STRING and
          // its `statement` is a LAZY RE-PARSE of that string, so a `WITH` written inside it is
          // already substituted by `Parser` on the way through and never needs this method.
          Left(
            s"A WITH clause cannot be attached to a ${other.getClass.getSimpleName}: it carries " +
            "the statement as SQL text, which is re-parsed on use. Write the WITH clause inside " +
            "that text and let Parser substitute it."
          )
      }
    }

  /** Bare single-part table names a statement references in FROM/JOIN position, recursively through
    * literal derived-table bodies AND through every statement embedded in a WHERE / HAVING / JOIN
    * `ON` criteria ([[Criteria.embeddedStatements]]) -- the input of the forward-reference check.
    *
    * ONE walk feeds both that check and [[SingleSearch.unsubstitutedCteReference]], so the rewrite
    * and the guard cannot disagree about where a reference may hide.
    */
  private[query] def referencedNames(q: DqlStatement): Seq[String] = q match {
    case s: SingleSearch =>
      s.from.tables.flatMap(tableNames) ++ embeddedStatements(s).flatMap(referencedNames)
    case m: MultiSearch => m.requests.flatMap(referencedNames)
    case _              => Nil
  }

  /** Every statement a `SingleSearch` embeds in a CRITERIA position: WHERE, HAVING and each JOIN
    * `ON`. Reads the structural hook on `Criteria`, so a criteria kind that carries a statement is
    * walked here by construction.
    */
  def embeddedStatements(s: SingleSearch): Seq[DqlStatement] =
    s.where.flatMap(_.criteria).toSeq.flatMap(_.embeddedStatements) ++
    s.having.flatMap(_.criteria).toSeq.flatMap(_.embeddedStatements) ++
    s.from.joins
      .collect { case sj: StandardJoin => sj }
      .flatMap(_.on.toSeq)
      .flatMap(_.criteria.embeddedStatements)

  private def tableNames(t: Table): Seq[String] = {
    val own = t.derived match {
      case Some(d) if d.cte.isEmpty  => referencedNames(d.query)
      case Some(_)                   => Nil // an already-substituted reference
      case None if t.parts.size <= 1 => Seq(t.name)
      case None                      => Nil
    }
    own ++ t.joins.flatMap {
      case sj: StandardJoin =>
        sj.source match {
          case d: DerivedTable if d.cte.isEmpty => referencedNames(d.query)
          case _: DerivedTable                  => Nil
          case s if sj.parts.size <= 1          => Seq(s.name)
          case _                                => Nil
        }
      case _ => Nil // UNNEST names a column path, not a table
    }
  }

  private def substituteStatement(q: DqlStatement, scope: Map[String, Cte]): DqlStatement =
    q match {
      case s: SingleSearch => substituteSingle(s, scope)
      case m: MultiSearch =>
        val branches = m.requests.map(b => substituteSingle(b, scope))
        if (branches.zip(m.requests).forall { case (a, b) => a eq b }) m
        else m.copy(requests = branches.map(_.update()))
      case other => other
    }

  /** Substitute the FROM/JOIN tree AND every statement embedded in a criteria (WHERE / HAVING /
    * JOIN `ON`). The result is NOT yet updated -- the callers decide when (`apply` updates the top
    * level; a rewritten BODY is updated by the caller that rewrote it).
    *
    * `eq`-preserving: a statement in which nothing matched is returned AS IS, so a body that
    * references no CTE is never touched and keeps its parse-time update.
    */
  private def substituteSingle(s: SingleSearch, scope: Map[String, Cte]): SingleSearch =
    if (scope.isEmpty) s
    else {
      val tables = s.from.tables.map(t => substituteTable(t, scope))
      val where = s.where.map(w => w.copy(criteria = w.criteria.map(mapCriteria(_, scope))))
      val having = s.having.map(h => h.copy(criteria = h.criteria.map(mapCriteria(_, scope))))
      val untouched =
        tables.zip(s.from.tables).forall { case (a, b) => a eq b } &&
        same(where.flatMap(_.criteria), s.where.flatMap(_.criteria)) &&
        same(having.flatMap(_.criteria), s.having.flatMap(_.criteria))
      if (untouched) s
      else s.copy(from = s.from.copy(tables = tables), where = where, having = having)
    }

  private def same(a: Option[Criteria], b: Option[Criteria]): Boolean = (a, b) match {
    case (Some(x), Some(y)) => x eq y
    case (None, None)       => true
    case _                  => false
  }

  private def mapCriteria(c: Criteria, scope: Map[String, Cte]): Criteria =
    c.mapEmbeddedStatements(q => substituteStatement(q, scope))

  /** Rule 5's canonical form: an alias that merely repeats the CTE name is NOT information, and
    * keeping the written spelling would make `FROM "m" AS m` and `FROM "m"` two different ASTs with
    * the same render -- a fixed point that fails for one of them.
    */
  private def canonicalAlias(part: NamePart, written: Option[Alias]): Alias =
    written match {
      case Some(a) if a.alias.nonEmpty && a.alias != part.value => a
      case _                                                    => Alias(part.value, part.quoted)
    }

  private def substituteTable(t: Table, scope: Map[String, Cte]): Table = {
    val joins = t.joins.map(j => substituteJoin(j, scope))
    val joinsUntouched = joins.zip(t.joins).forall { case (a, b) => a eq b }
    t.derived match {
      case Some(d) if d.cte.isEmpty =>
        // A LITERAL derived table: its body may reference a CTE of the enclosing scope.
        val body = substituteStatement(d.query, scope)
        if ((body eq d.query) && joinsUntouched) t
        else t.copy(joins = joins, derived = Some(d.copy(query = updated(body))))
      case Some(_) => if (joinsUntouched) t else t.copy(joins = joins)
      case None if t.parts.size <= 1 && scope.contains(t.name) =>
        val cte = scope(t.name)
        val alias = canonicalAlias(cte.name, t.tableAlias)
        Table(alias.alias, None, joins, Nil, Some(DerivedTable(cte.query, alias, Some(cte.name))))
      case None => if (joinsUntouched) t else t.copy(joins = joins)
    }
  }

  private def substituteJoin(j: Join, scope: Map[String, Cte]): Join = j match {
    case sj: StandardJoin =>
      // The ON criteria may embed a statement that references a CTE -- walk it too.
      val on = sj.on.map(o => o.copy(criteria = mapCriteria(o.criteria, scope)))
      val onUntouched = on.zip(sj.on).forall { case (a, b) => a.criteria eq b.criteria }
      val withOn = if (onUntouched) sj else sj.copy(on = on)
      sj.source match {
        case d: DerivedTable if d.cte.isEmpty =>
          val body = substituteStatement(d.query, scope)
          if (body eq d.query) withOn else withOn.copy(source = d.copy(query = updated(body)))
        case _: DerivedTable => withOn
        case src if sj.parts.size <= 1 && scope.contains(src.name) =>
          val cte = scope(src.name)
          val alias = canonicalAlias(cte.name, sj.alias)
          withOn.copy(
            source = DerivedTable(cte.query, alias, Some(cte.name)),
            alias = None,
            parts = Nil
          )
        case _ => withOn
      }
    case other => other
  }

  /** Re-resolve a body whose FROM tree was rewritten: its `tableAliases` changed, so every
    * identifier that resolved against the old source has to resolve again.
    */
  private def updated(q: DqlStatement): DqlStatement = q match {
    case s: SingleSearch => s.update()
    case m: MultiSearch  => m.update()
    case other           => other
  }
}
