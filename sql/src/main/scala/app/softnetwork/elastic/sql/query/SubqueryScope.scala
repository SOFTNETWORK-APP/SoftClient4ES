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

import app.softnetwork.elastic.sql.Identifier

/** Does a nested statement read a correlation name of an ENCLOSING statement?
  *
  * ONE detector, structural, on the AST the parser already built (story 22.1, consumed by stories
  * 22.2 and 22.3 — a second walk is the defect class this object exists to prevent).
  *
  * `GenericIdentifier.update` resolves a qualifier against the statement's OWN `aliasesToTable`
  * (FROM aliases, index names and UNNEST aliases); a qualifier that is NOT one of them is LEFT IN
  * THE NAME — `o.cid` keeps `name = "o.cid"`, `tableAlias = None`, `table = None`. That signature
  * IS the annotation: Elasticsearch would read such a name as an object path and match nothing,
  * with HTTP 200. So after the inner `update()`:
  *   - qualified-and-resolved (`tableAlias = Some(q)`): inner-scoped, never correlated;
  *   - qualified-and-unresolved: correlated iff the HEAD segment is a correlation name of an
  *     enclosing statement — innermost wins, a name the inner statement also declares shadows the
  *     outer one (SQL scoping);
  *   - bare (`cid`): NOT decided here. SQL resolves a bare name innermost-first, so it is assumed
  *     to be the inner column; story 22.2's seam re-checks it against both mappings.
  *
  * 🔴 A dotted path into an OBJECT field whose head happens to equal an enclosing alias is reported
  * as correlated. That is loud and the message says how to disambiguate — strictly better than the
  * alternative, which is reading an enclosing alias as an object path and answering zero rows with
  * HTTP 200.
  */
object SubqueryScope {

  // ── The scope model (story 22.3 AD-1) ────────────────────────────────────────────────────────
  //
  // A POST-PASS value object over the AST the parser already annotated — deliberately NOT a new
  // `Identifier` field. Annotating in `GenericIdentifier.update` would cost an arity change on a
  // 13-field case class read by three extensions files, a second place where `tableAlias`/`table`
  // get set (the `TemporalLiterals` "key language" obligation would gain a third speaker), and an
  // arm whose input — the ENCLOSING statement — is not in hand inside `update` at all. The
  // unresolved-qualifier-left-in-`name` signature IS the annotation; this object reads it.

  /** A source a name can resolve to inside ONE statement's FROM.
    *
    * `key` is the alias-map KEY (`From.tableAliases` / `From.aliasesToTable` key language — the
    * bare index unless the bare name is ambiguous, story 21.2 AD-6), `alias` is the correlation
    * name the statement writes, and `projection` is the column list when it is KNOWN without a
    * mapping. `None` there means OPAQUE — a plain index (no schema is attached at parse time) or a
    * `SELECT *` derived body — and an opaque source is never guessed at.
    *
    * 🔴 Any consumer comparing `key` against `Identifier.table` inherits the `joinSourceKeys`
    * obligation (`From.scala`, `TemporalLiterals`): the key of an ambiguous bare name is the
    * QUALIFIED reference, not the index name.
    */
  sealed trait ScopeSource {
    def key: String
    def alias: String
    def projection: Option[Seq[String]]
  }

  final case class PlainSource(key: String, alias: String) extends ScopeSource {
    override val projection: Option[Seq[String]] = None
  }

  final case class DerivedSource(alias: String, projection: Option[Seq[String]])
      extends ScopeSource {

    /** A derived table's key IS its alias (story 22.1 AD-1: `DerivedTable.name == alias.alias`). */
    override val key: String = alias
  }

  final case class UnnestSource(alias: String, path: String) extends ScopeSource {
    override val key: String = path
    override val projection: Option[Seq[String]] = None
  }

  /** One statement's scope: `depth` 0 is the statement itself, 1 its enclosing statement, and so on
    * outward.
    */
  final case class Scope(depth: Int, sources: Seq[ScopeSource]) {
    lazy val names: Set[String] = sources.flatMap(s => Seq(s.key, s.alias)).toSet
    def byName(n: String): Option[ScopeSource] = sources.find(s => s.alias == n || s.key == n)
  }

  /** The sources a statement's FROM declares, in the order it writes them (FROM items first, then
    * each JOIN leg).
    *
    * The key is taken from `From.aliasesToTable`, which is the LOSSLESS alias -> key direction
    * (story BIDC-8): reading `tableAliases` instead would lose one leg of a self-join, because that
    * map is keyed by TABLE and holds exactly one alias per key.
    */
  def scopeOf(s: SingleSearch, depth: Int = 0): Scope = {
    val from = s.from
    def keyOf(alias: String, fallback: String): String =
      from.aliasesToTable.getOrElse(alias, fallback)
    val fromSources: Seq[ScopeSource] = from.tables.map { t =>
      t.derived match {
        case Some(d) => DerivedSource(d.name, d.outputNames)
        case None =>
          val alias = t.tableAlias.map(_.alias).filter(_.nonEmpty).getOrElse(t.name)
          PlainSource(keyOf(alias, t.name), alias)
      }
    }
    val joinSources: Seq[ScopeSource] = from.joins.collect {
      case sj: StandardJoin =>
        sj.source match {
          case d: DerivedTable => DerivedSource(d.name, d.outputNames)
          case src =>
            val alias = sj.alias.map(_.alias).filter(_.nonEmpty).getOrElse(src.name)
            PlainSource(keyOf(alias, src.name), alias)
        }
      case u: Unnest =>
        UnnestSource(u.alias.map(_.alias).filter(_.nonEmpty).getOrElse(u.name), u.name)
    }
    Scope(depth, fromSources ++ joinSources)
  }

  /** The chain an INNER statement resolves against: its own scope first, then each enclosing
    * statement outward. `outers` is innermost-enclosing FIRST — the order `correlatedReferences`
    * accumulates scopes in.
    */
  def chain(inner: SingleSearch, outers: Seq[SingleSearch]): Seq[Scope] =
    scopeOf(inner, 0) +: outers.zipWithIndex.map { case (o, i) => scopeOf(o, i + 1) }

  sealed trait Resolution

  /** The identifier names `column` of `source`, `depth` scopes out (0 = the statement's own, so
    * anything >= 1 IS a correlated reference).
    */
  final case class Resolved(depth: Int, source: ScopeSource, column: String) extends Resolution

  /** More than one source of the innermost scope could own a bare name — never guessed. */
  case object Ambiguous extends Resolution

  /** A qualified name whose head is no correlation name in ANY scope: an object path into the
    * innermost source (`address.city`) or a typo. Parse time cannot tell them apart; the seam can,
    * when the inner mapping is loadable (AD-3).
    */
  case object Unresolved extends Resolution

  /** SQL scoping, innermost first.
    *
    * QUALIFIED names walk the chain outward and take the FIRST scope declaring the head, so an
    * inner declaration SHADOWS an outer one of the same name — story 22.2's rule, now a function,
    * which is what keeps the detector and this resolver from drifting apart.
    *
    * UN-QUALIFIED names resolve in the INNERMOST scope only, by four rules (story 22.4 AD-4,
    * subsumed here): (0) a lone source owns every bare name; (1) exactly one derived source
    * projects the name; (2) no derived source projects it, exactly one plain source, and no OPAQUE
    * derived source stands beside it; (3) else `Ambiguous`. A bare name is NEVER resolved outward:
    * SQL does that only when the inner source lacks the column, which needs a mapping — story
    * 22.2's PD-2 boundary, re-checked at the seam where the mappings are in hand.
    *
    * 🔴 Two DELIBERATE deviations, stated so a consumer does not inherit them by surprise:
    *   - rule (1) lets a derived source that projects the name win even when a plain source might
    *     also own it (`SELECT country FROM bi_events JOIN (SELECT country …) d` resolves `country`
    *     to `d`, where ANSI would say ambiguous). Recorded by story 22.4's review as an accepted
    *     SQL-92 deviation: only a derived table's projection is knowable without a mapping, and the
    *     ON-clause key resolution depends on it.
    *   - an [[UnnestSource]] takes part in NO un-qualified rule: it never projects, never counts as
    *     a plain source and never makes the scope opaque, so a bare name beside an UNNEST resolves
    *     to the plain leg rather than becoming `Ambiguous`. Qualified resolution DOES see it
    *     (`byName` searches every source).
    */
  def resolve(id: Identifier, chain: Seq[Scope]): Resolution =
    chain.headOption match {
      case None => Unresolved
      case Some(innermost) =>
        (id.tableAlias, id.table) match {
          case (Some(q), _) =>
            innermost.byName(q).map(Resolved(0, _, id.name)).getOrElse(Unresolved)
          case (None, Some(k)) =>
            innermost.byName(k).map(Resolved(0, _, id.name)).getOrElse(Unresolved)
          case (None, None) if id.name.contains(".") =>
            val parts = id.name.split("\\.", 2)
            val head = parts(0)
            val rest = parts(1)
            // `collectFirst`, not `.toStream.headOption`: `Stream` is deprecated on 2.13 and `sql`
            // is cross-built.
            chain.iterator
              .map(sc => sc.byName(head).map(src => Resolved(sc.depth, src, rest)))
              .collectFirst { case Some(r) => r }
              .getOrElse(Unresolved)
          case (None, None) if id.name.nonEmpty && id.name != "*" =>
            innermost.sources match {
              case Seq(lone) => Resolved(0, lone, id.name)
              case sources =>
                val projecting = sources.filter(_.projection.exists(_.contains(id.name)))
                val plain = sources.collect { case p: PlainSource => p }
                val opaque = sources.exists {
                  case d: DerivedSource => d.projection.isEmpty
                  case _                => false
                }
                (projecting, plain) match {
                  case (Seq(one), _)                 => Resolved(0, one, id.name)
                  case (Seq(), Seq(lone)) if !opaque => Resolved(0, lone, id.name)
                  case _                             => Ambiguous
                }
            }
          case _ => Unresolved // a literal, an ordinal or `*` — names no column
        }
    }

  /** Every name a statement's FROM makes addressable — the alias-map keys AND values (index names,
    * aliases, qualified keys) plus the UNNEST aliases.
    *
    * 🔴 Derived from [[scopeOf]] since story 22.3, and that is a FIX, not a tidy-up. It used to
    * read `from.tableAliases` directly, and that `ListMap` is keyed by TABLE, so it has ALREADY
    * collapsed two sources sharing an `aliasKey` by the time anyone reads it (story 21.2 AD-6 /
    * BIDC-8). MEASURED on `FROM orders o JOIN UNNEST(o.orders) AS i`: `tableAliases` is
    * `ListMap(orders -> i)`, so the alias `o` was NOT a correlation name, a body's `o.region` was
    * not reported as correlated, the statement did not route, and Elasticsearch read `o.region` as
    * an object path — zero rows, HTTP 200. `scopeOf` reads the LOSSLESS `aliasesToTable`, so both
    * legs survive.
    *
    * Widening is safe in BOTH directions: a name gained on the OUTER side makes a reference
    * correlated that would otherwise have been silently wrong, and a name gained on the INNER side
    * SHADOWS an outer one, which is what SQL specifies.
    */
  def correlationNames(s: SingleSearch): Set[String] = scopeOf(s).names

  def correlatedReferences(body: DqlStatement, outer: SingleSearch): Seq[Identifier] =
    correlatedReferences(body, correlationNames(outer))

  /** The Set-taking overload story 22.2 keeps (one detector, never two): the enclosing scopes are
    * ACCUMULATED as the walk descends, so a reference from two levels in to the OUTERMOST alias is
    * caught at the outermost statement too.
    */
  private[query] def correlatedReferences(
    body: DqlStatement,
    outerScopes: Set[String]
  ): Seq[Identifier] =
    body match {
      case inner: SingleSearch =>
        val innerNames = correlationNames(inner)
        val outerOnly = outerScopes -- innerNames // innermost wins
        val direct =
          if (outerOnly.isEmpty) Nil
          else
            inner.referencedIdentifiers.filter { id =>
              id.tableAlias.isEmpty && !id.nested && id.name.contains(".") &&
              outerOnly.contains(id.name.split("\\.", 2)(0))
            }
        // A derived table NESTED in this body, and (story 22.2) a WHERE SUBQUERY nested in it,
        // are walked with this statement's names added — so a reference two levels in to the
        // OUTERMOST alias is caught at the outermost `update()` too.
        val deeper = outerScopes ++ innerNames
        direct ++
        inner.from.derivedTables.values.toSeq.flatMap(d => correlatedReferences(d.query, deeper)) ++
        inner.whereSubqueries.flatMap(sq => correlatedReferences(sq.query, deeper))
      case multi: MultiSearch => multi.requests.flatMap(r => correlatedReferences(r, outerScopes))
      case _                  => Nil // a FROM-less body names no source and can reference nothing
    }

  /** LATERAL-shaped references: a DERIVED body (FROM or JOIN position) naming an ENCLOSING
    * correlation name.
    *
    * SQL-92 §7.6 gives a derived table NO access to the enclosing scope; only SQL:1999 `LATERAL`
    * does, and `lateral` is not a word this dialect knows. Without this arm the shape PARSES and
    * the relational engine (story 22.4) would execute the body as written — `WHERE cid = c.id`
    * reaching Elasticsearch as an object path, zero rows, HTTP 200 — which is exactly the
    * silent-wrong-answer mode epic 22 exists to close.
    *
    * Story 22.2/22.3 widen the walk to bodies nested inside WHERE subqueries; the shape of the
    * recursion is already here.
    */
  def lateralReferences(s: SingleSearch, outerScopes: Set[String] = Set.empty): Seq[Identifier] =
    lateralOffenders(s, outerScopes).map(_._2)

  /** The same walk, keeping the correlation name of the derived table whose body names each
    * offending identifier.
    *
    * 🔴 The pairing is not cosmetic. `correlatedReferences` recurses into derived tables NESTED in
    * a body, so a reference two levels in belongs to the INNER derived table, not to the outer one
    * a `find` over the top-level map would return — and the alias is the whole point of the
    * message. Returning the pair also removes the `getOrElse("")` that could render `derived table
    * ''`.
    */
  def lateralOffenders(
    s: SingleSearch,
    outerScopes: Set[String] = Set.empty
  ): Seq[(String, Identifier)] = {
    val here = outerScopes ++ correlationNames(s)
    s.from.derivedTables.values.toSeq.flatMap { d =>
      val inner: Seq[(String, Identifier)] = d.query match {
        // attribute a nested body's references to the NESTED derived table
        case body: SingleSearch => lateralOffenders(body, here)
        case _                  => Nil
      }
      val innerIds = inner.map(_._2).toSet
      correlatedReferences(d.query, here).filterNot(innerIds.contains).map(d.name -> _) ++ inner
    } ++
    // Story 22.3 (A2) — the SECOND half of the walk: a derived table nested inside a WHERE
    // SUBQUERY body is LATERAL too, and nothing saw it before.
    //
    // 🔴 It is not cosmetic and it is not optional. Until story 22.3b the shape was caught by
    // `SubqueryCriteria.commonChecks`' correlated arm — as a "Correlated subquery", the wrong
    // message — and 22.3b DELETES that arm. Without this line the deletion would turn
    // `WHERE c.id IN (SELECT x FROM (SELECT … WHERE o.cid = c.id) d)` from a loud rejection into an
    // accepted statement routed to the relational engine, which refuses a body carrying a derived
    // table anyway. The walk goes through `whereSubqueries` (story 22.2's NODE-level walker),
    // never through a second `Criteria` traversal of its own.
    s.whereSubqueries.flatMap(_.inner.toSeq).flatMap(i => lateralOffenders(i, here))
  }

  /** Story 22.2 (PD-2) — the BARE-name half of the correlation rule, decided by the two MAPPINGS at
    * the seam rather than structurally. A bare name has no alias to quote, so the message names the
    * two indices instead.
    */
  def bareCorrelatedMessage(name: String, innerIndex: String, outerIndex: String): String =
    s"Correlated subquery: '$name' is not a column of '$innerIndex' but is a column of " +
    s"'$outerIndex', so the subquery reads the outer row. An outer reference must be QUALIFIED " +
    "with the outer table's alias — a bare name is read as the subquery's own column, so this " +
    "statement cannot route to the relational engine as written. Qualify it, rewrite as a JOIN, " +
    "or make the subquery self-contained."

  /** Story 22.3 (AD-3) — a QUALIFIED name whose head names no source in ANY enclosing scope, and
    * which the inner index does not map as an object field either. Parse time cannot tell a typo
    * from an object path; the seam can, once the mapping is in hand, and the message then names
    * every scope it searched so the analyst can see which alias they meant to write.
    */
  def unresolvedMessage(id: Identifier, chain: Seq[Scope], context: String): String = {
    val head = id.name.split("\\.", 2)(0)
    s"'${id.name}' inside $context names no source in scope. " +
    // "visible at this level", not "every scope": the seam resolves one level at a time (an inner
    // body is executed through its own `resolveWithSchema`), so a three-deep nest reports the two
    // scopes THIS check actually searched rather than the whole chain.
    "Scopes searched (innermost first): " +
    chain
      .map(sc =>
        s"[${sc.depth}] " + sc.sources.map(src => s"${src.alias}=${src.key}").mkString(", ")
      )
      .mkString("; ") +
    s". If '$head' is an object field of the subquery's own table, qualify it with the subquery's " +
    "alias; if it is a column of an enclosing table, qualify it with that table's alias."
  }

  /** 🔴 The word LATERAL comes FIRST, and that is not a style choice. `GatewayApi.excerpt` caps a
    * parse-rejection reason at 200 characters and, above that, keeps the first 120 and the last 77
    * with an ellipsis BETWEEN them. MEASURED at the REPL against real Elasticsearch 8.18 while
    * writing this story: the previous wording was 241 characters and the elision landed exactly on
    * "this is LATERAL, which is not supported", so the user was told a derived table cannot
    * reference an outer alias and NEVER saw the name of the construct or why it is refused.
    *
    * Generalises to every `validate()` message: the terms a reader must see have to fit in the
    * first 120 characters, because the middle is what gets cut.
    */
  def lateralMessage(id: Identifier, derivedAlias: String): String =
    s"LATERAL is not supported: a derived table cannot reference an outer alias. '${id.name}' " +
    s"inside derived table '$derivedAlias' reads the enclosing FROM (SQL-92 §7.6). Move the " +
    "condition to the outer WHERE, or write it as a correlated WHERE subquery."
}
