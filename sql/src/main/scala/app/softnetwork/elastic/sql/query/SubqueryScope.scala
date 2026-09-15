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

  /** Every name a statement's FROM makes addressable: the alias-map keys AND values (index names,
    * aliases, qualified keys) plus the UNNEST aliases.
    */
  def correlationNames(s: SingleSearch): Set[String] =
    (s.from.tableAliases.keys ++ s.from.tableAliases.values ++ s.from.unnestAliases.keys).toSet

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
    }
  }

  /** Story 22.2 — the message a CORRELATED WHERE subquery is refused with. It names the offending
    * reference, the outer alias it reads, the story that will execute it, and the two rewrites that
    * work today — plus the object-field disambiguation, because a dotted path into an object field
    * whose head happens to equal an outer alias lands here too (loud beats zero rows with HTTP
    * 200).
    */
  def correlatedMessage(id: Identifier, node: Criteria): String = {
    val alias = id.name.split("\\.", 2)(0)
    s"Correlated subquery: '${id.name}' reads the outer alias '$alias' inside ${node.sql}. " +
    "Correlated subqueries require the relational engine (story 22.3, softclient4es-arrow-extensions) " +
    "and are not executed yet. Rewrite as a JOIN, make the subquery self-contained, or " +
    s"if '$alias' is an object field of the inner table, qualify it with the inner table's alias."
  }

  /** Story 22.2 (PD-2) — the BARE-name half of the correlation rule, decided by the two MAPPINGS at
    * the seam rather than structurally. A bare name has no alias to quote, so the message names the
    * two indices instead.
    */
  def bareCorrelatedMessage(name: String, innerIndex: String, outerIndex: String): String =
    s"Correlated subquery: '$name' is not a column of '$innerIndex' but is a column of " +
    s"'$outerIndex', so the subquery reads the outer row. Correlated subqueries require the " +
    "relational engine (story 22.3, softclient4es-arrow-extensions) and are not executed yet. " +
    "Rewrite as a JOIN, or make the subquery self-contained."

  def lateralMessage(id: Identifier, derivedAlias: String): String =
    s"A derived table cannot reference an outer alias: '${id.name}' inside derived table " +
    s"'$derivedAlias' reads the enclosing FROM (SQL-92 §7.6; this is LATERAL, which is not " +
    "supported). Move the condition to the outer WHERE or write it as a correlated WHERE subquery."
}
