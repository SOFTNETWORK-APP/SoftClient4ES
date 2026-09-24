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

package app.softnetwork.elastic.sql

import app.softnetwork.elastic.sql.`type`.{SQLType, SQLTypeUtils, SQLTypes}
import app.softnetwork.elastic.sql.operator.{SetOperator, UNION}
import app.softnetwork.elastic.sql.schema.{
  sqlConfig,
  validateScriptReferences,
  Column,
  IngestPipeline,
  IngestPipelineType,
  IngestProcessor,
  IngestProcessorType,
  PartitionDate,
  RemoveProcessor,
  RenameProcessor,
  ScriptProcessor,
  SetProcessor,
  Table => Schema,
  TableType
}
import app.softnetwork.elastic.sql.function.{FunctionChain, FunctionUtils}
import app.softnetwork.elastic.sql.function.cond.{Case, NullIf}
import app.softnetwork.elastic.sql.function.aggregate.WindowFunction
import app.softnetwork.elastic.sql.policy.{EnrichPolicy, EnrichPolicyType}
import app.softnetwork.elastic.sql.serialization._
import app.softnetwork.elastic.sql.transform.{
  Delay,
  Frequency,
  TransformTimeInterval,
  TransformTimeUnit
}
import app.softnetwork.elastic.sql.watcher.{
  Watcher,
  WatcherAction,
  WatcherCondition,
  WatcherInput,
  WatcherTrigger
}
import com.fasterxml.jackson.databind.JsonNode

import java.time.{Duration, Instant}
import scala.collection.immutable.ListMap

package object query {

  /** The `SingleSearch`es a statement EMBEDS — the ONE place these arms are spelled.
    *
    * Every closure-shaped question ("does this statement need the relational engine?", "does it
    * carry a derived table?") is a fold over this, never a second `match`: a second list of arms is
    * the desync class story 21.3 paid for. `Delete`, `CreateMaterializedView` and the watcher are
    * deliberately NOT arms — they reject closure shapes in the grammar or in `validate()`, so they
    * never reach a router carrying one.
    */
  def closureSearches(statement: Statement): Seq[SingleSearch] =
    embeddedSearchStatements(statement).flatMap {
      case s: SingleSearch => Seq(s)
      case m: MultiSearch  => m.requests
      case _               => Nil
    }

  /** The `SearchStatement`s a statement EMBEDS, AS WRITTEN — a `MultiSearch` stays whole here.
    *
    * 🔴 This is where the arms live; [[closureSearches]] derives from it by flattening a set
    * operation into its branches. Story 22.6 needs BOTH views and neither may be a second `match`:
    * the operator list is a property of the `MultiSearch` itself, invisible once it is flattened,
    * while every per-branch question (a derived table, a CTE, a correlated subquery) reads the
    * branches.
    */
  private def embeddedSearchStatements(statement: Statement): Seq[SearchStatement] =
    statement match {
      case s: SingleSearch      => Seq(s)
      case m: MultiSearch       => Seq(m)
      case sel: SelectStatement => sel.statement.toSeq.flatMap(embeddedSearchStatements)
      case ins: Insert          => ins.values.left.toOption.toSeq.flatMap(embeddedSearchStatements)
      case ct: CreateTable      => ct.ddl.left.toOption.toSeq.flatMap(embeddedSearchStatements)
      case _                    => Nil
    }

  /** Epic 22 AD-4 — the statement needs the relational engine (a cross-index JOIN, a derived table,
    * a CTE, a correlated subquery, or — story 22.6 — a set operator other than `UNION ALL`).
    *
    * 🔴 Reads the SET-OPERATION level, not only the branches: `SELECT a FROM t UNION SELECT a FROM
    * u` needs the engine although every branch alone is plain Elasticsearch, and flattening through
    * [[closureSearches]] would lose exactly that.
    */
  def relationalClosureRequired(statement: Statement): Boolean =
    embeddedSearchStatements(statement).exists {
      case s: SingleSearch => s.relationalClosureRequired
      case m: MultiSearch  => m.relationalClosureRequired
      case _               => false
    }

  /** Story 22.6 — a set operation other than `UNION ALL` is present. Read by the rejection message
    * (the shape the analyst wrote) and by the MATERIALIZED VIEW guard; routing reads
    * [[relationalClosureRequired]] only.
    */
  def setOperationsPresent(statement: Statement): Boolean =
    embeddedSearchStatements(statement).exists {
      case m: MultiSearch => !m.isUnionAllOnly
      case _              => false
    }

  /** Narrower than [[relationalClosureRequired]]: a derived table specifically. Read by the
    * rejection message (the remedy differs — a JOIN can be rewritten by hand, a derived table is
    * usually emitted by a BI tool the user does not control) and by the MATERIALIZED VIEW guard.
    */
  def derivedTablesPresent(statement: Statement): Boolean =
    closureSearches(statement).exists(_.hasDerivedTables)

  /** Story 22.5 — the statement carries a `WITH` clause. Narrower than
    * [[relationalClosureRequired]] and read for the SAME reason as [[derivedTablesPresent]]: the
    * rejection message names the shape the user actually wrote, and the remedy differs.
    */
  def ctesPresent(statement: Statement): Boolean =
    closureSearches(statement).exists(_.hasCtes)

  /** Story 22.2 — the statement carries a WHERE subquery somewhere. Read by the MATERIALIZED VIEW
    * and WATCHER guards, which must refuse one: both render the SELECT into an Elasticsearch
    * artefact (a transform, a watcher input) WITHOUT crossing `SearchApi.resolveWithSchema`, so the
    * two-phase rewrite never runs for them and an unresolved node would reach the query builder.
    */
  def whereSubqueriesPresent(statement: Statement): Boolean =
    closureSearches(statement).exists(_.hasWhereSubqueries)

  sealed trait Statement extends Token

  sealed trait DqlStatement extends Statement

  sealed trait SearchStatement extends DqlStatement {
    def explodeNested: Boolean
    def withoutNestedExplosion: SearchStatement
  }

  /** Select Statement wrapper
    * @param query
    *   - the SQL query
    * @param score
    *   - optional minimum score for the elasticsearch query
    */
  case class SelectStatement(
    query: SQL,
    score: Option[Double] = None,
    explodeNested: Boolean = true
  ) extends SearchStatement {
    import app.softnetwork.elastic.sql.SQLImplicits._

    override def withoutNestedExplosion: SelectStatement = this.copy(explodeNested = false)

    lazy val statement: Option[SearchStatement] = {
      queryToStatement(query) match {
        case Some(s: SearchStatement) => Some(if (!explodeNested) s.withoutNestedExplosion else s)
        case _                        => None
      }
    }

    override def sql: SQL =
      statement match {
        case Some(value) => value.sql
        case None        => query
      }

    def minScore(score: Double): SelectStatement = this.copy(score = Some(score))
  }

  case class SingleSearch(
    select: Select = Select(),
    from: From,
    where: Option[Where],
    groupBy: Option[GroupBy] = None,
    having: Option[Having] = None,
    orderBy: Option[OrderBy] = None,
    limit: Option[Limit] = None,
    score: Option[Double] = None,
    deleteByQuery: Boolean = false,
    updateByQuery: Boolean = false,
    onConflict: Option[OnConflict] = None,
    schema: Option[Schema] = None,
    explodeNested: Boolean = true,
    schemas: Map[String, Schema] = Map.empty,
    /** Story 22.5 — the statement's WITH list, in order, each body already resolved against the
      * CTEs before it. Carried for the RENDER (`sql` re-emits it), for `validate()` (an
      * UNREFERENCED CTE's body is still validated) and for the routing predicate
      * ([[relationalClosureRequired]]). The FROM/JOIN tree carries the REFERENCES as marked derived
      * tables (`DerivedTable.cte`), substituted ONCE at parse time by [[CteSubstitution]] — never
      * here, never in `update()`.
      *
      * For a `UNION ALL` statement the list lives on the FIRST branch only (the WITH clause is
      * textually attached to the first SELECT and `MultiSearch.sql` concatenates branch renders);
      * every branch's tree carries the substituted references. `MultiSearch`'s arity is unchanged.
      */
    ctes: Seq[Cte] = Nil
  ) extends SearchStatement {

    /** The rendered `WITH` prefix, or `""` (story 22.5). Split out of [[sql]] so story 22.6's
      * parenthesised set-operation render can HOIST it: `(WITH x AS (…) SELECT …) UNION (…)` cannot
      * parse, because a set-operation branch is a `single` and `single` has no `WITH`. ONE
      * derivation — [[sql]] is `ctePrefix + sqlWithoutCtes`.
      */
    lazy val ctePrefix: String =
      if (ctes.isEmpty) "" else ctes.map(_.sql).mkString("WITH ", ", ", " ")

    /** This statement rendered WITHOUT its `WITH` prefix — the body a parenthesised set-operation
      * branch renders as. Identical to [[sql]] whenever `ctes` is empty.
      */
    lazy val sqlWithoutCtes: String =
      s"$select$from${asString(where)}${asString(groupBy)}${asString(having)}${asString(
        orderBy
      )}${asString(limit)}${asString(onConflict)}"

    override def sql: String = ctePrefix + sqlWithoutCtes

    override def withoutNestedExplosion: SingleSearch = this.copy(explodeNested = false)

    lazy val fieldAliases: ListMap[String, String] = select.fieldAliases
    lazy val tableAliases: ListMap[String, String] = from.tableAliases

    /** correlation name -> derived table (story 22.1). */
    lazy val derivedTables: ListMap[String, DerivedTable] = from.derivedTables

    /** Read THIS, not `from.hasDerivedTables`, for the same reason as
      * [[relationalClosureRequired]]: story 22.5 makes a CTE reference a derived table that a
      * `From` alone cannot see.
      */
    lazy val hasDerivedTables: Boolean = derivedTables.nonEmpty

    /** Epic 22 AD-4 — THE predicate every venue routes on. Read THIS, never `from.` directly: it
      * carries constructs a `From` cannot see (story 22.3's correlated subqueries; story 22.5's
      * CTEs next) and a consumer reading the FROM member would silently miss them.
      *
      * 🔴 The predicate family, stated ONCE for whoever lands last of 22.3 / 22.5 / 22.6: each
      * story adds exactly ONE disjunct HERE, the package function `relationalClosureRequired` reads
      * THIS member, and `MultiSearch` folds its branches through it. Any spec that re-derives the
      * disjunction somewhere else is the story-21.3 desync class.
      */
    lazy val relationalClosureRequired: Boolean =
      from.relationalClosureRequired || hasCorrelatedSubqueries || ctes.nonEmpty

    /** Story 22.5 — `ctes.nonEmpty` is a disjunct of [[relationalClosureRequired]] even when NO CTE
      * is referenced, and that is deliberate rather than lazy: the regex classifier
      * (`JoinDetector.CtePattern`, arrow) keys on the statement's leading token and CANNOT count
      * references, while `JoinDetectorSpec`'s anti-drift property asserts that the classifier and
      * this AST predicate agree on every row. A statement whose CTEs are all unreferenced plans at
      * the engine as ONE plain leg — correct, and rare enough that a second rule would cost more
      * than the round trip it saves.
      */
    lazy val hasCtes: Boolean = ctes.nonEmpty

    /** Every WHERE-subquery node this statement carries, in statement order (story 22.2).
      *
      * 🔴 NOT part of [[relationalClosureRequired]], and that is the epic's routing rule, not an
      * oversight: an UNCORRELATED WHERE subquery executes ES-natively in core at EVERY venue (lead
      * ruling OQ-1), so such a statement is PASSTHROUGH — `relationalClosureRequired == false` and
      * `hasWhereSubqueries == true`. Story 22.3 widens the closure predicate with the CORRELATED
      * ones only.
      */
    lazy val whereSubqueries: Seq[SubqueryCriteria] =
      where.flatMap(_.criteria).map(_.subqueries).getOrElse(Nil)

    /** The ONE boolean `SearchApi.resolveWithSchema` tests per statement: decided once, on the AST,
      * so a statement without a subquery pays nothing (`feedback_no_per_row_hot_path_work`).
      */
    lazy val hasWhereSubqueries: Boolean = whereSubqueries.nonEmpty

    /** The TOP-LEVEL WHERE-subquery nodes whose subtree reads an enclosing scope (story 22.3).
      *
      * 🔴 Read what `correlatedRefs` MEANS, not its name (AD-2): story 22.2's walk recurses into a
      * body's own subqueries with the scopes ACCUMULATED, so a reference from the innermost body to
      * the MIDDLE body's alias — not correlated to THIS statement at all — is still reported on the
      * top-level node. `correlatedRefs.nonEmpty` therefore means "this node's SUBTREE reads outside
      * its own body", which is exactly the routing question: a middle body whose inner is
      * correlated to it cannot run ES-natively either.
      */
    lazy val correlatedSubqueries: Seq[SubqueryCriteria] =
      whereSubqueries.filter(_.correlatedRefs.nonEmpty)

    /** DEEP (story 22.3 AC 5): any WHERE-subquery node at any nesting depth whose subtree reads an
      * enclosing scope. A correlated body two levels down makes the MIDDLE body un-executable
      * ES-natively, so the WHOLE statement routes to the relational engine.
      *
      * The `s.inner.exists(...)` half is redundant with the accumulated walk described above and is
      * kept deliberately: it costs one already-computed boolean and it does not depend on the
      * walk's reach staying what it is today.
      */
    lazy val hasCorrelatedSubqueries: Boolean =
      whereSubqueries.exists(s =>
        s.correlatedRefs.nonEmpty || s.inner.exists(_.hasCorrelatedSubqueries)
      )

    /** Every identifier this statement NAMES, across every clause that can carry one — the SELECT
      * list (through each item's function chain), WHERE, HAVING, GROUP BY, ORDER BY and each
      * standard JOIN's ON.
      *
      * ONE list, several consumers (`derivedScopeCheck` here, `SubqueryScope`'s correlation walk,
      * story 22.2's subquery detector): a second enumeration of the clauses is the
      * one-key-two-derivations drift story 21.3 paid for four times.
      */
    lazy val referencedIdentifiers: Seq[Identifier] =
      select.fields.flatMap(f => FunctionUtils.funIdentifiers(f.identifier)) ++
      where.flatMap(_.criteria).map(_.referencedIdentifiers).getOrElse(Nil) ++
      having.flatMap(_.criteria).map(_.referencedIdentifiers).getOrElse(Nil) ++
      groupBy.map(_.buckets.map(_.identifier)).getOrElse(Nil) ++
      orderBy.map(_.sorts.map(_.field)).getOrElse(Nil) ++
      from.joins
        .collect { case sj: StandardJoin => sj }
        .flatMap(_.on.toSeq)
        .flatMap(_.criteria.referencedIdentifiers)

    /** alias -> table KEY, lossless (story BIDC-8): the map to consult when resolving a qualifier.
      * `tableAliases` (table -> alias) cannot hold two aliases of one table.
      */
    lazy val aliasesToTable: ListMap[String, String] = from.aliasesToTable
    lazy val unnestAliases: ListMap[String, (String, Option[Limit])] = from.unnestAliases

    /** Bucket lookup, keyed by EVERY spelling a bucket can be referenced with.
      *
      * Two consumers, and they ask different questions, which is why the map is DUAL-keyed:
      *   - `Identifier.update` builds the `BucketPath` by looking each GROUP BY bucket up under the
      *     spelling the statement WROTE (`b.identifier.identifierName` -- `"1"` for an ordinal,
      *     `"pays"` for an alias);
      *   - every SELECT / ORDER BY identifier looks its own bucket up under the RESOLVED column
      *     name (`request.bucketNames.get(identifierName)`).
      *
      * 🔴 OBLIGATION, not a reassurance: anything that looks a bucket up in this map must speak one
      * of those two key languages. Adding a resolution rule here (the ordinal and alias arms below)
      * without indexing BOTH spellings silently disables one of the two lookups -- which is exactly
      * how `GROUP BY pays` came to emit `terms { field: "pays" }` on a non-existent field. The
      * consumers are `sql/.../package.scala` (`Identifier.update`, 6 sites) and
      * `sql/.../function/aggregate/package.scala` (`WindowFunction.update`, 2 sites).
      *
      * Issue #253's companion defect: ordinal-ness used to be sniffed with `"\\d+".r.findFirstIn`
      * on the RENDERED name, guarded only by "the name contains no space", so ANY name carrying a
      * digit was a candidate -- `GROUP BY city2` indexed `select.fields(1)` on a one-column SELECT
      * and crashed `Parser.apply` with `IndexOutOfBoundsException`. This method is ON THE PARSE
      * PATH (`Identifier.update` forces it unconditionally whenever a GROUP BY is present) and it
      * runs BEFORE `Bucket.update`, so it -- not `Bucket.update`'s bounds check -- is what actually
      * crashed `GROUP BY 0` and `GROUP BY 99`. Ordinal-ness is now `Bucket.ordinalOf` on the AST,
      * and an out-of-range position is left unresolved for `Bucket.validate` to reject.
      *
      * ⚠️ The resolved identifier is deliberately NOT `.update(this)`-d: `Identifier.update` forces
      * `bucketNames` itself, so updating here would recurse into this very lazy val. `bucketNames`
      * builds LOOKUP KEYS; `Bucket.update` owns the resolution that reaches the emitted query, and
      * both route through `Bucket.selectItem` / `Bucket.aliasItem` so they cannot disagree.
      */
    lazy val bucketNames: ListMap[String, Bucket] = ListMap(buckets.flatMap { b =>
      val name = b.identifier.identifierName
      val resolved: Option[Identifier] =
        (b.ordinal match {
          case Some(position) => Bucket.selectItem(position, this)
          case None           => Bucket.aliasItem(b.identifier, this)
        }).map(_.identifier)
      resolved match {
        case Some(identifier) =>
          val resolvedName = identifier.identifierName
          // Carry the SELECT alias so this copy's `name` equals the real bucket's -- see the
          // OBLIGATION above; `Identifier.update` reads the same `fieldAliases` map.
          val display = fieldAliases.get(resolvedName)
          val named = identifier match {
            case g: GenericIdentifier => g.copy(fieldAlias = display.orElse(g.fieldAlias))
            case other                => other
          }
          val updated = b.copy(identifier = named)
          ListMap(
            (Seq(
              name             -> updated, // the spelling the statement was written with
              resolvedName     -> updated // the resolved column name
            ) ++ display.map(_ -> updated)): _* // the OUTPUT name a HAVING or WHERE would use
          )
        case None =>
          // The bucket was NOT substituted -- but it still has to be named the way the real bucket
          // is, or `Expression.includes` (which compares by DISPLAY name) stops matching and a
          // HAVING is silently dropped. `SELECT category AS cat ... GROUP BY category HAVING cat
          // <> 'x'` lost its `exclude` for exactly this reason: the copy was named `category` while
          // the bucket that reaches Elasticsearch is named `cat`. Broken on main too, but leaving
          // it made whether a HAVING is honoured depend on whether the GROUP BY spelled the alias
          // or the column, which is worse than uniformly broken.
          val display = fieldAliases.get(name)
          val named = b.identifier match {
            case g: GenericIdentifier => g.copy(fieldAlias = display.orElse(g.fieldAlias))
            case other                => other
          }
          val updated = b.copy(identifier = named)
          ListMap((Seq(name -> updated) ++ display.map(_ -> updated)): _*)
      }
    }: _*)

    var unnests: scala.collection.mutable.Map[String, Unnest] = {
      val map = from.unnests.map(u => u.alias.map(_.alias).getOrElse(u.name) -> u).toMap
      scala.collection.mutable.Map(map.toSeq: _*)
    }

    lazy val nestedFields: Map[String, Seq[Field]] =
      select.fields
        .filterNot(_.isAggregation)
        .filter(_.nested)
        .groupBy(_.identifier.innerHitsName.getOrElse(""))
    lazy val nested: Seq[NestedElement] =
      from.unnests.map(toNestedElement).groupBy(_.path).map(_._2.head).toList
    private[this] lazy val nestedFieldsWithoutCriteria: Map[String, Seq[Field]] = {
      val innerHitsWithCriteria = (where.map(_.nestedElements).getOrElse(Seq.empty) ++
        having.map(_.nestedElements).getOrElse(Seq.empty) ++
        groupBy.map(_.nestedElements).getOrElse(Seq.empty))
        .groupBy(_.path)
        .map(_._2.head)
        .toList
        .map(_.innerHitsName)
      val ret = nestedFields.filterNot { case (innerHitsName, _) =>
        innerHitsWithCriteria.contains(innerHitsName)
      }
      ret
    }
    // nested fields that are not part of where, having or group by clauses
    lazy val nestedElementsWithoutCriteria: Seq[NestedElement] =
      nested.filter(n => nestedFieldsWithoutCriteria.keys.toSeq.contains(n.innerHitsName))

    /** Mapping from inner hit name to (subFieldName, outputFieldName) pairs. Used to flatten inner
      * hits into individual rows (UNNEST row explosion).
      */
    lazy val nestedHitsMappings: Map[String, Seq[(String, String)]] = {
      val computedAliasMap = select.fieldsWithComputedAliases
        .flatMap(f => f.fieldAlias.map(a => f.identifier.identifierName -> a.alias))
        .toMap
      nestedFields.map { case (innerHitsName, fields) =>
        innerHitsName -> fields.map { f =>
          val subField = f.sourceField.stripPrefix(innerHitsName + ".")
          val outputName = f.fieldAlias
            .map(_.alias)
            .orElse(computedAliasMap.get(f.identifier.identifierName))
            .getOrElse(f.sourceField)
          (subField, outputName)
        }
      }
    }

    def toNestedElement(u: Unnest): NestedElement = {
      val updated = unnests.getOrElse(u.alias.map(_.alias).getOrElse(u.name), u)
      val parent = updated.parent.map(toNestedElement)
      NestedElement(
        path = updated.path,
        innerHitsName = updated.innerHitsName,
        size = limit.map(_.limit),
        children = Nil,
        sources = nestedFields
          .get(updated.innerHitsName)
          .map(_.map(_.identifier.name.split('.').tail.mkString(".")))
          .getOrElse(Nil),
        parent = parent
      )
    }

    lazy val sorts: ListMap[String, SortOrder] =
      ListMap(orderBy.map { _.sorts.map(s => s.name -> s.direction) }.getOrElse(Seq.empty): _*)

    def update(schema: Option[Schema] = None): SingleSearch = {
      schema match {
        case Some(s) => return this.copy(schema = Some(s)).update()
        case None    => // continue
      }
      (for {
        from <- Option(this.copy(from = from.update(this)))
        select <- Option(
          from.copy(
            select = select.update(from),
            groupBy = groupBy.map(_.update(from)),
            having = having.map(_.update(from))
          )
        )
        where   <- Option(select.copy(where = where.map(_.update(select))))
        updated <- Option(where.copy(orderBy = orderBy.map(_.update(where))))
      } yield updated).getOrElse(
        throw new IllegalStateException("Failed to update SQLSearchRequest")
      )
    }

    /** Resolves this statement against ONE SCHEMA PER TABLE, so every JOIN leg's columns can be
      * typed — not just the main table's.
      *
      * 🔴 `schemas` is keyed the way [[From.tableAliases]] keys it: the INDEX NAME (through
      * `aliasKey`, story 21.2 AD-6), never the SQL alias. `GenericIdentifier.update` looks it up
      * with `Identifier.table`, which IS that key. A map keyed by alias resolves NOTHING, and does
      * it silently — a miss leaves `baseType = Any`, which reads as "no conversion needed" rather
      * than as an error.
      *
      * No default: `updateAll()` with no schemas would be an expensive way to call [[update]], and
      * a silent no-op is the failure mode this whole area keeps producing. Callers with a single
      * schema want `update(Some(schema))`.
      */
    def updateAll(schemas: Map[String, Schema]): SingleSearch = {
      this.copy(schemas = schemas).update()
    }

    lazy val scriptFields: Seq[Field] = {
      // A GROUP BY emits `"size": 0`, so NO hit is ever returned and a `script_fields` block can
      // never be fetched -- Elasticsearch would still parse and compile every Painless script in it
      // on each query. It used to be emitted whenever the statement carried no metric aggregate,
      // which is exactly the aggregate-free GROUP BY shape issue #253 makes common. The VALUES that
      // block used to be asked for now come from `rowInvariantProjection` on the result side.
      if (aggregates.nonEmpty || groupBy.isDefined)
        Seq.empty
      else
        select.fieldsWithComputedAliases.filter(_.isScriptField)
    }

    lazy val fields: Seq[String] = {
      if (groupBy.isEmpty && !windowFunctions.exists(_.isWindowing))
        select.fields
          .filterNot(_.isScriptField)
          .filterNot(_.nested)
          .filterNot(_.isAggregation)
          .map(_.sourceField)
          .filterNot(f => excludes.contains(f))
          .distinct
      else
        Seq.empty
    }

    /** The row-invariant SELECT items and their values, keyed by OUTPUT column name (FOLD-IN 1).
      *
      * An aggregation response has no hits, so `script_fields` -- which is how a constant is
      * emitted -- is never fetched (`"size": 0`) and the column came back NULL on every row,
      * MEASURED on real Elasticsearch. The value is therefore taken from the AST and merged into
      * each aggregation row on the RESULT side: the constant is presentation, not something
      * Elasticsearch should be asked to compute. (The rejected alternative was rewriting it into
      * the GROUP BY as an extra scripted `terms` level -- it works, but it costs a Painless bucket
      * level per constant on the aggregation hot path.)
      *
      * The key is [[Field.outputName]] over `fieldsWithComputedAliases` -- the SAME expression
      * `SearchApi.extractOutputFieldNames` uses, so the projected key and the requested column are
      * the same string by construction. Row-VARIANT and unbound shapes are excluded by
      * [[SingleSearch.isRowInvariantLiteral]] -- a projected `?` would be a NULL column by another
      * route, which is the very thing this fixes.
      */
    lazy val rowInvariantProjection: ListMap[String, Any] = {
      // 🔴 A constant is NOT declared for a name some other SELECT item owns, and this guard is
      // load-bearing rather than belt-and-braces. Seeding makes Elasticsearch win only when it
      // actually EMITS a value: `extractMetrics` writes no entry for a null-valued metric, so
      // nothing lands on the right of `++` and the constant would survive. MEASURED against a
      // response whose `m` is `{"value": null}`, `SELECT category, 2 AS m, MAX(amount) AS m ...
      // GROUP BY category` returned `m = 2` where SQL says NULL. Precedence at the merge is
      // therefore necessary but not sufficient; the collision is excluded HERE, where the statement
      // is known, so the merge never has to arbitrate it.
      val claimedElsewhere =
        select.fieldsWithComputedAliases
          .filterNot(f => SingleSearch.isRowInvariantLiteral(f.identifier))
          .map(_.outputName)
          .toSet
      ListMap(
        select.fieldsWithComputedAliases
          .filter(f => SingleSearch.isRowInvariantLiteral(f.identifier))
          .filterNot(f => claimedElsewhere.contains(f.outputName))
          .map { f =>
            val value = f.identifier.functions.headOption match {
              case Some(v: Value[_]) => v.value
              case _                 => null
            }
            f.outputName -> value
          }: _*
      )
    }

    lazy val windowFields: Seq[Field] =
      select.fieldsWithComputedAliases.filter(_.identifier.hasWindow)

    lazy val windowFunctions: Seq[WindowFunction] = windowFields.flatMap(_.identifier.windows)

    /** Window-enriched ROW query: window functions present, no GROUP BY, and the SELECT projects at
      * least one non-aggregation column (or computes script fields). The canonical dispatch
      * condition shared by the search and scroll paths.
      */
    lazy val windowRowQuery: Boolean =
      windowFunctions.exists(_.isWindowing) && groupBy.isEmpty &&
      (!select.fields.forall(_.isAggregation) || scriptFields.nonEmpty)

    /** Response shape: true when the query returns DOCUMENT ROWS (plain, script-field or
      * window-enriched projections), false when it returns aggregation results (GROUP BY /
      * metric-only SELECT). A row query with no LIMIT means EVERY matching row (issue #209) —
      * consumers use this to route such queries through a paging-complete path.
      *
      * An explicit GROUP BY is aggregation-shaped WHATEVER the SELECT list contains (issue #253).
      * With no aggregate anywhere — `SELECT category FROM t GROUP BY category`, the SQL-standard
      * spelling of DISTINCT, and 22 of the 99 captured BI statements — `sqlAggregations` is empty,
      * which used to make the statement report itself as a row query: it was then routed to the
      * scroll path (`SearchApi.search`/`searchAsync`) or to the licensed row cap
      * (`CoreDqlExtension` rule 2) and came back as per-document rows instead of one row per group.
      * The scaladoc above already promised `false` here; the expression, not the contract, was the
      * bug.
      *
      * The guard sits on the aggregation arm ONLY because `windowRowQuery` already carries
      * `groupBy.isEmpty`, so hoisting it out would be equivalent today and would make correctness
      * depend on that internal. Do not "simplify" it.
      *
      * 🔴 Story 22.1 — for a statement whose FROM is [[relationalClosureRequired]] (a cross-index
      * JOIN or a derived table) this value describes the OUTER shape only: what the relational
      * engine's result will look like once story 22.4 executes it. It is NOT a routing licence. No
      * core router may act on it for such a statement, because every one of them runs BEHIND a
      * guard that fires first — `SearchApi.resolveWithSchema` precedes the `this match` on both the
      * sync and async search paths, and `CoreDqlExtension`'s closure arm precedes
      * `checkQuotasAndExecute`. `RelationalClosureGuardSpec` proves it rather than asserting it.
      * The EXPRESSION is deliberately untouched.
      */
    lazy val returnsRows: Boolean = windowRowQuery || (sqlAggregations.isEmpty && groupBy.isEmpty)

    /** `HAVING` with NO `GROUP BY`: standard SQL says the whole table is ONE implicit group, so the
      * predicate is evaluated once, over that group, and the statement returns either the single
      * aggregate row or NO row at all.
      *
      * 🔴 Before this discriminator existed the predicate EVAPORATED. A whole-table aggregation has
      * no buckets, so the `bucket_selector` the GROUP BY path attaches to each bucket had nothing
      * to attach to, and `buildBuckets` returned `Nil` with the criteria silently unused: MEASURED
      * on real Elasticsearch 8.18 over a 703-document index, `... HAVING COUNT(*) > 10000` returned
      * 703 and `... HAVING SUM(amount) > 99999` returned the unfiltered sum -- HTTP 200, wrong
      * answer, the #205/#209/#224/#253 family. Four of the 99 captured BI statements are this
      * shape: it is Tableau's data-source row-existence probe.
      *
      * When this is true the bridge wraps the root metric aggregations in a single-bucket keyed
      * `filters` aggregation named [[SingleSearch.WholeTableHavingAgg]] and hangs the SAME
      * `having_filter` `bucket_selector` off it. That is deliberately the one existing mechanism
      * rather than a second, engine-side evaluation of `HAVING`: the predicate then means exactly
      * what it means under a `GROUP BY` -- same script, same `buckets_path`, same null-guard
      * contract -- by construction rather than by agreement between two implementations.
      *
      * Shapes this does NOT cover are REFUSED by `validate()`, never silently dropped: a `HAVING`
      * naming a non-aggregated column, a SELECT list carrying one, and (the catch-all) any
      * remaining row-shaped statement.
      */
    lazy val wholeTableHaving: Boolean =
      groupBy.isEmpty && having.flatMap(_.criteria).isDefined && !returnsRows

    private lazy val selectAggs: Seq[Field] =
      select.fieldsWithComputedAliases
        .filter(f => f.isAggregation || f.isBucketScript)
        .filterNot(_.identifier.hasWindow) ++ windowFields

    // Aggregations referenced only in HAVING, WHERE, or ORDER BY clauses (not in SELECT)
    private lazy val auxiliaryAggs: Seq[Field] = {
      // Dedup against SELECT by EXPRESSION only. Matching by alias too let a user alias hijack a
      // derived metric name -- `SELECT MIN(x) AS max_x ... HAVING MAX(x) > 3` read MIN under
      // `params.max_x`; such a collision is now rejected by `validate()` instead.
      val selectAggNames = selectAggs.map(_.identifier.identifierName).toSet
      val havingAggs = having
        .flatMap(_.criteria)
        .map(_.extractAggregationFields)
        .getOrElse(Seq.empty)
      val whereAggs = where
        .flatMap(_.criteria)
        .map(_.extractAggregationFields)
        .getOrElse(Seq.empty)
      val orderByAggs = orderBy
        .map(_.sorts.flatMap(_.extractAggregationFields))
        .getOrElse(Seq.empty)
      // Aggregates nested inside a SELECT bucket script (`MAX(x) - MIN(x) AS d`): the script reads
      // each as `params.<metricName>`, so each must exist as its own aggregation. None was ever
      // created -- the bucket_script shipped with a self-referencing buckets_path and its operands
      // collapsed onto one name (issue #54). The wrapper identifier itself (its head is the
      // arithmetic expression, not an aggregate) is excluded by `isAggregation`; an operand that is
      // also a SELECT item is excluded below like any other auxiliary candidate.
      val bucketScriptAggs = selectAggs
        .filter(_.isBucketScript)
        .flatMap(f => FunctionUtils.funIdentifiers(f.identifier))
        .filter(_.isAggregation)
        .flatMap(id => id.metricName.map(name => Field(id, Some(Alias(name)))))
      // Dedup by name, keeping the first occurrence IN ORDER -- a `groupBy` here hashed the order,
      // so the emitted `aggs` shuffled between runs and could not be pinned.
      (havingAggs ++ whereAggs ++ orderByAggs ++ bucketScriptAggs)
        .filterNot(f => selectAggNames.contains(f.identifier.identifierName))
        .foldLeft(Seq.empty[Field]) { (acc, f) =>
          if (acc.exists(_.fieldAlias.map(_.alias) == f.fieldAlias.map(_.alias))) acc else acc :+ f
        }
    }

    lazy val aggregates: Seq[Field] = selectAggs ++ auxiliaryAggs

    lazy val sqlAggregations: ListMap[String, SQLAggregation] = {
      val auxiliaryAggNames = auxiliaryAggs
        .flatMap(f => SQLAggregation.fromField(f, this))
        .map(_.aggName)
        .toSet
      ListMap(
        aggregates.flatMap(f => SQLAggregation.fromField(f, this)).map { a =>
          a.aggName -> (if (auxiliaryAggNames.contains(a.aggName)) a.copy(auxiliary = true) else a)
        }: _*
      )
    }

    lazy val excludes: Seq[String] = select.except.map(_.fields.map(_.sourceField)).getOrElse(Nil)

    lazy val sources: Seq[String] = from.tables.map(_.name)

    lazy val bucketTree: BucketTree = BucketTree.fromBuckets(
      Seq(groupBy.map(_.buckets).getOrElse(Seq.empty)) ++ windowFunctions.map(
        _.buckets
      )
    )

    lazy val buckets: Seq[Bucket] = bucketTree.allBuckets.flatten

    /** Story 22.1 AD-4 — an OUTER reference into a derived table must name a column the derived
      * table PROJECTS (its `outputNames`: the SELECT alias when there is one, else the bare column,
      * so `SELECT amount AS total` exposes `total`, NOT `amount`).
      *
      * Checked HERE, not in `Identifier.update`: `update` runs inside `Parser.single`'s combinator
      * action and cannot report. That is the house pattern — record in `update`, format in
      * `validate` — and this arm needs no recording, because everything it reads survives on the
      * AST.
      *
      * Scope rules, deliberately narrow (epic 22: never guess a field):
      *   - a QUALIFIED reference (`d.x`) is checked against the derived table `d` names —
      *     `Identifier.table` is the alias-map key and for a derived table that key IS the alias;
      *   - an UN-QUALIFIED reference is checked ONLY when the FROM has exactly ONE source and it is
      *     a derived table (Tableau's `SELECT COL FROM (SELECT 1 AS COL) AS SUBQUERY`). With
      *     several sources a bare name is ambiguous today for plain tables too, and resolving it is
      *     story 22.3's scope model, not this story's;
      *   - an outer SELECT alias (`SELECT COL AS c … ORDER BY c`), an ordinal or literal (empty
      *     `name`), `*` and `COUNT(*)` are never derived-table references;
      *   - a derived table whose projection is OPAQUE (`outputNames == None`, i.e. a bare `SELECT
      *     *`) accepts EVERY reference: rejecting one would mean inventing the schema, and DuckDB's
      *     binder rejects a wrong one loudly once story 22.4 executes it.
      *
      * A dotted remainder (`d.items.name`) is struct/nested access INTO a projected column, so the
      * HEAD segment is what is compared, never the whole path.
      */
    private lazy val derivedScopeCheck: Either[String, Unit] = {
      val scopes = from.derivedTables
      if (scopes.isEmpty) Right(())
      else {
        // Story 22.3 (AD-1) — resolution goes through the ONE resolver, so this check and the
        // planner's cannot drift. It also retires the stale-`Identifier.table` fallback story 22.5
        // would otherwise have had to add: `SubqueryScope.resolve` reads `tableAlias` FIRST and
        // falls back to `table`, absorbing the re-`update()` staleness where every other consumer
        // already keys on the alias.
        val here = Seq(SubqueryScope.scopeOf(this))
        // `fieldAliases` is built over `fieldsWithComputedAliases`, so it also holds the synthetic
        // `__cN` names — harmless here, since no real column is spelled that way.
        val outerAliases: Set[String] = select.fieldAliases.values.toSet
        referencedIdentifiers.iterator
          .filter(id => id.name.nonEmpty && id.name != "*")
          // An OUTER SELECT alias (`SELECT COL AS c … ORDER BY c`) is not a derived-table
          // reference; it names a projection of THIS statement.
          .filterNot(id =>
            id.tableAlias.isEmpty && id.table.isEmpty && !id.name.contains(".") &&
            outerAliases.contains(id.name)
          )
          .flatMap { id =>
            // 🔴 The column comes FROM the resolution, never re-derived from `id.name`: `resolve`
            // has already stripped whatever qualifier it matched, and splitting the name again
            // here would compare the QUALIFIER (`D` of `D.total`, which `resolve` matches
            // case-insensitively) against the projection and refuse a statement it just resolved.
            // One derivation, two readers — the story 21.3 lesson.
            val scope: Option[(DerivedTable, String)] = SubqueryScope.resolve(id, here) match {
              case SubqueryScope.Resolved(0, src: SubqueryScope.DerivedSource, column) =>
                scopes.get(src.alias).map(_ -> column.split("\\.", 2)(0))
              case _ => None // Ambiguous / Unresolved / an enclosing scope: never guessed at
            }
            scope.flatMap { case (d, head) =>
              // Case-INSENSITIVE, the same match `SubqueryScope.resolve` makes when it decides
              // WHICH source projects the name: an SQL identifier is case-insensitive, and the two
              // halves disagreeing is exactly what made `SELECT Total … AS total` fail.
              d.outputNames
                .filterNot(names => SubqueryScope.projects(Some(names), head))
                .map(names => (id, d, names))
            }
          }
          .toSeq
          .headOption match {
          case Some((id, d, names)) =>
            Left(
              s"Column '${id.name}' is not projected by derived table '${d.name}' " +
              s"(it projects: ${names.mkString(", ")})"
            )
          case None => Right(())
        }
      }
    }

    /** Story 22.1 (amendment from story 22.3's review) — a derived body that reads an ENCLOSING
      * correlation name is SQL:1999 `LATERAL`, which this dialect does not support. Without this
      * arm the shape parses `Right` and story 22.4 would execute the body as written: the outer
      * qualifier reaches Elasticsearch as an object path and the statement answers ZERO rows with
      * HTTP 200. The detector is `SubqueryScope`, shared with stories 22.2 / 22.3.
      */
    private lazy val lateralCheck: Either[String, Unit] =
      // Story 22.3 — `hasWhereSubqueries` joins the guard because the walk now descends into
      // WHERE-subquery bodies: a statement whose OWN FROM carries no derived table can still hold
      // one inside a subquery body, and that shape is LATERAL just the same.
      if (!from.hasDerivedTables && !hasWhereSubqueries) Right(())
      else
        SubqueryScope.lateralOffenders(this).headOption match {
          // The offender is carried WITH the derived table whose body names it, so the message can
          // never name the wrong alias (or, worse, an empty one) for a reference two levels in.
          case Some((alias, id)) => Left(SubqueryScope.lateralMessage(id, alias))
          case None              => Right(())
        }

    /** Story 22.5 — a bare single-part FROM/JOIN reference that names one of THIS statement's CTEs
      * but is not a marked derived table can only come from a PROGRAMMATIC construction that
      * bypassed [[CteSubstitution]] (the grammar always substitutes). It would execute against an
      * INDEX of the CTE's name — silently. Reject it by name.
      *
      * It walks the SAME surface the substitution walks — FROM/JOIN, literal derived bodies, and
      * every statement embedded in a WHERE / HAVING / ON criteria — because it IS
      * `CteSubstitution.referencedNames`, so the check and the rewrite cannot disagree about where
      * a reference may hide. (`referencedNames` already skips marked derived tables and qualified
      * references, so a substituted statement reports NO name: that is the invariant checked here.)
      */
    private lazy val unsubstitutedCteReference: Either[String, Unit] = {
      val names = ctes.map(_.name.value).toSet
      if (names.isEmpty) Right(())
      else
        CteSubstitution.referencedNames(this).find(names.contains) match {
          case Some(n) =>
            Left(
              s"CTE '$n' is referenced but was not substituted: build the statement through " +
              "Parser or CteSubstitution"
            )
          case None => Right(())
        }
    }

    /** The rules that can only be checked once a SCHEMA has been attached (issue #384).
      *
      * 🔴 Deliberately NOT part of [[validate]]. `Parser.apply` validates the statement it just
      * parsed, where every column's type is `Any`; the types these rules read exist only after
      * `update(Some(schema))`. Core calls this at the ONE seam that produces a resolved statement
      * (`SearchApi.resolveWithSchema`), which is the same seam #306 uses to attach the schema in
      * the first place.
      *
      * ⚠️ It is NOT a second `validate()`. Running the whole of `validate()` against resolved types
      * would surface every pre-existing disagreement between a column's DECLARED type and its
      * chain's — including `CAST(ts AS TIME) = CAST('07:00:00' AS TIME)`, which works today and
      * which `Expression.validate`'s `identifier.out` (the COLUMN's type, not the chain's) already
      * rejects when it is asked after resolution. This method carries exactly the rules that were
      * measured, and each one names the shape it refuses.
      */
    def validateResolved(): Either[String, Unit] =
      ((where.flatMap(_.criteria).toSeq ++ having.flatMap(_.criteria).toSeq ++
      select.fields.flatMap(f => Case.conditionsOf(f.identifier)) ++
      orderBy.toSeq.flatMap(_.sorts.flatMap(s => Case.conditionsOf(s.field))) ++
      groupBy.toSeq.flatMap(_.buckets.flatMap(b => Case.conditionsOf(b.identifier))))
        .flatMap(_.temporalComparisonErrors) ++
        scriptedExpressions.flatMap(NullIf.mismatchesOf)).headOption
        .map(Left(_))
        .getOrElse(Right(()))

    /** Every expression of this statement that can be emitted as Painless, as the chain it is.
      *
      * The SELECT list, both operands of every WHERE / HAVING criterion (`referencedIdentifiers`,
      * which is what carries `NULLIF(a, b) > 1`'s left side), every ORDER BY sort and every GROUP
      * BY bucket -- the SAME surface the temporal rule above walks, so a rule added to one cannot
      * quietly cover less ground than the other. A WHERE subquery's own body is NOT walked: it
      * crosses the resolution seam as a statement of its own (story 22.2).
      */
    private def scriptedExpressions: Seq[FunctionChain] =
      select.fields.map(_.identifier) ++
      (where.flatMap(_.criteria).toSeq ++ having.flatMap(_.criteria).toSeq)
        .flatMap(_.referencedIdentifiers) ++
      orderBy.toSeq.flatMap(_.sorts.map(_.field)) ++
      groupBy.toSeq.flatMap(_.buckets.map(_.identifier))

    override def validate(): Either[String, Unit] = {
      for {
        // Story 22.5 — an UNREFERENCED CTE's body reaches `DerivedTable.validate()` through no
        // path at all (nothing in the FROM tree points at it), so its own GROUP BY / HAVING rules
        // would be silently skipped. A REFERENCED body is validated twice (here and through
        // `Table.validate` -> `d.validate()`), which is idempotent and cheap.
        _ <- ctes.map(_.validate()).collectFirst { case l @ Left(_) => l }.getOrElse(Right(()))
        _ <- unsubstitutedCteReference
        _ <- from.validate()
        // AFTER `from.validate()` so a derived table's OWN body is validated first, and BEFORE
        // every clause rule so the scope message wins over a downstream symptom.
        _ <- derivedScopeCheck
        _ <- lateralCheck
        _ <- select.validate()
        _ <- where.map(_.validate()).getOrElse(Right(()))
        _ <- groupBy.map(_.validate()).getOrElse(Right(()))
        _ <- having.map(_.validate()).getOrElse(Right(()))
        _ <- orderBy.map(_.validate()).getOrElse(Right(()))
        _ <- limit.map(_.validate()).getOrElse(Right(()))
        // (An aggregate in WHERE is rejected by Where.validate() itself -- run above through
        // `where.map(_.validate())` -- so DELETE / UPDATE are covered too; see there.)
        _ <- {
          // Arithmetic over aggregates written INLINE in HAVING (`HAVING MAX(x) - MIN(x) > 3`) has no
          // aggregation to read from and was silently dropped. Alias it in SELECT and reference the
          // alias (`... AS d ... HAVING d > 3`), which IS supported (Having.resolveAggregateAliases).
          having
            .flatMap(_.criteria)
            .map(_.referencedIdentifiers)
            .getOrElse(Nil)
            .find(id => !id.isAggregation && id.hasAggregation && id.fieldAlias.isEmpty) match {
            case Some(id) =>
              Left(
                s"HAVING cannot combine aggregates arithmetically inline (${id.sql}); alias the expression in SELECT and reference the alias"
              )
            case None => Right(())
          }
        }
        _ <- {
          // 🔴 Issue #389 -- a HAVING over a FUNCTION of an aggregate must FILTER, or FAIL. It used
          // to VANISH: a function never looks inside its own arguments, so `ABS(COUNT(*)) > 1` was
          // invisible to the bucket-metric detection, the selector degenerated to `1 == 1` and
          // every group came back with HTTP 200 (the #205 / #209 / #253 silent-wrong-answer
          // family). Everything the engine can express is now emitted; everything it cannot is
          // refused HERE, inside `Parser.apply`, so every venue sees it -- the REPL, JDBC, Flight,
          // the materialized-view extension and the bridge's own emission path alike.
          //
          // ⚠️ A PARTIAL emission counts as a wrong answer: one un-expressible conjunct refuses the
          // whole statement rather than silently filtering on the other half.
          having.map(_.unrepresentable).getOrElse(Nil).headOption match {
            case Some(u) => Left(u.message)
            case None    => Right(())
          }
        }
        _ <- {
          // The residual of the rule above, and the reason it needs the STATEMENT rather than the
          // clause: `HAVING NULLIF(c, 0) > 1` names a SELECT aggregate by its ALIAS from inside a
          // function ARGUMENT. `Having.resolveAggregateAliases` substitutes an OPERAND only, so `c`
          // stays a bare column, the predicate references no aggregate at all, and the rule above
          // cannot see it -- it renders `arg0 == 0 ? null : arg0 > 1`, reading a context parameter
          // no bucket pipeline binds. MEASURED: dropped silently on `main`.
          //
          // ⚠️ The alias set is read from `Having.aggregateAliases`, the SAME map the substitution
          // uses, so the two cannot disagree about which bare names are aggregate references. A
          // COLUMN that happens to share a SELECT aggregate's alias is therefore refused here for
          // exactly the reason it is SUBSTITUTED there -- one ambiguity, one reading.
          having.flatMap(_.criteria) match {
            case None => Right(())
            case Some(criteria) =>
              val aliases = Having.aggregateAliases(this).keySet
              criteria.referencedIdentifiers
                .filter(id => id.functions.nonEmpty && id.bucketMetrics.isEmpty)
                .flatMap(id =>
                  FunctionUtils.funIdentifiers(id).map(_.name).filter(aliases.contains).map(id -> _)
                )
                .headOption match {
                case Some((id, alias)) =>
                  Left(
                    s"HAVING cannot apply a function to the aggregate alias '$alias' (${id.sql}); " +
                    "compare the aggregate itself"
                  )
                case None => Right(())
              }
          }
        }
        _ <- {
          // A HAVING / ORDER BY aggregate whose derived name equals a SELECT alias of a DIFFERENT
          // aggregate (`SELECT MIN(x) AS max_x ... HAVING MAX(x) > 3`) would read the wrong metric
          // under that name -- reject instead of colliding.
          val selectAliases = selectAggs.flatMap(_.fieldAlias.map(_.alias)).toSet
          auxiliaryAggs.flatMap(_.fieldAlias.map(_.alias)).find(selectAliases.contains) match {
            case Some(alias) =>
              Left(
                s"Alias '$alias' names a SELECT aggregate and a different aggregate referenced in HAVING or ORDER BY; rename one of them"
              )
            case None => Right(())
          }
        }
        /*_ <- {
          // validate that having clauses are only applied when group by is present
          if (having.isDefined && groupBy.isEmpty) {
            Left("HAVING clauses can only be applied when GROUP BY is present")
          } else {
            Right(())
          }
        }*/
        _ <- {
          // validate that non-aggregated fields are not present when group by is present
          if (groupBy.isDefined) {
            val nonAggregatedFields =
              select.fields.filterNot(f => f.hasAggregation)
            val invalidFields = nonAggregatedFields
              .filterNot(f => buckets.exists(_.name == f.outputName))
              // FOLD-IN 1: a ROW-INVARIANT literal is legal beside a GROUP BY in standard SQL --
              // it cannot vary within a group, so it needs no bucket. It is not merely accepted:
              // `rowInvariantProjection` carries its VALUE into every aggregation row, because a
              // column that parses and then comes back NULL is the silent wrong answer this story
              // exists to close.
              .filterNot(f => SingleSearch.isRowInvariantLiteral(f.identifier))
            if (invalidFields.nonEmpty) {
              Left(
                s"Non-aggregated fields ${invalidFields.map(_.sql).mkString(", ")} cannot be selected when GROUP BY is present"
              )
            } else {
              Right(())
            }
          } else {
            Right(())
          }
        }
        _ <- {
          // HAVING with NO GROUP BY -- standard SQL's implicit whole-table group. It is HONOURED
          // (see `wholeTableHaving`), and every shape that cannot be honoured is REFUSED here.
          // Nothing may fall through silently: a dropped predicate returns the UNFILTERED
          // aggregate with HTTP 200, which is the failure mode this whole family exists to close.
          having.flatMap(_.criteria) match {
            // A HAVING that reaches into a NESTED relation is out of scope, and deliberately so:
            // it has its OWN long-standing mechanism (`requestToNestedFilterAggregation` turns it
            // into a `filter` aggregation scoped to the inner-hits path), it is exercised by the
            // bridge fixtures, and it is not discarded. The rules below are about the FLAT
            // whole-table group.
            case Some(criteria) if groupBy.isEmpty && criteria.nestedElements.isEmpty =>
              // (a) With no GROUP BY there are no grouping keys, so every column the predicate
              //     names must be aggregated -- `HAVING category = 'x'` is not a filter over a
              //     group, it is a WHERE written in the wrong clause.
              val nonAggregated = criteria.referencedIdentifiers
                .filterNot(_.hasAggregation)
                .filter(_.name.nonEmpty)
                .map(_.identifierName)
                .distinct
              // (b) ... and so must every SELECT item, for the same reason the GROUP BY branch
              //     above rejects one: there is no key it could be grouped by. A row-invariant
              //     literal is excused there and is excused here, identically.
              val invalidFields = select.fields
                .filterNot(_.hasAggregation)
                .filterNot(f => SingleSearch.isRowInvariantLiteral(f.identifier))
              if (nonAggregated.nonEmpty)
                Left(
                  s"Non-aggregated fields ${nonAggregated.mkString(", ")} cannot be used in " +
                  "HAVING when GROUP BY is absent; use WHERE, or add a GROUP BY"
                )
              else if (invalidFields.nonEmpty)
                Left(
                  s"Non-aggregated fields ${invalidFields.map(_.sql).mkString(", ")} cannot be " +
                  "selected when HAVING is present without a GROUP BY"
                )
              else if (!wholeTableHaving)
                // (c) The catch-all. `wholeTableHaving` is what makes the bridge emit the
                //     predicate; anything reaching here is row-shaped (a windowed projection, say)
                //     and would have had its HAVING discarded. Refusing is the contract.
                Left(
                  "HAVING without GROUP BY is only supported for an aggregate query over the " +
                  "whole table; this statement returns document rows"
                )
              else Right(())
            case _ => Right(())
          }
        }
        _ <- {
          // NULLS FIRST / NULLS LAST cannot be honored when the query is an
          // aggregation / GROUP BY query: Elasticsearch terms aggregations have
          // no `missing` parameter, so the bridge routes these sorts through the
          // aggregation path (guarded by `aggregates.isEmpty && buckets.isEmpty`)
          // and would silently drop the null ordering. Reject explicitly.
          if (aggregates.nonEmpty || buckets.nonEmpty) {
            val nullOrdered =
              orderBy.map(_.sorts.filter(_.nullOrdering.isDefined)).getOrElse(Seq.empty)
            if (nullOrdered.nonEmpty) {
              Left(
                s"NULLS FIRST / NULLS LAST is not supported on ORDER BY when GROUP BY or aggregations are present (offending sort: ${nullOrdered
                  .map(_.sql)
                  .mkString(", ")})"
              )
            } else {
              Right(())
            }
          } else {
            Right(())
          }
        }
        _ <- {
          // OFFSET under a GROUP BY was silently DROPPED: the aggregation path emits
          // `size 0 fetchSource false` whenever `allAggregations.nonEmpty && fields.isEmpty`, which
          // is every GROUP BY, so the `limit ... from ...` branch that would carry the offset is
          // never taken and the offset never reached Elasticsearch. A real group offset needs the
          // `composite` aggregation and `after_key` — explicitly out of scope. Same reasoning as
          // the NULLS FIRST / NULLS LAST guard above, and applied uniformly to the
          // aggregate-bearing and aggregate-free forms alike.
          if (groupBy.isDefined && limit.exists(_.offset.isDefined)) {
            Left(
              "OFFSET is not supported when GROUP BY is present: group results are not paginated " +
              "(the offset used to be ignored silently). Remove the OFFSET, or page by key ranges."
            )
          } else {
            Right(())
          }
        }
      } yield ()
    }

  }

  object SingleSearch {

    /** Name of the synthetic single-bucket aggregation the bridge emits for a `HAVING` with no
      * `GROUP BY` (see [[SingleSearch.wholeTableHaving]]). It exists only so that the
      * `bucket_selector` has a multi-bucket parent to hang from -- Elasticsearch rejects one inside
      * a single-bucket `filter` aggregation with a `class_cast_exception` (MEASURED on 8.18.3),
      * while a keyed `filters` aggregation with a single `match_all` accepts it and drops the whole
      * bucket when the predicate is false.
      *
      * The name is RESERVED and shared: the bridge emits it, and `ElasticConversion` treats a
      * bucket aggregation carrying it as TRANSPARENT -- it contributes no key column to the result
      * row (`rowNormalizer` appends unrequested keys rather than dropping them, so without this the
      * synthetic bucket key would surface as an extra column). One constant, two readers, so the
      * two cannot drift.
      */
    val WholeTableHavingAgg: String = "__whole_table_having__"

    /** The key of the single bucket inside [[WholeTableHavingAgg]]. */
    val WholeTableHavingBucket: String = "_all"

    /** A bare ROW-INVARIANT literal: an empty name carrying exactly one scalar constant `Value` --
      * the shape the parser builds for `2 AS COL2` (`GenericIdentifier("", List(LongValue(2)))`).
      * Such an item is the same for every document in a group.
      *
      * THREE production consumers. The first two are the halves of one feature (issue #253, FOLD-IN
      * 1 -- "a constant is legal beside a GROUP BY"):
      *   - `SingleSearch.validate()` excuses such a field from the non-aggregated-field check, so
      *     `SELECT category, 2 AS flag FROM t GROUP BY category` parses;
      *   - `SingleSearch.rowInvariantProjection` carries its VALUE, which `SearchApi` merges into
      *     each aggregation row -- without that the column parses and then comes back NULL, because
      *     an aggregation response has no hits and the `script_fields` entry a constant is emitted
      *     as is never fetched under `"size": 0`.
      *
      * The third is [[app.softnetwork.elastic.sql.function.aggregate.CountAgg.rowCountingOperand]]:
      * `COUNT(<non-null literal>)` is `COUNT(*)` in ANSI SQL, so the parser rewrites such an
      * operand to `*`. That consumer carves out exactly one member of this allow-list -- `Null`,
      * because `COUNT(NULL)` is 0, not a row count -- and it is the only carve-out anywhere.
      *
      * It does NOT decide whether a BUCKET must be scripted: that rule is `identifier.name.isEmpty`
      * (a bucket with no field name has nothing to name), which covers every nameless `Value` and
      * not merely the row-invariant ones.
      *
      * 🔴 It is an ALLOW-list on purpose, not a deny-list. A deny-list ("everything but
      * `RandomValue`/`ParamValue`/...") defaults to ACCEPT, so it silently widens the moment a
      * `Value` subclass is added -- and it would already admit `Values` (array literals, which
      * extend `Value[Seq[T]]`) unmeasured. Keeping `ParamValue` / `RandomValue` / `IdValue` /
      * `IngestTimestampValue` / `Values` OUT is load-bearing: they are row-variant or unbound, and
      * a projected `?` would be a NULL column by another route -- the very thing this fixes. `Null`
      * IS included: it is a constant, and excluding it would be an exception with no principle
      * behind it (a projected SQL `NULL` column is correct, though indistinguishable downstream
      * from one that failed to project, since `rowNormalizer` null-fills identically). `CharValue`
      * / `EValue` are not reachable from today's grammar (`value =
      * literal|pi|random|double|long|boolean|null|param|array`); they are classified with their
      * peers rather than left to the reject arm by accident.
      */
    def isRowInvariantLiteral(identifier: Identifier): Boolean =
      identifier.name.isEmpty && (identifier.functions match {
        case (v: Value[_]) :: Nil =>
          v match {
            case _: NumericValue[_] => true // LongValue, DoubleValue
            case _: StringValue     => true
            case _: CharValue       => true
            case _: BooleanValue    => true
            case PiValue | EValue   => true
            case Null               => true
            case _                  => false
          }
        case _ => false
      })
  }

  /** Two or more `SELECT` branches combined by set operators (story 22.6; before it, `UNION ALL`
    * only).
    *
    * The list is FLAT and in TEXT order: `operators(i)` sits between `requests(i)` and `requests(i
    * + 1)`. `Nil` — the default, and what every pre-22.6 construction produces — means "every
    * operator is [[app.softnetwork.elastic.sql.operator.UNION]] ", i.e. `UNION ALL`, so
    * `MultiSearch(branches)` keeps its historical meaning and every positional construction in
    * core/macros/tests keeps compiling.
    *
    * 🔴 `Nil` is the CANONICAL form, not merely a default: the parser stores `Nil` whenever every
    * operator is `UNION` ([[app.softnetwork.elastic.sql.parser.Parser.searchStatement]]). Without
    * that normalisation a programmatic `MultiSearch(reqs)` and its own re-parse would render to the
    * same text and compare UNEQUAL (`Nil` vs `Seq(UNION)`), so `Parser(stmt.sql) == Right(stmt)` —
    * the house fixed point — would fail for the one shape that pre-dates this story. Readers
    * consult [[resolvedOperators]], never the raw field.
    *
    * Precedence (`INTERSECT` before `UNION`/`EXCEPT`, otherwise left to right, SQL-92 §7.10) is a
    * DERIVED view ([[tree]]), never stored: the flat list re-renders to the flat text, so the
    * render fixed point holds by construction and no parenthesisation has to be remembered. A
    * user-written grouping that DIFFERS from precedence — `(a UNION b) INTERSECT c` — is rejected
    * in the grammar and is written as a derived table instead.
    *
    * Binary-incompatible for a downstream that CONSTRUCTS or DESTRUCTURES `MultiSearch` (arity
    * 2→3); release note.
    */
  case class MultiSearch(
    requests: Seq[SingleSearch],
    explodeNested: Boolean = true,
    operators: Seq[SetOperator] = Nil
  ) extends SearchStatement {

    /** [[operators]] with the legacy `Nil` expanded — the ONE list every reader consults. */
    lazy val resolvedOperators: Seq[SetOperator] =
      if (operators.isEmpty) Seq.fill(math.max(requests.size - 1, 0))(UNION) else operators

    /** The historical shape: `UNION ALL` between every pair. Only this shape may reach
      * Elasticsearch's `_msearch`; every other operator needs the relational engine.
      */
    lazy val isUnionAllOnly: Boolean = resolvedOperators.forall(_ == UNION)

    /** Epic 22 AD-4, lifted to the set operation: true when the OPERATOR LIST needs the engine
      * (anything but `UNION ALL`) or when any branch does.
      *
      * 🔴 Reads the branch's STATEMENT-level member [[SingleSearch.relationalClosureRequired]],
      * never `_.from.…`: story 22.3 widened that member with `hasCorrelatedSubqueries` and 22.5
      * with `ctes.nonEmpty`, and a set operation over a correlated or CTE-bearing branch must route
      * exactly as that branch would alone. One member, every widening inherited.
      */
    lazy val relationalClosureRequired: Boolean =
      !isUnionAllOnly || requests.exists(_.relationalClosureRequired)

    /** SQL-92 precedence as a binary tree over branch INDICES (0-based). Left-associative within a
      * precedence level; `INTERSECT [ALL]` groups first. Consumed by the DuckDB render in
      * softclient4es-arrow and by nothing in core.
      */
    lazy val tree: SetOpNode =
      MultiSearch.precedenceTree(requests.indices.toList, resolvedOperators.toList)

    /** The WITH prefix this statement renders, hoisted from branch 0 (story 22.5 keeps the list
      * there and `MultiSearch` gains no `ctes` field of its own).
      */
    private lazy val ctePrefix: String = requests.headOption.map(_.ctePrefix).getOrElse("")

    /** `UNION ALL`-only renders BYTE-FOR-BYTE as before story 22.6 (epic AC 4). Any other operator
      * list renders every branch parenthesised: a branch carrying its own `ORDER BY` / `LIMIT`
      * would otherwise re-parse under the trailing-clause rejection, and the parenthesised form is
      * accepted for every operator, so the fixed point holds for every shape with ONE rule.
      *
      * 🔴 Branch 0's `WITH` prefix is hoisted OUT of the parentheses and emitted once, ahead of the
      * whole operation: `(WITH x AS (…) SELECT …) UNION (…)` cannot parse, because a set-operation
      * branch is a `single` and `single` has no `WITH`. `validate()` refuses a WITH list on any
      * OTHER branch, which the grammar cannot produce and whose render could not re-parse.
      */
    override def sql: String =
      if (isUnionAllOnly) requests.map(_.sql).mkString(" UNION ALL ")
      else {
        val rendered = requests.zipWithIndex.map {
          case (r, 0) => s"(${r.sqlWithoutCtes})"
          case (r, _) => s"(${r.sql})"
        }
        ctePrefix + rendered
          .zipAll(resolvedOperators.map(op => s" ${op.sql} "), "", "")
          .map { case (branch, op) => branch + op }
          .mkString
      }

    override def withoutNestedExplosion: MultiSearch = {
      this.copy(explodeNested = false, requests = requests.map(_.withoutNestedExplosion))
    }

    def update(): MultiSearch = this.copy(requests = requests.map(_.update()))

    override def validate(): Either[String, Unit] = {
      for {
        _ <- requests.map(_.validate()).filter(_.isLeft) match {
          case Nil    => Right(())
          case errors => Left(errors.map { case Left(err) => err }.mkString("\n"))
        }
        _ <-
          if (operators.isEmpty || operators.size == requests.size - 1) Right(())
          else
            Left(
              s"A set operation over ${requests.size} branches needs ${requests.size - 1} " +
              s"operators, got ${operators.size}"
            )
        // The grammar attaches a WITH clause to the FIRST SELECT only, so this can be reached by a
        // programmatic construction alone -- and it is refused rather than rendered, because the
        // parenthesised render would hoist branch 0's prefix and silently DROP this one.
        _ <- requests.zipWithIndex.drop(1).collectFirst { case (r, i) if r.hasCtes => i } match {
          case Some(i) =>
            Left(
              s"A WITH clause must precede the FIRST branch of a set operation; branch ${i + 1} " +
              "carries one. Move the common table expressions in front of the first SELECT."
            )
          case None => Right(())
        }
        _ <- MultiSearch.branchArity(requests)
        _ <- MultiSearch.branchTypes(requests)
      } yield ()
    }

    lazy val sqlAggregations: ListMap[String, SQLAggregation] =
      ListMap(requests.flatMap(_.sqlAggregations).distinct: _*)

    lazy val fieldAliases: ListMap[String, String] =
      ListMap(requests.flatMap(_.fieldAliases).distinct: _*)
  }

  /** A node of [[MultiSearch.tree]]. Leaves are branch INDICES into `MultiSearch.requests`. */
  sealed trait SetOpNode

  case class SetOpLeaf(branch: Int) extends SetOpNode

  case class SetOpBranch(left: SetOpNode, operator: SetOperator, right: SetOpNode) extends SetOpNode

  object MultiSearch {

    /** Fold the flat list into the precedence tree: first every `INTERSECT [ALL]` pair, left to
      * right, then the remaining `UNION`/`EXCEPT` operators, left to right. Pure, total,
      * index-based.
      */
    private[query] def precedenceTree(branches: List[Int], ops: List[SetOperator]): SetOpNode = {
      val (nodes1, ops1) = ops
        .zip(branches.tail)
        .foldLeft((List[SetOpNode](SetOpLeaf(branches.head)), List[SetOperator]())) {
          case ((nodes, kept), (op, b)) if op.bindsTighter =>
            (nodes.init :+ SetOpBranch(nodes.last, op, SetOpLeaf(b)), kept)
          case ((nodes, kept), (op, b)) => (nodes :+ SetOpLeaf(b), kept :+ op)
        }
      ops1.zip(nodes1.tail).foldLeft(nodes1.head) { case (acc, (op, n)) => SetOpBranch(acc, op, n) }
    }

    /** The projection a branch DECLARES — `None` for a bare `SELECT *` (OPAQUE: its width is the
      * index mapping's, unknown here, and is never guessed).
      */
    private def declared(s: SingleSearch): Option[Seq[Field]] =
      if (s.select.fields.exists(f => f.identifier.name == "*" && f.identifier.functions.isEmpty))
        None
      else Some(s.select.fields)

    /** SQL-92 §7.10: every branch must project the same number of columns. Checked between the
      * DECLARING branches only.
      */
    private[query] def branchArity(requests: Seq[SingleSearch]): Either[String, Unit] = {
      val known = requests.zipWithIndex.flatMap { case (r, i) => declared(r).map(f => (i, f)) }
      known.headOption match {
        case None => Right(())
        case Some((i0, f0)) =>
          known.find(_._2.size != f0.size) match {
            case Some((i, f)) =>
              Left(
                s"Set operation branches must project the same number of columns: branch " +
                s"${i0 + 1} projects ${f0.size} (${f0.map(_.outputName).mkString(", ")}), branch " +
                s"${i + 1} projects ${f.size} (${f.map(_.outputName).mkString(", ")})"
              )
            case None => Right(())
          }
      }
    }

    /** Positional type compatibility with the FIRST declaring branch, using the ONE pairwise
      * predicate the cast/coercion layer already trusts (`SQLTypeUtils.matches`: same family, or
      * either side `Any`/`Null`).
      *
      * At parse time a bare column is `Any` and passes; once `SearchApi.resolveWithSchema` has
      * attached each branch's schema the SAME method rejects a `keyword UNION long` pair before any
      * request is sent — which is why this is `private[elastic]` and not `private[query]`.
      *
      * 🔴 Deliberately NOT `leastCommonSuperType`, which answers `Varchar` for `{BigInt, Varchar}`
      * — a super type, not a verdict — and would let `SELECT 1 … UNION SELECT 'a' …` through. And
      * this is the ONLY type guard: DuckDB performs implicit casting across set-operation branches
      * (MEASURED on 1.5.5.1: `BIGINT UNION VARCHAR` succeeds and yields a VARCHAR column), so the
      * engine is never a backstop.
      *
      * Column NAMES are NOT checked: SQL takes the first branch's.
      */
    private[elastic] def branchTypes(requests: Seq[SingleSearch]): Either[String, Unit] = {
      val known = requests.zipWithIndex.flatMap { case (r, i) => declared(r).map(f => (i, f)) }
      known.headOption match {
        case None => Right(())
        case Some((i0, f0)) =>
          val offending = for {
            (i, f)        <- known.tail
            ((a, b), pos) <- f0.zip(f).zipWithIndex
            if !SQLTypeUtils.matches(a.identifier.out, b.identifier.out)
          } yield (i, pos, a, b)
          offending.headOption match {
            case Some((i, pos, a, b)) =>
              Left(
                s"Set operation branches must project compatible types at column ${pos + 1}: " +
                s"branch ${i0 + 1} '${a.outputName}' is ${a.identifier.out.typeId}, branch " +
                s"${i + 1} '${b.outputName}' is ${b.identifier.out.typeId}"
              )
            case None => Right(())
          }
      }
    }
  }

  /** FROM-less SELECT of constant scalar expressions — the connection/health idiom of the
    * JDBC/SQLAlchemy ecosystem (issue #251): Tableau re-issues `SELECT 1` on every interaction,
    * Superset's connect test sends `SELECT 1` and its engine probe `SELECT 1 LIMIT 100`.
    *
    * EXECUTED AGAINST ELASTICSEARCH (lead direction 2026-09-02): the select-list is rewritten to a
    * SingleSearch over the dedicated handshake index and rides the existing SQL to Painless
    * translation as script_fields — a locally-answered handshake would report "connected" against a
    * dead cluster. Deliberately NOT a SearchStatement: the PUBLIC statement must never reach
    * SearchApi / the scroll machinery or the SingleSearch quota arm; only the INTERNAL rewrite
    * (toSingleSearch, always LIMIT 1) executes, below those seams.
    */
  case class FromlessSelect(
    // NO default for `select`: Select()'s own default is `SELECT *`
    // (fields = Seq(Field(Identifier("*"))), query/Select.scala) — a no-arg
    // FromlessSelect() would construct exactly the statement this class rejects.
    select: Select,
    limit: Option[Limit] = None
  ) extends DqlStatement {

    override def sql: String = s"$select${asString(limit)}"

    /** Output column names, SELECT-list order: explicit alias, else the rendered expression
      * (`SELECT 1` -> "1") — the convention hosts already accept (19.4 shim, MySQL family). The
      * internal `__cN` computed aliases (Select.fieldsWithComputedAliases) name the ES script
      * fields only; result assembly maps them back to these names.
      */
    lazy val columnNames: Seq[String] =
      select.fields.map(f => f.fieldAlias.map(_.alias).getOrElse(f.identifier.identifierName))

    /** The execution rewrite: exactly what the parser's `single` production would build for `SELECT
      * <items> FROM <dummyIndex> LIMIT 1` (Parser.single maps through .update(); programmatic
      * SingleSearch(...).update() is the established #212 pattern). ALWAYS `LIMIT 1`: the
      * statement's own LIMIT/OFFSET are applied on the assembled row by the executor (AD-12) and
      * must never leak here — a host's LIMIT 11000 would otherwise flip `requiresScrollPaging` and
      * route a handshake into scroll/PIT.
      */
    def toSingleSearch(dummyIndex: String): SingleSearch =
      SingleSearch(
        select = select,
        // `parts` is spelled out rather than left at its `Nil` default so this rewrite really is
        // the AST `FromParser.table` would build for the same text — otherwise the "generated SQL
        // is a fixed point" test compares a `Table` with no parts against a parsed one that has
        // them and fails on a rendering that is byte-identical (story 21.2).
        from = From(Seq(Table(dummyIndex, parts = Seq(NamePart(dummyIndex, quoted = false))))),
        where = None,
        limit = Some(Limit(1, None))
      ).update()

    /** ListMap rows silently collapse duplicate keys (PD-3). */
    private def checkDuplicateColumns(): Either[String, Unit] = {
      val dupes = columnNames.diff(columnNames.distinct).distinct
      if (dupes.nonEmpty)
        Left(
          s"Duplicate column name(s) ${dupes.mkString(", ")} in FROM-less SELECT: " +
          "alias each expression distinctly"
        )
      else Right(())
    }

    /** `Limit.validate()` is the Validation-trait default no-op and the `long` regex accepts a
      * sign, so `LIMIT -5` (and a negatively-wrapping Int overflow like `LIMIT 4294967291`,
      * LimitParser's `.toInt`) would silently yield zero rows through `take(negative)` — reject
      * loudly instead (AD-8). Honest bound: an overflow that wraps POSITIVE is indistinguishable
      * from that literal once Limit holds an Int — pre-existing LimitParser behaviour, every
      * statement kind.
      */
    private def checkLimit(): Either[String, Unit] =
      limit match {
        case Some(l) if l.limit < 0 || l.offset.exists(_.offset < 0) =>
          Left(s"LIMIT/OFFSET must be non-negative in FROM-less SELECT, got${l.sql}")
        case _ => Right(())
      }

    /** One field's acceptance guard — the lead's rule: literals or Painless-translatable scalars
      * with NO document-field reference; no window functions; no aggregations. ORDER IS
      * LOAD-BEARING: hasWindow FIRST (WindowFunction extends AggregateFunction,
      * function/aggregate/package.scala — aggregation-first makes the window reject dead code and
      * mislabels it); then aggregation; then `*`; then the field's OWN name (a bare
      * `Identifier("col")` has empty `functions` so its `dependencies` is EMPTY —
      * Function.dependencies filters on the walked identifiers, not the root); then `dependencies`
      * for names embedded through functions (`UPPER(col)`, `COALESCE(col,1)`); then the placeholder
      * walk.
      */
    private def checkField(f: Field): Either[String, Unit] =
      if (f.identifier.hasWindow)
        Left(s"Window function '${f.identifier.identifierName}' requires a FROM clause")
      else if (f.hasAggregation)
        Left(s"Aggregation '${f.identifier.identifierName}' requires a FROM clause")
      else if (f.identifier.name == "*")
        Left("SELECT * requires a FROM clause")
      else if (f.identifier.name.nonEmpty)
        Left(s"Column reference '${f.identifier.name}' requires a FROM clause")
      else
        f.identifier.dependencies.headOption match {
          case Some(dep) => Left(s"Column reference '${dep.name}' requires a FROM clause")
          case None      => FromlessSelect.checkPlaceholders(f.identifier)
        }

    override def validate(): Either[String, Unit] =
      for {
        _ <- select.except match {
          case Some(_) => Left("EXCEPT(...) requires a FROM clause")
          case None    => Right(())
        }
        _ <- checkLimit()
        _ <- checkDuplicateColumns()
        _ <- select.fields.foldLeft[Either[String, Unit]](Right(())) { (acc, f) =>
          acc.flatMap(_ => checkField(f))
        }
      } yield ()
  }

  object FromlessSelect {

    /** Reject non-constant placeholder Values ANYWHERE in the expression tree (walk shape mirrors
      * FunctionUtils.funIdentifiers / aggregateFunctions, function/package.scala):
      *   - ParamValue renders `params.paramValue`; a nested unbound `?` reads as painless NULL at
      *     ES — the silent-NULL #253 class, re-closed here for the ES path. (Top-level `SELECT ?`
      *     is the only host-reachable shape — JDBC substitutes `?` textually before core sees the
      *     SQL — but raw gateway.run callers can nest it.)
      *   - IdValue / IngestTimestampValue render moustache `{{...}}` templates — not query-context
      *     painless (programmatic-only; no select-position production).
      *   - Values (array literals) render multi-element painless lists, defeating the 1-element
      *     script_fields unwrap contract (AD-11). Every arm verified against the real classes;
      *     extend deliberately when a new container production appears — never by falling through.
      *     Arm ORDER is load-bearing: Conversion embeds its operand and overrides `args` to empty
      *     (before FunctionN); Identifier extends FunctionChain (before FunctionChain).
      */
    private[query] def checkPlaceholders(f: function.Function): Either[String, Unit] =
      f match {
        case ParamValue =>
          Left("Unbound parameter '?' is not supported without a FROM clause")
        case IdValue | IngestTimestampValue =>
          Left("Ingest placeholder is not supported without a FROM clause")
        case vs: Values[_, _] =>
          Left(s"Array literal ${vs.sql} is not supported without a FROM clause")
        case c: function.convert.Conversion =>
          // Conversion EMBEDS its operand (value: PainlessScript) and its args are empty
          c.value match {
            case g: function.Function => checkPlaceholders(g)
            case _                    => Right(())
          }
        case id: Identifier =>
          id.functions.foldLeft[Either[String, Unit]](Right(())) { (acc, g) =>
            acc.flatMap(_ => checkPlaceholders(g))
          }
        case fn: function.FunctionN[_, _] =>
          // covers BinaryFunction (args = List(left, right)), hence ArithmeticExpression
          // operands, COALESCE/NULLIF/GREATEST/LEAST args, ...
          fn.args.foldLeft[Either[String, Unit]](Right(())) { (acc, a) =>
            a match {
              case g: function.Function => acc.flatMap(_ => checkPlaceholders(g))
              case _                    => acc
            }
          }
        case fwi: function.FunctionWithIdentifier => checkPlaceholders(fwi.identifier)
        case fc: function.FunctionChain =>
          fc.functions.foldLeft[Either[String, Unit]](Right(())) { (acc, g) =>
            acc.flatMap(_ => checkPlaceholders(g))
          }
        case _ => Right(())
      }
  }

  sealed trait DmlStatement extends Statement

  case class OnConflict(target: Option[Seq[String]], doUpdate: Boolean) extends Token {
    override def sql: String = {
      val targetSql =
        target match {
          case Some(t) =>
            if (t.isEmpty) " () " else s" (${t.map(renderColumnName).mkString(", ")}) "
          case None => " "
        }
      val actionSql = if (doUpdate) "DO UPDATE" else "DO NOTHING"
      s" ON CONFLICT$targetSql$actionSql"
    }
  }

  case class Insert(
    table: String,
    cols: Seq[String],
    values: Either[SearchStatement, Seq[Seq[Value[_]]]],
    onConflict: Option[OnConflict] = None,
    /** The reference as the ordered part list the statement wrote — never split, never
      * role-assigned, never dropped (#85, story 21.2 AD-1; extended to every DDL/DML statement by
      * story 21.7). `Nil` for a programmatic construction AND for a single bare part, which IS the
      * name — so the AST of every bare-spelled statement is byte-identical to what it was before
      * story 21.7. Read by the render only; `name` stays what every consumer reads.
      */
    parts: Seq[NamePart] = Nil
  ) extends DmlStatement {
    lazy val conflictTarget: Option[Seq[String]] = onConflict.flatMap(_.target)

    lazy val doUpdate: Boolean = onConflict.exists(_.doUpdate)

    private def valueToSql(value: Value[_]): String = value match {
      case v if v.isInstanceOf[ObjectValues] =>
        v.asInstanceOf[ObjectValues]
          .values
          .map(valueToSql)
          .mkString("[", ", ", "]")
      case v if v.isInstanceOf[ObjectValue] =>
        v.asInstanceOf[ObjectValue]
          .value
          .map { case (k, v) =>
            v match {
              case IdValue | IngestTimestampValue => s"""$k = "${v.ddl}""""
              case _                              => s"""$k = ${v.ddl}"""
            }
          }
          .mkString("{", ", ", "}")
      case v => s"${v.ddl}"
    }

    private def rowToSql(row: Seq[Value[_]]): String = {
      val rowSql = row
        .map(valueToSql)
        .mkString(", ")
      s"($rowSql)"
    }

    override def sql: String = {
      val target = renderName(parts, table)
      values match {
        case Left(query) if cols.isEmpty =>
          s"INSERT INTO $target ${query.sql}${asString(onConflict)}"
        case Left(query) =>
          s"INSERT INTO $target (${cols.map(renderColumnName).mkString(",")}) ${query.sql}${asString(onConflict)}"
        case Right(rows) =>
          val valuesSql = rows
            .map(rowToSql)
            .mkString(", ")
          s"INSERT INTO $target (${cols.map(renderColumnName).mkString(",")}) VALUES $valuesSql${asString(onConflict)}"
      }
    }

    override def validate(): Either[String, Unit] = {
      for {
        _ <- values match {
          case Left(query) => query.validate()
          case Right(rows) =>
            val invalidRows = rows.filter(_.size != cols.size)
            if (invalidRows.nonEmpty)
              Left(
                s"Some rows have invalid number of values: ${invalidRows
                  .map(r => s"(${r.map(_.value).mkString(",")})")
                  .mkString("; ")}"
              )
            else Right(())
          case _ =>
            Right(())
        }
        _ <- conflictTarget match {
          case Some(target) =>
            values match {
              case Left(query: SingleSearch) =>
                val queryFields =
                  query.select.fields.map(f => f.fieldAlias.map(_.alias).getOrElse(f.sourceField))
                if (!target.forall(queryFields.contains))
                  Left(
                    s"Conflict target columns (${target.mkString(",")}) must be part of the inserted columns from SELECT (${queryFields
                      .mkString(",")})"
                  )
                else Right(())
              case _ =>
                if (!target.forall(cols.contains))
                  Left(
                    s"Conflict target columns (${target.mkString(",")}) must be part of the inserted columns (${cols
                      .mkString(",")})"
                  )
                else Right(())

            }
          case _ => Right(())
        }
      } yield ()
    }

    def toJson: Option[JsonNode] = {
      values match {
        case Right(rows) =>
          val maps: Seq[ObjectValue] =
            for (row <- rows) yield {
              val map: ListMap[String, Value[_]] =
                ListMap(
                  cols
                    .zip(row)
                    .map { case (k, v) =>
                      k -> v
                    }: _*
                )
              ObjectValue(map)
            }
          val json: JsonNode = ObjectValues(maps)
          Some(json)
        case _ => None
      }
    }
  }

  case class Update(
    table: String,
    values: ListMap[String, PainlessScript],
    where: Option[Where],
    /** The reference as the ordered part list the statement wrote — never split, never
      * role-assigned, never dropped (#85, story 21.2 AD-1; extended to every DDL/DML statement by
      * story 21.7). `Nil` for a programmatic construction AND for a single bare part, which IS the
      * name — so the AST of every bare-spelled statement is byte-identical to what it was before
      * story 21.7. Read by the render only; `name` stays what every consumer reads.
      */
    parts: Seq[NamePart] = Nil
  ) extends DmlStatement {
    // Uses `value.sql` (not `value.value`) for the literal branch so the rendered string is
    // re-parseable into the same AST. `value.value` loses the syntactic shape of string
    // literals (`'USA'` becomes `USA`), and any downstream consumer that re-parses this output
    // — e.g. GatewayApi.run → api.updateByQuery(table, update.sql) before it was changed to
    // accept the AST directly — would mis-parse the bare token as an Identifier and silently
    // null the column via a broken painless script.
    override def sql: String = s"UPDATE ${renderName(parts, table)} SET ${values
      .map { case (k, v) =>
        val key = renderColumnName(k)
        v match {
          case value: Value[_] => s"$key = ${value.sql}"
          case painlessScript  => s"$key = ${painlessScript.sql}"
        }
      }
      .mkString(", ")}${where.map(w => s"${w.sql}").getOrElse("")}"

    // The parse path validates every statement kind; without this, an aggregate in a DML WHERE
    // (`UPDATE t SET ... WHERE COUNT(x) > 5`) slipped through as `match_all` and touched EVERY
    // document (BIDC-2 review, S2-2).
    override def validate(): Either[String, Unit] = where.map(_.validate()).getOrElse(Right(()))

    lazy val customPipeline: IngestPipeline = IngestPipeline(
      s"update-$table-${Instant.now.toEpochMilli}",
      IngestPipelineType.Custom,
      values.map { case (k, v) =>
        v match {
          case value: Value[_] =>
            SetProcessor(
              pipelineType = IngestPipelineType.Custom,
              column = k,
              value = value
            )
          case script =>
            ScriptProcessor.fromScript(k, script, pipelineType = IngestPipelineType.Custom)
        }
      }.toSeq
    )

  }

  case class Delete(table: Table, where: Option[Where]) extends DmlStatement {
    // `Table.render`, not `table.name`: DELETE routes through `FromParser.table` (widened from a
    // bare `ident` for #213), so it can carry a quoted qualifier — and `table.name` is the BARE
    // index, which would delete the qualifier from the rendering and break the `Parser(stmt.sql)
    // == Right(stmt)` fixed point. It is deliberately not `table.sql`: a DELETE has no alias and
    // no joins to render (story 21.2).
    override def sql: String =
      s"DELETE FROM ${Table.render(table.parts, table.name)}${asString(where)}"

    // `DELETE FROM t WHERE COUNT(x) > 5` used to become `match_all` and WIPE the index (S2-2).
    override def validate(): Either[String, Unit] =
      // Story 22.1 — the grammar already rejects `DELETE FROM (SELECT …) d`, but that is a PARSER
      // guard: `GatewayApi.run(statement: Statement)` accepts a programmatically built `Delete`,
      // and `Table.validate()`'s AD-1 arm is SATISFIED by a well-formed derived table. Without
      // this the delete-by-query would target an index named after the subquery's alias — or, if
      // one of that name happens to exist, the wrong index entirely. Every other AD-1 invariant in
      // this story is enforced in `validate()`; this is the same belt for the DML side.
      if (table.derived.isDefined)
        Left(
          "DELETE cannot target a derived table (subquery in FROM): Elasticsearch deletes by " +
          "query over an index."
        )
      else where.map(_.validate()).getOrElse(Right(()))
  }

  sealed trait FileFormat extends Token {
    def name: String

    override def sql: SQL = s" FILE_FORMAT = $name"
  }

  case object Parquet extends FileFormat {
    override def name: String = "PARQUET"
  }

  case object Json extends FileFormat {
    override def name: String = "JSON"
  }

  case object JsonArray extends FileFormat {
    override def name: String = "JSON_ARRAY"
  }

  case object Delta extends FileFormat {
    override def name: String = "DELTA_LAKE"
  }

  case object Unknown extends FileFormat {
    override def name: String = "UNKNOWN"
  }

  object FileFormat {
    def apply(format: String): FileFormat = {
      format.toUpperCase match {
        case "PARQUET"    => Parquet
        case "JSON"       => Json
        case "JSON_ARRAY" => JsonArray
        case "DELTA_LAKE" => Delta
        case _            => Unknown
      }
    }
  }

  case class CopyInto(
    source: String,
    targetTable: String,
    fileFormat: Option[FileFormat] = None,
    onConflict: Option[OnConflict] = None,
    /** The reference as the ordered part list the statement wrote — never split, never
      * role-assigned, never dropped (#85, story 21.2 AD-1; extended to every DDL/DML statement by
      * story 21.7). `Nil` for a programmatic construction AND for a single bare part, which IS the
      * name — so the AST of every bare-spelled statement is byte-identical to what it was before
      * story 21.7. Read by the render only; `name` stays what every consumer reads.
      */
    parts: Seq[NamePart] = Nil
  ) extends DmlStatement {
    override def sql: String = {
      // The grammar only accepts a quoted source literal, so render one — an unquoted path
      // (`FROM /tmp/data.json`) is not valid SQL and cannot survive a re-parse. The escape rule has
      // ONE owner (`escapeStringLiteral`); this site used to inline a byte-identical fourth copy,
      // which is how a rule and its reverse drift apart.
      val quoted = s"'${escapeStringLiteral(source)}'"
      s"COPY INTO ${renderName(parts, targetTable)} FROM $quoted${asString(fileFormat)}${asString(
        onConflict
      )}"
    }
  }

  sealed trait DdlStatement extends Statement

  sealed trait PipelineStatement extends Statement

  case class CreatePipeline(
    name: String,
    pipelineType: IngestPipelineType,
    ifNotExists: Boolean = false,
    orReplace: Boolean = false,
    processors: Seq[IngestProcessor],
    /** The reference as the ordered part list the statement wrote — never split, never
      * role-assigned, never dropped (#85, story 21.2 AD-1; extended to every DDL/DML statement by
      * story 21.7). `Nil` for a programmatic construction AND for a single bare part, which IS the
      * name — so the AST of every bare-spelled statement is byte-identical to what it was before
      * story 21.7. Read by the render only; `name` stays what every consumer reads.
      */
    parts: Seq[NamePart] = Nil
  ) extends PipelineStatement
      with DdlStatement {
    override def sql: String = {
      val processorsDdl = processors.map(_.ddl).mkString(", ")
      val replaceClause = if (orReplace) " OR REPLACE" else ""
      val ineClause = if (!orReplace && ifNotExists) " IF NOT EXISTS" else ""
      s"CREATE$replaceClause PIPELINE$ineClause ${renderName(parts, name)} WITH PROCESSORS ($processorsDdl)"
    }

    lazy val ddlPipeline: IngestPipeline =
      IngestPipeline(name, pipelineType, processors)
  }

  sealed trait AlterPipelineStatement extends AlterTableStatement

  case class AddPipelineProcessor(processor: IngestProcessor) extends AlterPipelineStatement {
    override def sql: String = s"ADD PROCESSOR ${processor.ddl}"
    override def ddlProcessor: Option[IngestProcessor] = Some(processor)
  }
  case class DropPipelineProcessor(processorType: IngestProcessorType, column: String)
      extends AlterPipelineStatement {
    override def sql: String =
      s"DROP PROCESSOR ${processorType.name.toUpperCase}(${renderColumnName(column)})"
  }
  case class AlterPipelineProcessor(processor: IngestProcessor) extends AlterPipelineStatement {
    override def sql: String = s"ALTER PROCESSOR ${processor.ddl}"
    override def ddlProcessor: Option[IngestProcessor] = Some(processor)
  }

  case class AlterPipeline(
    name: String,
    ifExists: Boolean,
    statements: List[AlterPipelineStatement],
    /** See `Table.parts` / story 21.7: `Nil` unless the reference carries a qualifier or quoting.
      */
    parts: Seq[NamePart] = Nil
  ) extends PipelineStatement
      with DdlStatement {
    override def sql: String = {
      val ifExistsClause = if (ifExists) " IF EXISTS" else ""
      val parenthesesNeeded = statements.size > 1
      val statementsSql = if (parenthesesNeeded) {
        statements.map(_.sql).mkString("(\n\t", ",\n\t", "\n)")
      } else {
        statements.map(_.sql).mkString("")
      }
      s"ALTER PIPELINE$ifExistsClause ${renderName(parts, name)} $statementsSql"
    }

    lazy val ddlProcessors: Seq[IngestProcessor] = statements.flatMap(_.ddlProcessor)

    lazy val pipeline: IngestPipeline =
      IngestPipeline(
        s"alter-pipeline-$name-${Instant.now}",
        IngestPipelineType.Custom,
        ddlProcessors
      )
  }

  case class DropPipeline(name: String, ifExists: Boolean = false, parts: Seq[NamePart] = Nil)
      extends PipelineStatement
      with DdlStatement {
    override def sql: String = {
      val ifExistsClause = if (ifExists) "IF EXISTS " else ""
      s"DROP PIPELINE $ifExistsClause${renderName(parts, name)}"
    }
  }

  case class ShowPipeline(name: String, parts: Seq[NamePart] = Nil)
      extends PipelineStatement
      with DqlStatement {
    override def sql: String = s"SHOW PIPELINE ${renderName(parts, name)}"
  }

  case class ShowCreatePipeline(name: String, parts: Seq[NamePart] = Nil)
      extends PipelineStatement
      with DqlStatement {
    override def sql: String = s"SHOW CREATE PIPELINE ${renderName(parts, name)}"
  }

  case class DescribePipeline(name: String, parts: Seq[NamePart] = Nil)
      extends PipelineStatement
      with DqlStatement {
    override def sql: String = s"DESCRIBE PIPELINE ${renderName(parts, name)}"
  }

  case object ShowPipelines extends PipelineStatement with DqlStatement {
    override def sql: String = s"SHOW PIPELINES"
  }

  sealed trait TableStatement extends Statement

  sealed trait MaterializedViewStatement extends TableStatement

  case class CreateMaterializedView(
    view: String,
    dql: DqlStatement,
    ifNotExists: Boolean = false,
    orReplace: Boolean = false,
    frequency: Option[Frequency] = None,
    options: ListMap[String, Value[_]] = ListMap.empty,
    /** See `Table.parts` / story 21.7: `Nil` unless the reference carries a qualifier or quoting.
      */
    parts: Seq[NamePart] = Nil
  ) extends MaterializedViewStatement
      with DdlStatement {

    /** Same reasoning as `CreateTable.validate()` (story BIDC-8): the view's query is validated by
      * the rules that govern any SELECT — this statement used to inherit the no-op default.
      *
      * Story 22.1 adds the derived-table refusal, and it is ordered FIRST so the DERIVED message
      * wins whenever both apply. A materialized view is an Elasticsearch TRANSFORM, and the
      * extension that plans it (`MaterializedViewExtension`, softclient4es-extensions) keys on
      * `from.tables` by INDEX NAME: a derived table's name is its ALIAS, so the transform would be
      * planned over an index that does not exist — silently (the extensions#45/#46 class). A
      * cross-index JOIN in a materialized view IS supported by that extension, so the guard is on
      * the derived half only.
      */
    override def validate(): Either[String, Unit] =
      // Story 22.6 — FIRST, and it narrows nothing: `search` (below) THROWS
      // `IllegalArgumentException("Materialized view must be a single search")` for EVERY
      // non-`SingleSearch` body, `UNION ALL` included, and `MaterializedViewExtension` reads
      // `create.search`. So every set operation already failed here; what changes is that it now
      // fails as a parse-time 400 carrying a reason instead of a caught throwable.
      if (dql.isInstanceOf[MultiSearch])
        Left(
          "MATERIALIZED VIEW over a set operation is not supported: UNION ALL, UNION, INTERSECT " +
          "and EXCEPT are not materializable, because an Elasticsearch transform reads indices " +
          "and cannot combine or de-duplicate across sources. Materialize each branch as its own " +
          "view."
        )
      else if (derivedTablesPresent(dql))
        Left(
          "MATERIALIZED VIEW over a derived table (subquery in FROM/JOIN) is not supported: an " +
          "Elasticsearch transform reads indices. Materialize the subquery as its own view and " +
          "reference it."
        )
      else if (whereSubqueriesPresent(dql))
        Left(
          "MATERIALIZED VIEW over a WHERE subquery (IN (SELECT ...), EXISTS (SELECT ...), a " +
          "scalar or quantified subquery) is not supported: an Elasticsearch transform cannot run " +
          "the inner query. Materialize the subquery's values first and reference them."
        )
      else dql.validate()

    override def sql: String = {
      // The leading space belongs HERE, not to `Frequency.sql`: `TransformConfig` renders the same
      // value on a line of its own and supplies its own indentation. Without it the render read
      // `... MATERIALIZED VIEW mvREFRESH EVERY 8 SECONDS ...`, which re-parses as a view literally
      // named `mvREFRESH` and then fails - so `SHOW CREATE MATERIALIZED VIEW` emitted a statement
      // no parser accepts whenever the view carried a `REFRESH EVERY` clause (story HELP-1b).
      val frequencySql = frequency match {
        case Some(freq) => s" ${freq.sql}"
        case None       => ""
      }
      val optionsSql = if (options.nonEmpty) {
        s" WITH (${options
          .map { case (k, v) => s"$k = $v" }
          .mkString(", ")})"
      } else {
        ""
      }
      val replaceClause = if (orReplace) " OR REPLACE" else ""
      val ineClause = if (!orReplace && ifNotExists) " IF NOT EXISTS" else ""
      s"CREATE$replaceClause MATERIALIZED VIEW$ineClause ${renderName(
        parts,
        view
      )}$frequencySql$optionsSql AS ${dql.sql}"
    }

    lazy val search: SingleSearch = dql match {
      case s: SingleSearch => s
      case _ => throw new IllegalArgumentException("Materialized view must be a single search")
    }

    lazy val userLatency: Option[Duration] = options.get("user_latency") match {
      case Some(value) =>
        value match {
          case s: StringValue =>
            TransformTimeInterval(s.value).map(t => Duration.ofSeconds(t.toSeconds))
          case i: LongValue => Some(Duration.ofSeconds(i.value))
          case _            => None
        }
      case None => None
    }

    lazy val delay: Option[Delay] = options.get("delay") match {
      case Some(value) =>
        value match {
          case s: StringValue => TransformTimeInterval(s.value)
          case i: LongValue   => Some(Delay(TransformTimeUnit.Seconds, i.value))
          case _              => None
        }
      case None => None
    }
  }

  case class DropMaterializedView(
    name: String,
    ifExists: Boolean = false,
    parts: Seq[NamePart] = Nil
  ) extends MaterializedViewStatement
      with DdlStatement {
    override def sql: String = {
      val ifExistsClause = if (ifExists) "IF EXISTS " else ""
      s"DROP MATERIALIZED VIEW $ifExistsClause${renderName(parts, name)}"
    }
  }

  case class RefreshMaterializedView(
    name: String,
    ifExists: Boolean,
    scheduleNow: Boolean,
    parts: Seq[NamePart] = Nil
  ) extends MaterializedViewStatement
      with DdlStatement {
    override def sql: String = {
      val ifExistsClause = if (ifExists) "IF EXISTS " else ""
      val scheduleNowClause = if (scheduleNow) " WITH SCHEDULE NOW" else ""
      s"REFRESH MATERIALIZED VIEW $ifExistsClause${renderName(parts, name)}$scheduleNowClause"
    }
  }

  case object ShowMaterializedViews extends MaterializedViewStatement with DqlStatement {
    override def sql: String = s"SHOW MATERIALIZED VIEWS"
  }

  case class ShowMaterializedView(name: String, parts: Seq[NamePart] = Nil)
      extends MaterializedViewStatement
      with DqlStatement {
    override def sql: String = s"SHOW MATERIALIZED VIEW ${renderName(parts, name)}"
  }

  case class ShowMaterializedViewStatus(name: String, parts: Seq[NamePart] = Nil)
      extends MaterializedViewStatement
      with DqlStatement {
    override def sql: String = s"SHOW MATERIALIZED VIEW STATUS ${renderName(parts, name)}"
  }

  case class ShowCreateMaterializedView(name: String, parts: Seq[NamePart] = Nil)
      extends MaterializedViewStatement
      with DqlStatement {
    override def sql: String = s"SHOW CREATE MATERIALIZED VIEW ${renderName(parts, name)}"
  }

  case class DescribeMaterializedView(name: String, parts: Seq[NamePart] = Nil)
      extends MaterializedViewStatement
      with DqlStatement {
    override def sql: String = s"DESCRIBE MATERIALIZED VIEW ${renderName(parts, name)}"
  }

  /** Does this column, or any of its sub-fields, carry a `SCRIPT AS (…)`? Sub-fields count: a
    * computed column can be declared inside a `STRUCT`, and `validateScriptReferences` recurses.
    */
  private def hasScript(column: Column): Boolean =
    column.script.isDefined || column.multiFields.exists(hasScript)

  case class CreateTable(
    table: String,
    ddl: Either[SearchStatement, List[Column]],
    ifNotExists: Boolean = false,
    orReplace: Boolean = false,
    primaryKey: List[String] = Nil,
    partitionBy: Option[PartitionDate] = None,
    options: ListMap[String, Value[_]] = ListMap.empty,
    /** See `Table.parts` / story 21.7: `Nil` unless the reference carries a qualifier or quoting.
      */
    parts: Seq[NamePart] = Nil
  ) extends TableStatement
      with DdlStatement {

    lazy val partitioned: Boolean = partitionBy.isDefined

    /** The AS-SELECT is a full `SearchStatement` and every rule `SingleSearch.validate()` enforces
      * applies to it unchanged; before story BIDC-8 this statement inherited the no-op default, so
      * a CTAS carried its query past `Parser.apply` UNVALIDATED (measured: `CREATE TABLE t AS
      * SELECT a.x, b.y FROM t a, t b` was accepted while the bare SELECT was rejected). The column
      * form keeps its previous acceptance — nothing validated it before and nothing new does now.
      */
    override def validate(): Either[String, Unit] =
      ddl match {
        case Left(select) => select.validate()
        // 🔴 Validated through `schema`, i.e. on the RESOLVED table, not on the parsed column list.
        // `schema` is a memoised `lazy val` ending in `Table.update()`, which settles every path
        // and re-derives every `SCRIPT AS (…)` against the final column list -- so a forward
        // reference (`CREATE TABLE t (c INTEGER SCRIPT AS (n + 1), n INTEGER)`) resolves and is
        // accepted, where validating the parsed columns would reject it. It costs nothing: every
        // caller that goes on to execute this statement forces the same `lazy val`.
        // 🔴 The guard is the OPTIMISATION, and it reads the PARSED columns so it costs nothing:
        // a table with no computed column has nothing to validate, and this way it never forces
        // the `schema` lazy val (`Table.update()`) during a parse that would not otherwise have
        // done so. Measured interleaved, 3 rounds: plain DDL 53.5-55.5 us control vs 55.4-56.4 us
        // branch -- overlapping, no signal; only `SCRIPT AS` DDL pays (204.6-208.4 -> 222.3-236.3).
        case Right(cols) if cols.exists(hasScript) => validateScriptReferences(schema)
        case Right(_)                              => Right(())
      }

    override def sql: String = {
      val replaceClause = if (orReplace) " OR REPLACE" else ""
      val ineClause = if (!orReplace && ifNotExists) " IF NOT EXISTS" else ""
      val target = renderName(parts, table)
      ddl match {
        case Left(select) =>
          s"CREATE$replaceClause TABLE$ineClause $target AS ${select.sql}"
        case Right(columns) =>
          val colsSql = columns.map(_.sql).mkString(",\n ")
          val primaryKeyClause =
            if (primaryKey.nonEmpty)
              s",\n PRIMARY KEY (${primaryKey.map(renderColumnName).mkString(", ")})"
            else
              ""
          val partitionClause = partitionBy match {
            case Some(partition) => partition.sql
            case None            => ""
          }
          val optionsClause =
            if (options.nonEmpty)
              s" OPTIONS ${ObjectValue(options).ddl}"
            else
              ""
          s"CREATE$replaceClause TABLE$ineClause $target (\n $colsSql$primaryKeyClause\n)$partitionClause$optionsClause"
      }
    }

    private val artificialPkColumnName: String =
      s"${table}_${sqlConfig.artificialPrimaryKeyColumnName}"

    lazy val columns: Seq[Column] = {
      val artificialPkColumn = if (primaryKey.isEmpty) {
        Seq(
          Column(
            name = artificialPkColumnName,
            dataType = SQLTypes.Keyword,
            defaultValue = Some(IdValue),
            comment = Some("Artificial primary key column")
          )
        )
      } else {
        Nil
      }
      (ddl match {
        case Left(select) =>
          select match {
            case s: SingleSearch =>
              s.select.fields.map(f => Column(f.identifier.aliasOrName, f.out))
            case m: MultiSearch =>
              m.requests.headOption
                .map { req =>
                  req.select.fields.map(f => Column(f.identifier.aliasOrName, f.out))
                }
                .getOrElse(Nil)
            case _ => Nil
          }
        case Right(cols) => cols
      }).filterNot(_.name == artificialPkColumnName) ++ artificialPkColumn
    }

    lazy val mappings: ListMap[String, Value[_]] = options.get("mappings") match {
      case Some(value) =>
        value match {
          case o: ObjectValue => o.value
          case _              => ListMap.empty
        }
      case None => ListMap.empty
    }

    lazy val settings: ListMap[String, Value[_]] = options.get("settings") match {
      case Some(value) =>
        value match {
          case o: ObjectValue => o.value
          case _              => ListMap.empty
        }
      case None => ListMap.empty
    }

    lazy val aliases: ListMap[String, Value[_]] = options.get("aliases") match {
      case Some(value) =>
        value match {
          case o: ObjectValue => o.value
          case _              => ListMap.empty
        }
      case None => ListMap.empty
    }

    lazy val materializedViews: List[String] = options.get("materialized_views") match {
      case Some(value) =>
        value match {
          case ov: StringValues =>
            ov.values.flatMap {
              case o: StringValue =>
                Some(o.value)
              case _ => None
            }.toList
          case _ => Nil
        }
      case None => Nil
    }

    lazy val tableType: TableType = (mappings.get("_meta") match {
      case Some(meta) =>
        meta match {
          case o: ObjectValue =>
            o.value.get("type") match {
              case Some(s: StringValue) => Some(TableType(s.value))
              case _                    => None
            }
          case _ => None
        }
      case None => None
    }).getOrElse(TableType.Regular)

    lazy val schema: Schema = Schema(
      name = table,
      columns = columns.toList,
      primaryKey = primaryKey,
      partitionBy = partitionBy,
      mappings = mappings,
      settings = settings,
      aliases = aliases,
      materializedViews = materializedViews,
      tableType = tableType
    ).update()

    lazy val defaultPipeline: IngestPipeline = schema.defaultPipeline

  }

  case class AlterTable(
    table: String,
    ifExists: Boolean,
    statements: List[AlterTableStatement],
    /** See `Table.parts` / story 21.7: `Nil` unless the reference carries a qualifier or quoting.
      */
    parts: Seq[NamePart] = Nil
  ) extends TableStatement
      with DdlStatement {
    override def sql: String = {
      val ifExistsClause = if (ifExists) " IF EXISTS" else ""
      val parenthesesNeeded = statements.size > 1
      val statementsSql = if (parenthesesNeeded) {
        statements.map(_.sql).mkString("(\n\t", ",\n\t", "\n)")
      } else {
        statements.map(_.sql).mkString("")
      }
      s"ALTER TABLE$ifExistsClause ${renderName(parts, table)} $statementsSql"
    }

    lazy val processors: Seq[IngestProcessor] = statements.flatMap(_.ddlProcessor)

    lazy val pipeline: IngestPipeline =
      IngestPipeline(s"alter-$table-${Instant.now}", IngestPipelineType.Custom, processors)
  }

  sealed trait AlterTableStatement extends Token {
    def ddlProcessor: Option[IngestProcessor] = None
  }
  case class AddColumn(column: Column, ifNotExists: Boolean = false) extends AlterTableStatement {
    override def sql: String = {
      val ifNotExistsClause = if (ifNotExists) " IF NOT EXISTS" else ""
      s"ADD COLUMN$ifNotExistsClause ${column.sql}"
    }
  }
  case class DropColumn(columnName: String, ifExists: Boolean = false) extends AlterTableStatement {
    override def sql: String = {
      val ifExistsClause = if (ifExists) " IF EXISTS" else ""
      s"DROP COLUMN$ifExistsClause ${renderColumnName(columnName)}"
    }
    override def ddlProcessor: Option[IngestProcessor] = Some(
      RemoveProcessor(column = columnName)
    )
  }
  case class RenameColumn(oldName: String, newName: String) extends AlterTableStatement {
    override def sql: String =
      s"RENAME COLUMN ${renderColumnName(oldName)} TO ${renderColumnName(newName)}"
    override def ddlProcessor: Option[IngestProcessor] = Some(
      RenameProcessor(column = oldName, newName = newName)
    )
  }
  case class AlterColumnOptions(
    columnName: String,
    options: ListMap[String, Value[_]],
    ifExists: Boolean = false
  ) extends AlterTableStatement {
    override def sql: String = {
      val ifExistsClause = if (ifExists) " IF EXISTS" else ""
      s"ALTER COLUMN$ifExistsClause ${renderColumnName(columnName)} SET OPTIONS (${options
        .map { case (k, v) => s"$k = $v" }
        .mkString(", ")})"
    }
  }
  case class AlterColumnOption(
    columnName: String,
    optionKey: String,
    optionValue: Value[_],
    ifExists: Boolean = false
  ) extends AlterTableStatement {
    override def sql: String = {
      val ifExistsClause = if (ifExists) " IF EXISTS" else ""
      s"ALTER COLUMN$ifExistsClause ${renderColumnName(columnName)} SET OPTION ($optionKey = $optionValue)"
    }
  }
  case class DropColumnOption(
    columnName: String,
    optionKey: String,
    ifExists: Boolean = false
  ) extends AlterTableStatement {
    override def sql: String = {
      val ifExistsClause = if (ifExists) " IF EXISTS" else ""
      s"ALTER COLUMN$ifExistsClause ${renderColumnName(columnName)} DROP OPTION $optionKey"
    }
  }
  case class AlterColumnType(columnName: String, newType: SQLType, ifExists: Boolean = false)
      extends AlterTableStatement {
    override def sql: String = {
      val ifExistsClause = if (ifExists) " IF EXISTS" else ""
      s"ALTER COLUMN$ifExistsClause ${renderColumnName(columnName)} SET DATA TYPE $newType"
    }
  }
  case class AlterColumnScript(
    columnName: String,
    newScript: ScriptProcessor,
    ifExists: Boolean = false
  ) extends AlterTableStatement {
    override def sql: String = {
      val ifExistsClause = if (ifExists) " IF EXISTS" else ""
      s"ALTER COLUMN$ifExistsClause ${renderColumnName(columnName)} SET SCRIPT AS (${newScript.script})"
    }
  }
  case class DropColumnScript(columnName: String, ifExists: Boolean = false)
      extends AlterTableStatement {
    override def sql: String = {
      val ifExistsClause = if (ifExists) " IF EXISTS" else ""
      s"ALTER COLUMN$ifExistsClause ${renderColumnName(columnName)} DROP SCRIPT"
    }
  }
  case class AlterColumnDefault(
    columnName: String,
    defaultValue: Value[_],
    ifExists: Boolean = false
  ) extends AlterTableStatement {
    override def sql: String = {
      val ifExistsClause = if (ifExists) " IF EXISTS" else ""
      s"ALTER COLUMN$ifExistsClause ${renderColumnName(columnName)} SET DEFAULT $defaultValue"
    }
    override def ddlProcessor: Option[IngestProcessor] =
      Some(
        SetProcessor(
          column = columnName,
          value = defaultValue
        )
      )
  }
  case class DropColumnDefault(columnName: String, ifExists: Boolean = false)
      extends AlterTableStatement {
    override def sql: String = {
      val ifExistsClause = if (ifExists) " IF EXISTS" else ""
      s"ALTER COLUMN$ifExistsClause ${renderColumnName(columnName)} DROP DEFAULT"
    }
  }
  case class AlterColumnComment(
    columnName: String,
    comment: String,
    ifExists: Boolean = false
  ) extends AlterTableStatement {
    override def sql: String = {
      val ifExistsClause = if (ifExists) " IF EXISTS" else ""
      s"ALTER COLUMN$ifExistsClause ${renderColumnName(columnName)} SET COMMENT '${escapeStringLiteral(comment)}'"
    }
  }
  case class DropColumnComment(columnName: String, ifExists: Boolean = false)
      extends AlterTableStatement {
    override def sql: String = {
      val ifExistsClause = if (ifExists) " IF EXISTS" else ""
      s"ALTER COLUMN$ifExistsClause ${renderColumnName(columnName)} DROP COMMENT"
    }
  }
  case class AlterColumnNotNull(columnName: String, ifExists: Boolean = false)
      extends AlterTableStatement {
    override def sql: String = {
      val ifExistsClause = if (ifExists) " IF EXISTS" else ""
      s"ALTER COLUMN$ifExistsClause ${renderColumnName(columnName)} SET NOT NULL"
    }
  }
  case class DropColumnNotNull(columnName: String, ifExists: Boolean = false)
      extends AlterTableStatement {
    override def sql: String = {
      val ifExistsClause = if (ifExists) " IF EXISTS" else ""
      s"ALTER COLUMN$ifExistsClause ${renderColumnName(columnName)} DROP NOT NULL"
    }
  }
  case class AlterColumnFields(
    columnName: String,
    fields: Seq[Column],
    ifExists: Boolean = false
  ) extends AlterTableStatement {
    override def sql: String = {
      val ifExistsClause = if (ifExists) " IF EXISTS" else ""
      val fieldsSql = fields.map(_.sql).mkString("(\n\t\t", ",\n\t\t", "\n\t)")
      s"ALTER COLUMN$ifExistsClause ${renderColumnName(columnName)} SET FIELDS $fieldsSql"
    }
  }
  case class AlterColumnField(
    columnName: String,
    field: Column,
    ifExists: Boolean = false
  ) extends AlterTableStatement {
    override def sql: String = {
      val ifExistsClause = if (ifExists) " IF EXISTS" else ""
      s"ALTER COLUMN$ifExistsClause ${renderColumnName(columnName)} SET FIELD ${field.sql}"
    }
  }
  case class DropColumnField(
    columnName: String,
    fieldName: String,
    ifExists: Boolean = false
  ) extends AlterTableStatement {
    override def sql: String = {
      val ifExistsClause = if (ifExists) " IF EXISTS" else ""
      s"ALTER COLUMN$ifExistsClause ${renderColumnName(columnName)} DROP FIELD $fieldName"
    }
  }
  case class AlterTableMapping(optionKey: String, optionValue: Value[_])
      extends AlterTableStatement {
    override def sql: String =
      s"SET MAPPING $optionKey = ${optionValue.ddl}"
  }
  case class DropTableMapping(optionKey: String) extends AlterTableStatement {
    override def sql: String =
      s"DROP MAPPING $optionKey"
  }
  case class AlterTableSetting(optionKey: String, optionValue: Value[_])
      extends AlterTableStatement {
    override def sql: String =
      s"SET SETTING $optionKey = ${optionValue.ddl}"
  }
  case class DropTableSetting(optionKey: String) extends AlterTableStatement {
    override def sql: String =
      s"DROP SETTING $optionKey"
  }
  case class AlterTableAlias(optionKey: String, optionValue: Value[_]) extends AlterTableStatement {
    override def sql: String =
      s"SET ALIAS $optionKey = ${optionValue.ddl}"
  }
  case class DropTableAlias(optionKey: String) extends AlterTableStatement {
    override def sql: String =
      s"DROP ALIAS $optionKey"
  }

  case class DropTable(
    table: String,
    ifExists: Boolean = false,
    cascade: Boolean = false,
    parts: Seq[NamePart] = Nil
  ) extends TableStatement
      with DdlStatement {
    override def sql: String = {
      val ifExistsClause = if (ifExists) "IF EXISTS " else ""
      val cascadeClause = if (cascade) " CASCADE" else ""
      s"DROP TABLE $ifExistsClause${renderName(parts, table)}$cascadeClause"
    }
  }

  case class TruncateTable(table: String, parts: Seq[NamePart] = Nil)
      extends TableStatement
      with DdlStatement {
    override def sql: String = s"TRUNCATE TABLE ${renderName(parts, table)}"
  }

  case class ShowTable(table: String, parts: Seq[NamePart] = Nil)
      extends TableStatement
      with DqlStatement {
    override def sql: String = s"SHOW TABLE ${renderName(parts, table)}"
  }

  case class ShowTables(indices: Seq[String] = Seq.empty) extends TableStatement with DqlStatement {
    override def sql: String = {
      if (indices.nonEmpty) {
        s"SHOW TABLES LIKE ${indices.mkString("'", "', '", "'")}"
      } else "SHOW TABLES"
    }
  }

  case class ShowCreateTable(table: String, parts: Seq[NamePart] = Nil)
      extends TableStatement
      with DqlStatement {
    override def sql: String = s"SHOW CREATE TABLE ${renderName(parts, table)}"
  }

  case class DescribeTable(table: String, parts: Seq[NamePart] = Nil)
      extends TableStatement
      with DqlStatement {
    override def sql: String = s"DESCRIBE TABLE ${renderName(parts, table)}"
  }

  sealed trait WatcherStatement extends Statement

  case class CreateWatcher(
    name: String,
    orReplace: Boolean = false,
    ifNotExists: Boolean = false,
    condition: WatcherCondition, // e.g., WHEN condition
    trigger: WatcherTrigger,
    actions: ListMap[String, WatcherAction],
    input: WatcherInput,
    options: ListMap[String, Value[_]] = ListMap.empty
  ) extends WatcherStatement
      with DdlStatement {

    lazy val watcher: Watcher = Watcher(
      id = name,
      condition = condition,
      trigger = trigger,
      actions = actions,
      input = input,
      throttlePeriod = options.get("throttle_period") match {
        case Some(value) =>
          value match {
            case s: StringValue => TransformTimeInterval(s.value)
            case i: LongValue   => Some(Delay(TransformTimeUnit.Seconds, i.value))
            case _              => None
          }
        case None => None
      },
      metadata = options.get("metadata") match {
        case Some(value) =>
          value match {
            case o: ObjectValue => Some(o)
            case _              => None
          }
        case _ => None
      }
    )

    override def sql: String = watcher.sql
  }

  case class ShowWatcherStatus(name: String, parts: Seq[NamePart] = Nil)
      extends WatcherStatement
      with DqlStatement {
    override def sql: String = s"SHOW WATCHER STATUS ${renderName(parts, name)}"
  }

  case class DropWatcher(name: String, ifExists: Boolean = false, parts: Seq[NamePart] = Nil)
      extends WatcherStatement
      with DdlStatement {
    override def sql: String = {
      val ifExistsClause = if (ifExists) "IF EXISTS " else ""
      s"DROP WATCHER $ifExistsClause${renderName(parts, name)}"
    }
  }

  case object ShowWatchers extends WatcherStatement with DqlStatement {
    override def sql: String = s"SHOW WATCHERS"
  }

  sealed trait EnrichPolicyStatement extends Statement

  case class CreateEnrichPolicy(
    name: String,
    policyType: EnrichPolicyType,
    from: Seq[String],
    on: String,
    enrichFields: List[String],
    where: Option[Where] = None,
    orReplace: Boolean = false,
    ifNotExists: Boolean = false,
    /** See `Table.parts` / story 21.7: `Nil` unless the reference carries a qualifier or quoting.
      */
    parts: Seq[NamePart] = Nil
  ) extends EnrichPolicyStatement
      with DdlStatement {
    override def sql: String = {
      val ineClause = if (ifNotExists) " IF NOT EXISTS" else ""
      val replaceClause = if (orReplace) " OR REPLACE" else ""
      val whereClause = this.where.map(w => s" $w").getOrElse("")
      s"CREATE$replaceClause ENRICH POLICY$ineClause ${renderName(
        parts,
        name
      )} TYPE ${policyType.name.toUpperCase} FROM ${from
        .map(renderColumnName)
        .mkString(",")} ON ${renderColumnName(on)} ENRICH ${enrichFields
        .map(renderColumnName)
        .mkString(", ")}$whereClause"
    }

    lazy val policy: EnrichPolicy = EnrichPolicy(
      name = name,
      policyType = policyType,
      indices = from,
      matchField = on,
      enrichFields = enrichFields,
      criteria = where.flatMap(_.criteria)
    )

    override def validate(): Either[SQL, Unit] = {
      if (from.isEmpty) {
        Left("Source indices cannot be empty")
      } else if (on.isEmpty) {
        Left("Match field cannot be empty")
      } else if (enrichFields.isEmpty) {
        Left("Enrich fields cannot be empty")
      } else {
        // The source WHERE is a document-level filter: an aggregate in it is dropped by
        // ElasticCriteria and the policy would enrich from EVERY source document (match_all).
        where.map(_.validate()).getOrElse(Right(()))
      }
    }
  }

  case class ExecuteEnrichPolicy(name: String, parts: Seq[NamePart] = Nil)
      extends EnrichPolicyStatement
      with DdlStatement {
    override def sql: String = s"EXECUTE ENRICH POLICY ${renderName(parts, name)}"
  }

  case class DropEnrichPolicy(name: String, ifExists: Boolean = false, parts: Seq[NamePart] = Nil)
      extends EnrichPolicyStatement
      with DdlStatement {
    override def sql: String = {
      val ifExistsClause = if (ifExists) "IF EXISTS " else ""
      s"DROP ENRICH POLICY $ifExistsClause${renderName(parts, name)}"
    }
  }

  case class ShowEnrichPolicy(name: String, parts: Seq[NamePart] = Nil)
      extends EnrichPolicyStatement
      with DqlStatement {
    override def sql: String = s"SHOW ENRICH POLICY ${renderName(parts, name)}"
  }

  case object ShowEnrichPolicies extends EnrichPolicyStatement with DqlStatement {
    override def sql: String = s"SHOW ENRICH POLICIES"
  }

  // ========================================================================
  // Cluster statements
  // ========================================================================

  sealed trait ClusterStatement extends Statement

  case object ShowClusterName extends ClusterStatement with DqlStatement {
    override def sql: String = "SHOW CLUSTER NAME"
  }

  // ========================================================================
  // License statements
  // ========================================================================

  sealed trait LicenseStatement extends Statement with DqlStatement

  case object ShowLicense extends LicenseStatement {
    override def sql: String = "SHOW LICENSE"
  }

  case object RefreshLicense extends LicenseStatement {
    override def sql: String = "REFRESH LICENSE"
  }
}
