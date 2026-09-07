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

import app.softnetwork.elastic.sql.`type`.{SQLType, SQLTypes}
import app.softnetwork.elastic.sql.schema.{
  sqlConfig,
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
import app.softnetwork.elastic.sql.function.FunctionUtils
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
    explodeNested: Boolean = true
  ) extends SearchStatement {
    override def sql: String =
      s"$select$from${asString(where)}${asString(groupBy)}${asString(having)}${asString(orderBy)}${asString(limit)}${asString(onConflict)}"

    override def withoutNestedExplosion: SingleSearch = this.copy(explodeNested = false)

    lazy val fieldAliases: ListMap[String, String] = select.fieldAliases
    lazy val tableAliases: ListMap[String, String] = from.tableAliases
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
      // No de-duplication against other SELECT items: the constants SEED `parseAggregations`'
      // `parentContext`, so anything Elasticsearch computes is merged on the RIGHT of `++` and
      // therefore WINS -- including a computed null, which a "never overwrite a non-null value"
      // guard could not express. Precedence is structural, not a rule to remember.
      ListMap(
        select.fieldsWithComputedAliases
          .filter(f => SingleSearch.isRowInvariantLiteral(f.identifier))
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
      */
    lazy val returnsRows: Boolean = windowRowQuery || (sqlAggregations.isEmpty && groupBy.isEmpty)

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

    override def validate(): Either[String, Unit] = {
      for {
        _ <- from.validate()
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
              .filterNot(f =>
                buckets.exists(b => b.name == f.fieldAlias.map(_.alias).getOrElse(f.sourceField))
              )
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

    /** A bare ROW-INVARIANT literal: an empty name carrying exactly one scalar constant `Value` --
      * the shape the parser builds for `2 AS COL2` (`GenericIdentifier("", List(LongValue(2)))`).
      * Such an item is the same for every document in a group.
      *
      * TWO production consumers, and they are two halves of one feature (issue #253, FOLD-IN 1 --
      * "a constant is legal beside a GROUP BY"):
      *   - `SingleSearch.validate()` excuses such a field from the non-aggregated-field check, so
      *     `SELECT category, 2 AS flag FROM t GROUP BY category` parses;
      *   - `SingleSearch.rowInvariantProjection` carries its VALUE, which `SearchApi` merges into
      *     each aggregation row -- without that the column parses and then comes back NULL, because
      *     an aggregation response has no hits and the `script_fields` entry a constant is emitted
      *     as is never fetched under `"size": 0`.
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

  case class MultiSearch(requests: Seq[SingleSearch], explodeNested: Boolean = true)
      extends SearchStatement {
    override def sql: String = s"${requests.map(_.sql).mkString(" UNION ALL ")}"

    override def withoutNestedExplosion: MultiSearch = {
      this.copy(explodeNested = false, requests = requests.map(_.withoutNestedExplosion))
    }

    def update(): MultiSearch = this.copy(requests = requests.map(_.update()))

    override def validate(): Either[String, Unit] = {
      requests.map(_.validate()).filter(_.isLeft) match {
        case Nil    => Right(()) // TODO validate that all requests have the same fields
        case errors => Left(errors.map { case Left(err) => err }.mkString("\n"))
      }
    }

    lazy val sqlAggregations: ListMap[String, SQLAggregation] =
      ListMap(requests.flatMap(_.sqlAggregations).distinct: _*)

    lazy val fieldAliases: ListMap[String, String] =
      ListMap(requests.flatMap(_.fieldAliases).distinct: _*)
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
          case Some(t) => if (t.isEmpty) " () " else s" (${t.mkString(", ")}) "
          case None    => " "
        }
      val actionSql = if (doUpdate) "DO UPDATE" else "DO NOTHING"
      s" ON CONFLICT$targetSql$actionSql"
    }
  }

  case class Insert(
    table: String,
    cols: Seq[String],
    values: Either[SearchStatement, Seq[Seq[Value[_]]]],
    onConflict: Option[OnConflict] = None
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
      values match {
        case Left(query) if cols.isEmpty =>
          s"INSERT INTO $table ${query.sql}${asString(onConflict)}"
        case Left(query) =>
          s"INSERT INTO $table (${cols.mkString(",")}) ${query.sql}${asString(onConflict)}"
        case Right(rows) =>
          val valuesSql = rows
            .map(rowToSql)
            .mkString(", ")
          s"INSERT INTO $table (${cols.mkString(",")}) VALUES $valuesSql${asString(onConflict)}"
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

  case class Update(table: String, values: ListMap[String, PainlessScript], where: Option[Where])
      extends DmlStatement {
    // Uses `value.sql` (not `value.value`) for the literal branch so the rendered string is
    // re-parseable into the same AST. `value.value` loses the syntactic shape of string
    // literals (`'USA'` becomes `USA`), and any downstream consumer that re-parses this output
    // — e.g. GatewayApi.run → api.updateByQuery(table, update.sql) before it was changed to
    // accept the AST directly — would mis-parse the bare token as an Identifier and silently
    // null the column via a broken painless script.
    override def sql: String = s"UPDATE $table SET ${values
      .map { case (k, v) =>
        v match {
          case value: Value[_] => s"$k = ${value.sql}"
          case painlessScript  => s"$k = ${painlessScript.sql}"
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
    override def validate(): Either[String, Unit] = where.map(_.validate()).getOrElse(Right(()))
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
    onConflict: Option[OnConflict] = None
  ) extends DmlStatement {
    override def sql: String = {
      // The grammar only accepts a quoted source literal, so render one — an unquoted path
      // (`FROM /tmp/data.json`) is not valid SQL and cannot survive a re-parse.
      val quoted = s"'${source.replace("\\", "\\\\").replace("'", "\\'")}'"
      s"COPY INTO $targetTable FROM $quoted${asString(fileFormat)}${asString(onConflict)}"
    }
  }

  sealed trait DdlStatement extends Statement

  sealed trait PipelineStatement extends Statement

  case class CreatePipeline(
    name: String,
    pipelineType: IngestPipelineType,
    ifNotExists: Boolean = false,
    orReplace: Boolean = false,
    processors: Seq[IngestProcessor]
  ) extends PipelineStatement
      with DdlStatement {
    override def sql: String = {
      val processorsDdl = processors.map(_.ddl).mkString(", ")
      val replaceClause = if (orReplace) " OR REPLACE" else ""
      val ineClause = if (!orReplace && ifNotExists) " IF NOT EXISTS" else ""
      s"CREATE$replaceClause PIPELINE$ineClause $name WITH PROCESSORS ($processorsDdl)"
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
    override def sql: String = s"DROP PROCESSOR ${processorType.name.toUpperCase}($column)"
  }
  case class AlterPipelineProcessor(processor: IngestProcessor) extends AlterPipelineStatement {
    override def sql: String = s"ALTER PROCESSOR ${processor.ddl}"
    override def ddlProcessor: Option[IngestProcessor] = Some(processor)
  }

  case class AlterPipeline(
    name: String,
    ifExists: Boolean,
    statements: List[AlterPipelineStatement]
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
      s"ALTER PIPELINE$ifExistsClause $name $statementsSql"
    }

    lazy val ddlProcessors: Seq[IngestProcessor] = statements.flatMap(_.ddlProcessor)

    lazy val pipeline: IngestPipeline =
      IngestPipeline(
        s"alter-pipeline-$name-${Instant.now}",
        IngestPipelineType.Custom,
        ddlProcessors
      )
  }

  case class DropPipeline(name: String, ifExists: Boolean = false)
      extends PipelineStatement
      with DdlStatement {
    override def sql: String = {
      val ifExistsClause = if (ifExists) "IF EXISTS " else ""
      s"DROP PIPELINE $ifExistsClause$name"
    }
  }

  case class ShowPipeline(name: String) extends PipelineStatement with DqlStatement {
    override def sql: String = s"SHOW PIPELINE $name"
  }

  case class ShowCreatePipeline(name: String) extends PipelineStatement with DqlStatement {
    override def sql: String = s"SHOW CREATE PIPELINE $name"
  }

  case class DescribePipeline(name: String) extends PipelineStatement with DqlStatement {
    override def sql: String = s"DESCRIBE PIPELINE $name"
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
    options: ListMap[String, Value[_]] = ListMap.empty
  ) extends MaterializedViewStatement
      with DdlStatement {
    override def sql: String = {
      val frequencySql = frequency match {
        case Some(freq) => freq.sql
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
      s"CREATE$replaceClause MATERIALIZED VIEW$ineClause $view$frequencySql$optionsSql AS ${dql.sql}"
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

  case class DropMaterializedView(name: String, ifExists: Boolean = false)
      extends MaterializedViewStatement
      with DdlStatement {
    override def sql: String = {
      val ifExistsClause = if (ifExists) "IF EXISTS " else ""
      s"DROP MATERIALIZED VIEW $ifExistsClause$name"
    }
  }

  case class RefreshMaterializedView(name: String, ifExists: Boolean, scheduleNow: Boolean)
      extends MaterializedViewStatement
      with DdlStatement {
    override def sql: String = {
      val ifExistsClause = if (ifExists) "IF EXISTS " else ""
      val scheduleNowClause = if (scheduleNow) " WITH SCHEDULE NOW" else ""
      s"REFRESH MATERIALIZED VIEW $ifExistsClause$name$scheduleNowClause"
    }
  }

  case object ShowMaterializedViews extends MaterializedViewStatement with DqlStatement {
    override def sql: String = s"SHOW MATERIALIZED VIEWS"
  }

  case class ShowMaterializedView(name: String)
      extends MaterializedViewStatement
      with DqlStatement {
    override def sql: String = s"SHOW MATERIALIZED VIEW $name"
  }

  case class ShowMaterializedViewStatus(name: String)
      extends MaterializedViewStatement
      with DqlStatement {
    override def sql: String = s"SHOW MATERIALIZED VIEW STATUS $name"
  }

  case class ShowCreateMaterializedView(name: String)
      extends MaterializedViewStatement
      with DqlStatement {
    override def sql: String = s"SHOW CREATE MATERIALIZED VIEW $name"
  }

  case class DescribeMaterializedView(name: String)
      extends MaterializedViewStatement
      with DqlStatement {
    override def sql: String = s"DESCRIBE MATERIALIZED VIEW $name"
  }

  case class CreateTable(
    table: String,
    ddl: Either[SearchStatement, List[Column]],
    ifNotExists: Boolean = false,
    orReplace: Boolean = false,
    primaryKey: List[String] = Nil,
    partitionBy: Option[PartitionDate] = None,
    options: ListMap[String, Value[_]] = ListMap.empty
  ) extends TableStatement
      with DdlStatement {

    lazy val partitioned: Boolean = partitionBy.isDefined

    override def sql: String = {
      val replaceClause = if (orReplace) " OR REPLACE" else ""
      val ineClause = if (!orReplace && ifNotExists) " IF NOT EXISTS" else ""
      ddl match {
        case Left(select) =>
          s"CREATE$replaceClause TABLE$ineClause $table AS ${select.sql}"
        case Right(columns) =>
          val colsSql = columns.map(_.sql).mkString(",\n ")
          val primaryKeyClause =
            if (primaryKey.nonEmpty)
              s",\n PRIMARY KEY (${primaryKey.mkString(", ")})"
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
          s"CREATE$replaceClause TABLE$ineClause $table (\n $colsSql$primaryKeyClause\n)$partitionClause$optionsClause"
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

  case class AlterTable(table: String, ifExists: Boolean, statements: List[AlterTableStatement])
      extends TableStatement
      with DdlStatement {
    override def sql: String = {
      val ifExistsClause = if (ifExists) " IF EXISTS" else ""
      val parenthesesNeeded = statements.size > 1
      val statementsSql = if (parenthesesNeeded) {
        statements.map(_.sql).mkString("(\n\t", ",\n\t", "\n)")
      } else {
        statements.map(_.sql).mkString("")
      }
      s"ALTER TABLE$ifExistsClause $table $statementsSql"
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
      s"DROP COLUMN$ifExistsClause $columnName"
    }
    override def ddlProcessor: Option[IngestProcessor] = Some(
      RemoveProcessor(column = columnName)
    )
  }
  case class RenameColumn(oldName: String, newName: String) extends AlterTableStatement {
    override def sql: String = s"RENAME COLUMN $oldName TO $newName"
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
      s"ALTER COLUMN$ifExistsClause $columnName SET OPTIONS (${options
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
      s"ALTER COLUMN$ifExistsClause $columnName SET OPTION ($optionKey = $optionValue)"
    }
  }
  case class DropColumnOption(
    columnName: String,
    optionKey: String,
    ifExists: Boolean = false
  ) extends AlterTableStatement {
    override def sql: String = {
      val ifExistsClause = if (ifExists) " IF EXISTS" else ""
      s"ALTER COLUMN$ifExistsClause $columnName DROP OPTION $optionKey"
    }
  }
  case class AlterColumnType(columnName: String, newType: SQLType, ifExists: Boolean = false)
      extends AlterTableStatement {
    override def sql: String = {
      val ifExistsClause = if (ifExists) " IF EXISTS" else ""
      s"ALTER COLUMN$ifExistsClause $columnName SET DATA TYPE $newType"
    }
  }
  case class AlterColumnScript(
    columnName: String,
    newScript: ScriptProcessor,
    ifExists: Boolean = false
  ) extends AlterTableStatement {
    override def sql: String = {
      val ifExistsClause = if (ifExists) " IF EXISTS" else ""
      s"ALTER COLUMN$ifExistsClause $columnName SET SCRIPT AS (${newScript.script})"
    }
  }
  case class DropColumnScript(columnName: String, ifExists: Boolean = false)
      extends AlterTableStatement {
    override def sql: String = {
      val ifExistsClause = if (ifExists) " IF EXISTS" else ""
      s"ALTER COLUMN$ifExistsClause $columnName DROP SCRIPT"
    }
  }
  case class AlterColumnDefault(
    columnName: String,
    defaultValue: Value[_],
    ifExists: Boolean = false
  ) extends AlterTableStatement {
    override def sql: String = {
      val ifExistsClause = if (ifExists) " IF EXISTS" else ""
      s"ALTER COLUMN$ifExistsClause $columnName SET DEFAULT $defaultValue"
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
      s"ALTER COLUMN$ifExistsClause $columnName DROP DEFAULT"
    }
  }
  case class AlterColumnComment(
    columnName: String,
    comment: String,
    ifExists: Boolean = false
  ) extends AlterTableStatement {
    override def sql: String = {
      val ifExistsClause = if (ifExists) " IF EXISTS" else ""
      s"ALTER COLUMN$ifExistsClause $columnName SET COMMENT '${escapeStringLiteral(comment)}'"
    }
  }
  case class DropColumnComment(columnName: String, ifExists: Boolean = false)
      extends AlterTableStatement {
    override def sql: String = {
      val ifExistsClause = if (ifExists) " IF EXISTS" else ""
      s"ALTER COLUMN$ifExistsClause $columnName DROP COMMENT"
    }
  }
  case class AlterColumnNotNull(columnName: String, ifExists: Boolean = false)
      extends AlterTableStatement {
    override def sql: String = {
      val ifExistsClause = if (ifExists) " IF EXISTS" else ""
      s"ALTER COLUMN$ifExistsClause $columnName SET NOT NULL"
    }
  }
  case class DropColumnNotNull(columnName: String, ifExists: Boolean = false)
      extends AlterTableStatement {
    override def sql: String = {
      val ifExistsClause = if (ifExists) " IF EXISTS" else ""
      s"ALTER COLUMN$ifExistsClause $columnName DROP NOT NULL"
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
      s"ALTER COLUMN$ifExistsClause $columnName SET FIELDS $fieldsSql"
    }
  }
  case class AlterColumnField(
    columnName: String,
    field: Column,
    ifExists: Boolean = false
  ) extends AlterTableStatement {
    override def sql: String = {
      val ifExistsClause = if (ifExists) " IF EXISTS" else ""
      s"ALTER COLUMN$ifExistsClause $columnName SET FIELD ${field.sql}"
    }
  }
  case class DropColumnField(
    columnName: String,
    fieldName: String,
    ifExists: Boolean = false
  ) extends AlterTableStatement {
    override def sql: String = {
      val ifExistsClause = if (ifExists) " IF EXISTS" else ""
      s"ALTER COLUMN$ifExistsClause $columnName DROP FIELD $fieldName"
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

  case class DropTable(table: String, ifExists: Boolean = false, cascade: Boolean = false)
      extends TableStatement
      with DdlStatement {
    override def sql: String = {
      val ifExistsClause = if (ifExists) "IF EXISTS " else ""
      val cascadeClause = if (cascade) " CASCADE" else ""
      s"DROP TABLE $ifExistsClause$table$cascadeClause"
    }
  }

  case class TruncateTable(table: String) extends TableStatement with DdlStatement {
    override def sql: String = s"TRUNCATE TABLE $table"
  }

  case class ShowTable(table: String) extends TableStatement with DqlStatement {
    override def sql: String = s"SHOW TABLE $table"
  }

  case class ShowTables(indices: Seq[String] = Seq.empty) extends TableStatement with DqlStatement {
    override def sql: String = {
      if (indices.nonEmpty) {
        s"SHOW TABLES LIKE ${indices.mkString("'", "', '", "'")}"
      } else "SHOW TABLES"
    }
  }

  case class ShowCreateTable(table: String) extends TableStatement with DqlStatement {
    override def sql: String = s"SHOW CREATE TABLE $table"
  }

  case class DescribeTable(table: String) extends TableStatement with DqlStatement {
    override def sql: String = s"DESCRIBE TABLE $table"
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

  case class ShowWatcherStatus(name: String) extends WatcherStatement with DqlStatement {
    override def sql: String = s"SHOW WATCHER STATUS $name"
  }

  case class DropWatcher(name: String, ifExists: Boolean = false)
      extends WatcherStatement
      with DdlStatement {
    override def sql: String = {
      val ifExistsClause = if (ifExists) "IF EXISTS " else ""
      s"DROP WATCHER $ifExistsClause$name"
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
    ifNotExists: Boolean = false
  ) extends EnrichPolicyStatement
      with DdlStatement {
    override def sql: String = {
      val ineClause = if (ifNotExists) " IF NOT EXISTS" else ""
      val replaceClause = if (orReplace) " OR REPLACE" else ""
      val whereClause = this.where.map(w => s" $w").getOrElse("")
      s"CREATE$replaceClause ENRICH POLICY$ineClause $name TYPE ${policyType.name.toUpperCase} FROM ${from
        .mkString(",")} ON $on ENRICH ${enrichFields
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

  case class ExecuteEnrichPolicy(name: String) extends EnrichPolicyStatement with DdlStatement {
    override def sql: String = s"EXECUTE ENRICH POLICY $name"
  }

  case class DropEnrichPolicy(name: String, ifExists: Boolean = false)
      extends EnrichPolicyStatement
      with DdlStatement {
    override def sql: String = {
      val ifExistsClause = if (ifExists) "IF EXISTS " else ""
      s"DROP ENRICH POLICY $ifExistsClause$name"
    }
  }

  case class ShowEnrichPolicy(name: String) extends EnrichPolicyStatement with DqlStatement {
    override def sql: String = s"SHOW ENRICH POLICY $name"
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
