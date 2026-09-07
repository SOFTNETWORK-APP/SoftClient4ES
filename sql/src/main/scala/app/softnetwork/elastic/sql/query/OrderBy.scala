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

import app.softnetwork.elastic.sql.function.{Function, FunctionChain}
import app.softnetwork.elastic.sql.{Alias, Expr, Identifier, TokenRegex, Updateable}

case object OrderBy extends Expr("ORDER BY") with TokenRegex

sealed trait SortOrder extends TokenRegex

case object Desc extends Expr("DESC") with SortOrder

case object Asc extends Expr("ASC") with SortOrder

sealed trait NullOrdering extends TokenRegex

case object NullsFirst extends Expr("NULLS FIRST") with NullOrdering

case object NullsLast extends Expr("NULLS LAST") with NullOrdering

case class FieldSort(
  field: Identifier,
  order: Option[SortOrder],
  nullOrdering: Option[NullOrdering] = None,
  bareTableAlias: Option[String] = None,
  unresolvedOrdinal: Option[UnresolvedOrdinal] = None,
  resolved: Boolean = false,
  substitution: Option[Bucket.Substitution] = None
) extends FunctionChain
    with Updateable {
  lazy val functions: List[Function] = field.functions
  lazy val direction: SortOrder = order.getOrElse(Asc)
  lazy val name: String = field.identifierName

  /** The 1-based SELECT position this sort names, when it is an ordinal `ORDER BY <n>`. Same
    * predicate as `Bucket.ordinal` — ONE definition of "position n" for both clauses.
    *
    * `ORDER BY <n>` has ALWAYS parsed (via `fieldWithFunction`'s
    * `identifierWithArithmeticExpression` alternative); it just never resolved. Before this,
    * `isScriptSort` was true and the clause became a Painless sort over a CONSTANT on the row path,
    * or an unmatched bucket-order key on the GROUP BY path — silently dropped either way (issue
    * #253's family).
    *
    * 🔴 The `resolved` latch mirrors `Bucket.ordinal`'s and for the same reason: a resolved
    * position that named a bare SELECT literal is itself ordinal-SHAPED, so a second `update()`
    * (`Table.mergeWithSearch` re-updates an already-parsed search) would read it back as a
    * position. `SELECT 2 AS c FROM t ORDER BY 1` is the witness.
    */
  lazy val ordinal: Option[Long] = if (resolved) None else Bucket.ordinalOf(field)

  // Render via field.sql (not identifierName) so the table-alias qualifier
  // survives the AST → SQL round-trip — consumers such as the join planner
  // re-parse this output and reject unqualified columns as ambiguous (#158).
  //
  // 🔴 A SUBSTITUTED sort re-emits the SELECT alias, for exactly the reason `Bucket.sql` does: an
  // `ORDER BY <n>` resolved onto a bare numeric literal renders as that literal, which the grammar
  // reads back as a POSITION. MEASURED before this:
  // `SELECT 2 AS flag, SUM(amount) AS s FROM t GROUP BY flag ORDER BY flag ASC` rendered
  // `ORDER BY 2 ASC` and re-parsed as `ORDER BY SUM(amount) ASC` -- a bucket sort silently becoming
  // a METRIC sort -- and `SELECT 2 AS flag FROM t GROUP BY 1 ORDER BY 1 ASC` rendered a statement
  // that no longer parses at all. The render IS re-parsed by consumers (#158's join planner, and
  // `MaterializedViewExtension` re-runs a persisted one), so this is a live corruption, not a
  // cosmetic round-trip failure. `Bucket` was given this protection in the same commit that taught
  // `FieldSort` to substitute; the rule now covers both clauses, with no "except".
  override def sql: String =
    s"${substitution.flatMap(_.alias).getOrElse(field.sql)} $direction${nullOrdering.map(n => s" ${n.sql}").getOrElse("")}"

  override def update(request: SingleSearch): FieldSort =
    ordinal match {
      case Some(position) =>
        // ⚠️ NO `.update(request)` here, and the asymmetry with `Bucket.update` is deliberate.
        // `SingleSearch.update` is a four-step pipeline: `from`, then `select`/`groupBy`/`having`
        // against that, then `where`, then `orderBy` LAST — so by the time this runs
        // `request.select.fields` is the ALREADY-UPDATED SELECT list, whereas `Bucket.update`
        // receives the RAW one and therefore must update what it picks. Updating an
        // already-updated identifier a second time re-runs the dotted-name / table-alias rewrite
        // on a name that no longer carries its qualifier: harmless for a plain column, not
        // obviously so for a nested or unnested field. Take the resolved identifier as it stands.
        Bucket.selectItem(position, request) match {
          case Some(selected) =>
            this.copy(
              field = selected.identifier,
              unresolvedOrdinal = None,
              resolved = true,
              substitution = Some(Bucket.Substitution.of(selected))
            )
          case None =>
            this.copy(
              unresolvedOrdinal = Some(UnresolvedOrdinal(position, request.select.fields.size)),
              resolved = true
            )
        }
      case None =>
        // 🔴 NO alias resolution here, and that is a DELIBERATE deletion, not an omission.
        //
        // An earlier round resolved an ORDER BY that named a bucket by its output name, to keep
        // `SingleSearch.sorts`' key in step with what `buildBuckets` looks up. It was proved
        // REDUNDANT by falsification -- disabling it left all eight ORDER BY shapes emitting the
        // correct terms `order`, because `BucketOrder` in the bridge already falls back to the
        // bucket's own name -- and it was actively HARMFUL: `FieldSort.update` is shared verbatim
        // between the statement's ORDER BY and every window's `OVER (...)` sort (six call sites in
        // `function/aggregate`), with nothing to tell them apart, so it rewrote
        // `ROW_NUMBER() OVER (PARTITION BY country ORDER BY pays)` into `ORDER BY country` whenever
        // the statement happened to carry a GROUP BY. The guard that hid this keyed on a property
        // of the STATEMENT (`groupBy.isDefined`), never of the sort being updated.
        //
        // Keep the user's spelling. The bridge resolves the direction; the render stays a fixed
        // point; and a window's own ORDER BY is left alone.
        this.copy(
          field = field.update(request),
          resolved = true,
          // `ORDER BY e` where `e` aliases a FROM table sorts on nothing (#159). It used to be
          // caught downstream, because `Identifier.update` rewrote any bare name matching an alias
          // to the empty string; that rewrite also broke a column legitimately sharing its table's
          // name, so it is gone and the collision is recorded here, where the FROM aliases are
          // still in scope.
          bareTableAlias =
            if (!field.name.contains('.') && request.tableAliases.exists(_._2 == field.name))
              Some(field.name)
            else
              None
        )
    }

  override def validate(): Either[String, Unit] =
    unresolvedOrdinal match {
      case Some(u) =>
        Left(
          s"ORDER BY position ${u.position} is out of range: the SELECT list has ${u.selectSize} " +
          s"item(s), so positions 1 to ${u.selectSize} are valid"
        )
      case None =>
        bareTableAlias match {
          case Some(alias) => Left(s"Column name expected after table alias '$alias'")
          case None =>
            field.tableAlias match {
              case Some(alias) if field.name.isEmpty || field.name.endsWith(".") =>
                Left(s"Column name expected after table alias '$alias'")
              case _ => super.validate()
            }
        }
    }
  def isScriptSort: Boolean = functions.nonEmpty && !hasAggregation && field.fieldAlias.isEmpty

  def isBucketScript: Boolean = functions.nonEmpty && !isAggregation && hasAggregation

  def extractAggregationFields: Seq[Field] =
    if (field.aggregations.nonEmpty)
      field.metricName.map(name => Field(field, Some(Alias(name)))).toSeq
    else
      Seq.empty
}

case class OrderBy(sorts: Seq[FieldSort]) extends Updateable {
  override def sql: String = s" $OrderBy ${sorts.mkString(", ")}"

  override def validate(): Either[String, Unit] =
    for {
      _ <-
        if (sorts.isEmpty)
          Left("At least one sort field is required")
        else
          sorts.map(_.validate()).filter(_.isLeft) match {
            case Nil    => Right(())
            case errors => Left(errors.map { case Left(err) => err }.mkString("\n"))
          }
    } yield ()

  def update(request: SingleSearch): OrderBy =
    this.copy(sorts = sorts.map(_.update(request)))
}
