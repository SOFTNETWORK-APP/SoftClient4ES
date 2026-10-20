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

import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.schema.{Column, Table => SchemaTable}
import app.softnetwork.elastic.sql.`type`.SQLTypes
import app.softnetwork.elastic.sql.toRegex
import app.softnetwork.elastic.sql.function.FunctionUtils

/** Story IDENT-4 -- the population a qualified GROUP BY key is judged over (public, like story
  * IDENT-1's `IdentityPopulation`, so later stories can reuse it).
  *
  * ALPHABET: {unqualified, alias `a` (the MAIN table), alias `b`} on the GROUP BY key and,
  * independently, on the HAVING operand; three FROM shapes {one table, two indices, a self-join};
  * eight operators; and, on the two joined shapes, the grouping on BOTH correlations. Every cell is
  * judged at two resolutions: R1, the one `update()` `Parser.apply` performs (what an index-pattern
  * search, a materialized view and a watcher use), and R2, the second one a venue performs --
  * `update(Some(schema))` for the one-table shape (`SearchApi.resolveWithSchema`), `updateAll` for
  * a JOIN (extensions' `FieldAnalyzer`, CTAS).
  *
  * The expected answer comes from the SQL MEANING alone ([[Cell.addressed]]), never from the code
  * under test: a key spelled identically is the same key; otherwise the predicate addresses the key
  * whose CORRELATION it names, an unqualified name being the main table's (the rule an unqualified
  * column obeys everywhere else); a qualifier the FROM does not declare is no qualifier at all --
  * an object path.
  *
  * The SELECT is metric-only on purpose: no display alias then separates two levels, which is the
  * harshest input for a lookup keyed by name.
  */
object QualifiedKeyPopulation {

  sealed abstract class Shape(val label: String, val from: String, val declared: Set[String])
  case object SingleTable extends Shape("one table", "FROM orders a", Set("a"))
  case object TwoIndices
      extends Shape("two indices", "FROM orders a JOIN depts b ON a.did = b.id", Set("a", "b"))
  case object SelfJoin
      extends Shape("self-join", "FROM orders a JOIN orders b ON a.did = b.id", Set("a", "b"))

  val shapes: Seq[Shape] = Seq(SingleTable, TwoIndices, SelfJoin)

  val qualifiers: Seq[Option[String]] = Seq(None, Some("a"), Some("b"))

  def spell(q: Option[String], column: String): String =
    q.map(alias => s"$alias.$column").getOrElse(column)

  /** The correlation a spelling names in a shape; `None` when it names none (an object path). */
  def correlationOf(shape: Shape, q: Option[String]): Option[String] = q match {
    case None                                 => Some("a")
    case Some(alias) if shape.declared(alias) => Some(alias)
    case Some(_)                              => None
  }

  sealed abstract class Grouping(val label: String, val keys: Seq[Option[String]])
  final case class OneKey(q: Option[String])
      extends Grouping(s"GROUP BY ${spell(q, "category")}", Seq(q))
  case object BothCorrelations
      extends Grouping("GROUP BY a.category, b.category", Seq(Some("a"), Some("b")))

  def groupingsOf(shape: Shape): Seq[Grouping] =
    qualifiers.map(q => OneKey(q): Grouping) ++
    (if (shape == SingleTable) Nil else Seq(BothCorrelations))

  private val empty = BucketIncludesExcludes()

  /** One operator: how it is spelled over the key, and the (kept, removed) channel it must put on
    * the level it addresses -- stated from the operator's meaning.
    */
  final case class Op(
    label: String,
    render: String => String,
    kept: BucketIncludesExcludes,
    removed: BucketIncludesExcludes
  )

  val ops: Seq[Op] = Seq(
    Op("=", k => s"$k = 'x'", BucketIncludesExcludes(values = Set("x")), empty),
    Op("<>", k => s"$k <> 'x'", empty, BucketIncludesExcludes(values = Set("x"))),
    Op("IN", k => s"$k IN ('x', 'y')", BucketIncludesExcludes(values = Set("x", "y")), empty),
    Op(
      "NOT IN",
      k => s"$k NOT IN ('x', 'y')",
      empty,
      BucketIncludesExcludes(values = Set("x", "y"))
    ),
    Op("LIKE", k => s"$k LIKE 'x%'", BucketIncludesExcludes(regex = Some(toRegex("x%"))), empty),
    Op(
      "NOT LIKE",
      k => s"$k NOT LIKE 'x%'",
      empty,
      BucketIncludesExcludes(regex = Some(toRegex("x%")))
    ),
    Op("RLIKE", k => s"$k RLIKE 'x.*'", BucketIncludesExcludes(regex = Some("x.*")), empty),
    Op("MATCH", k => s"MATCH ($k) AGAINST ('x')", BucketIncludesExcludes(regex = Some("x")), empty)
  )

  final case class Cell(shape: Shape, grouping: Grouping, having: Option[String], op: Op) {
    val predicate: String = op.render(spell(having, "category"))

    val label: String = s"${shape.label} | ${grouping.label} | HAVING $predicate"

    val sql: String =
      s"SELECT COUNT(*) AS c, MAX(amount) AS mx ${shape.from} " +
      s"GROUP BY ${grouping.keys.map(spell(_, "category")).mkString(", ")} HAVING $predicate"

    /** The GROUP BY position (0-based) the predicate addresses; `None` when it names no grouping
      * key -- the cell must be refused, and no level may carry the predicate.
      */
    lazy val addressed: Option[Int] = {
      val exact = grouping.keys.indexOf(having)
      if (exact >= 0) Some(exact)
      else
        correlationOf(shape, having).flatMap { leaf =>
          Some(grouping.keys.indexWhere(k => correlationOf(shape, k).contains(leaf))).filter(_ >= 0)
        }
    }

    /** The channel every level must carry, in GROUP BY order. */
    lazy val expectedChannels: Seq[(BucketIncludesExcludes, BucketIncludesExcludes)] =
      grouping.keys.indices.map { i =>
        if (addressed.contains(i)) (op.kept, op.removed) else (empty, empty)
      }
  }

  lazy val cells: Seq[Cell] =
    for {
      shape    <- shapes
      grouping <- groupingsOf(shape)
      having   <- qualifiers
      op       <- ops
    } yield Cell(shape, grouping, having, op)

  /** (3 + 4 + 4) groupings × 3 HAVING spellings × 8 operators -- a FLOOR against a shrunken product
    * (a `groupingsOf` change), not evidence: the population is the product itself.
    */
  val ExpectedCellCount: Int = 11 * 3 * 8

  val ordersSchema: SchemaTable = SchemaTable(
    "orders",
    columns = List(
      Column("id", SQLTypes.Keyword),
      Column("did", SQLTypes.Keyword),
      Column("category", SQLTypes.Keyword),
      Column("s", SQLTypes.Keyword),
      Column("amount", SQLTypes.Int),
      Column("n", SQLTypes.BigInt),
      Column("d", SQLTypes.Date)
    )
  )

  val deptsSchema: SchemaTable = SchemaTable(
    "depts",
    columns = List(Column("id", SQLTypes.Keyword), Column("category", SQLTypes.Keyword))
  )

  /** R1 -- the statement as `Parser.apply` resolved it, validation skipped so that a refused cell's
    * channels stay observable.
    */
  def r1(sql: String): SingleSearch =
    Parser.parseUnvalidated(sql) match {
      case Right(ss: SingleSearch) => ss
      case other => throw new IllegalStateException(s"[$sql] expected a SingleSearch, got $other")
    }

  /** R2 -- the second resolution the venue performs. */
  def r2(shape: Shape, ss: SingleSearch): SingleSearch = shape match {
    case SingleTable => ss.update(Some(ordersSchema))
    case _           => ss.updateAll(Map("orders" -> ordersSchema, "depts" -> deptsSchema))
  }

  /** The function-of-column keys of the registry (story IDENT-1's `IdentityPopulation`), depth 1:
    * (spelling, the key over the qualified column `t.d`, the key over the bare column `d`).
    */
  lazy val functionKeys: Seq[(String, String, String)] =
    IdentityPopulation.exercised.collect {
      case (cell, _) if cell.operandKind == "qualified" && cell.depth == 1 =>
        (
          cell.spelling,
          cell.template.format(cell.spelling, "t.d"),
          cell.template.format(cell.spelling, "d")
        )
    }.distinct

  /** The condition's R1 leaf still holds the PARSED column (`t.d`, no alias) as a function
    * ARGUMENT, so no level owns that named leaf and rule (a) refuses the fully-qualified condition
    * at parse. Asked of the AST, never of a function-name list.
    */
  def staleArgument(ss: SingleSearch): Boolean =
    ss.havingLeaves.headOption.toSeq
      .flatMap(e => FunctionUtils.funIdentifiers(e.identifier).drop(1))
      .exists(id => id.name.contains('.') && id.tableAlias.isEmpty)

  /** Every grouping level, in tree order, by PATH (a self-join's two levels share a name), with the
    * (kept, removed) lists the HAVING puts on it -- asked of the REAL `Criteria.includes` /
    * `excludes`, as both bridges ask them.
    */
  def channelsOf(
    ss: SingleSearch
  ): Seq[(String, BucketIncludesExcludes, BucketIncludesExcludes)] = {
    val criteria = ss.having.flatMap(_.criteria)
    val nodes = ss.bucketTree.allTrees.flatten.foldLeft(Vector.empty[BucketNode]) { (acc, n) =>
      if (acc.exists(_.bucketPath == n.bucketPath)) acc else acc :+ n
    }
    nodes.map { node =>
      (
        node.bucketPath,
        criteria.map(_.includes(node.bucket, not = false, empty)).getOrElse(empty),
        criteria.map(_.excludes(node.bucket, not = false, empty)).getOrElse(empty)
      )
    }
  }
}
