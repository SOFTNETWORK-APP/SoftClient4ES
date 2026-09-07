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

import app.softnetwork.elastic.sql.`type`.SQLType
import app.softnetwork.elastic.sql.operator._
import app.softnetwork.elastic.sql.{
  quoteIdentifier,
  Expr,
  GenericIdentifier,
  Identifier,
  LongValue,
  PainlessContext,
  PainlessScript,
  ParamValue,
  TokenRegex,
  Updateable
}

case object GroupBy extends Expr("GROUP BY") with TokenRegex

case class GroupBy(buckets: Seq[Bucket]) extends Updateable {
  override def sql: String = s" $GroupBy ${buckets.mkString(", ")}"
  def update(request: SingleSearch): GroupBy =
    this.copy(buckets = buckets.map(_.update(request)))
  lazy val bucketNames: Map[String, Bucket] = buckets.map { b =>
    b.identifier.identifierName -> b
  }.toMap

  override def validate(): Either[String, Unit] = {
    if (buckets.isEmpty) {
      Left("At least one bucket is required in GROUP BY clause")
    } else {
      buckets.map(_.validate()).filter(_.isLeft) match {
        case Nil    => Right(())
        case errors => Left(errors.map { case Left(err) => err }.mkString("\n"))
      }
    }
  }

  def nestedElements: Seq[NestedElement] =
    buckets.flatMap(_.nestedElement).distinct
}

/** An ANSI position (`GROUP BY <n>` / `ORDER BY <n>`) that named no SELECT item, carried from
  * `update()` to `validate()` so the rejection reaches `Parser.apply` as a `Left` produced by the
  * VALIDATOR instead of an exception caught by its `NonFatal` boundary (issue #250's family:
  * `Parser.single` builds the statement inside its own combinator action, so anything thrown there
  * escapes the grammar entirely). Mirrors the `FieldSort.bareTableAlias` precedent.
  */
case class UnresolvedOrdinal(position: Long, selectSize: Int)

object Bucket {

  /** SQL `GROUP BY` with no `LIMIT` means every group, but an Elasticsearch `terms` aggregation
    * without an explicit `size` silently returns only its default 10 buckets (issue #205). Emit
    * Elasticsearch's own `search.max_buckets` ceiling (65536) instead: cardinalities beyond it fail
    * loudly server-side rather than truncate silently.
    */
  val DefaultSize: Int = 65536

  /** The 1-based SELECT position an identifier names, when it is an ANSI ordinal at all.
    *
    * The ordinal shape is EXACTLY what the implicit `functionAsIdentifier` builds from a bare
    * integer literal: an empty name carrying a single `LongValue`. Measured 2026-09-07 -- every
    * other numeric-looking sort or bucket wraps differently and is NOT an ordinal: `a + 1` / `1 +
    * 1` / `2 * 1` give `List(ArithmeticExpression)`, `ABS(a)` gives
    * `List(MathematicalFunctionWithOp)`, `SUBSTRING(a, 1, 3)` gives `List(Substring)`. A QUOTED
    * digit (`` `1` `` / `"1"`, story 21.1) has `name = "1"` and NO functions, so it is a column --
    * that escape hatch only works because ordinal-ness is decided here on the AST.
    *
    * ONE definition, THREE call sites (`Bucket.update`, `SingleSearch.bucketNames`,
    * `FieldSort.update`), so `GROUP BY <n>` and `ORDER BY <n>` can never disagree about what
    * "position n" means. NEVER re-derive it from a rendered name: that re-derivation, a
    * `"\d+".r.findFirstIn` over `identifierName`, is what crashed `GROUP BY city2` with
    * `IndexOutOfBoundsException` and silently re-pointed `GROUP BY status2`.
    */
  def ordinalOf(identifier: Identifier): Option[Long] =
    if (identifier.name.isEmpty) identifier.functions match {
      case (pos: LongValue) :: Nil => Some(pos.value)
      case _                       => None
    }
    else None

  /** The SELECT item an ordinal position names (1-based), or `None` when the position is out of
    * range -- including 0 and negatives, which name nothing.
    */
  def selectItem(position: Long, request: SingleSearch): Option[Field] =
    if (position >= 1 && position <= request.select.fields.size)
      Some(request.select.fields((position - 1).toInt))
    else
      None

  /** How a bucket that was SUBSTITUTED -- an ordinal `GROUP BY <n>` or a `GROUP BY <select alias>`
    * replaced by the SELECT item it names -- must re-emit itself.
    *
    * 🔴 The render has to RE-PARSE to the same bucket, and after substitution the identifier's own
    * render often does not: `GROUP BY COL2` naming `2 AS COL2` renders the bare `2`, which the
    * grammar reads back as POSITION 2 (measured: it became `GROUP BY SUM(amount)`, a different
    * aggregation). `2.5 AS d` and `2 + 0 AS c` render to something that does not parse in GROUP BY
    * position at all. `MaterializedViewExtension` persists this render and re-runs it via
    * `client.run(alter.sql)`, so it is a live corruption, not a cosmetic round-trip failure.
    *
    * The rule is therefore stated once, with no "except": **a substituted bucket re-emits the ALIAS
    * it was resolved through**, which `Bucket.aliasItem` reads back to the same SELECT item by
    * construction. With no alias the substituted bucket falls back to the identifier's own render,
    * which is correct for a plain column (`SELECT country FROM t GROUP BY 1` renders `GROUP BY
    * country`, the established house behaviour) and is rejected by [[Bucket.validate]] when the
    * identifier has no name of its own to render.
    *
    * `alias` comes from the RAW `Field` and carries its own quoting, so a quoted or reserved-word
    * alias re-emits quoted (`GROUP BY "COL 2"`, not `GROUP BY COL 2`). It is deliberately NOT
    * `identifier.fieldAlias`, which after `update` also carries the `__cN` aliases
    * `Select.fieldsWithComputedAliases` invents -- those resolve through nothing.
    */
  case class Substitution(alias: Option[String])

  object Substitution {
    def of(field: Field): Substitution =
      Substitution(field.fieldAlias.map(a => if (a.quoted) quoteIdentifier(a.alias) else a.alias))
  }

  /** The SELECT item a bare-name bucket references through its ALIAS, when it does -- `GROUP BY
    * pays` where the SELECT list carries `country AS pays`. Resolution rules, in order:
    *   - only a BARE identifier is eligible (no functions, no dot): an expression or a qualified
    *     bucket is never an alias reference;
    *   - a name matching a PROJECTED column's own name wins as the COLUMN, so `GROUP BY country`
    *     never re-routes through an alias that happens to be spelled `country`;
    *   - otherwise a name equal to some SELECT field's alias resolves to that field's IDENTIFIER
    *     (AST-level, never a rendered name, and never through `Select.aliasesToMap`, which is built
    *     over `fieldsWithComputedAliases` and invents `__cN` aliases). Comparison is VERBATIM
    *     (case-sensitive), consistent with Elasticsearch field naming.
    *
    * Without this, the bucket became `terms { field: "<alias>" }` on a field that does not exist
    * and Elasticsearch answered ZERO buckets with HTTP 200 -- `SingleSearch.validate()` passed
    * because the bucket's `name` coincidentally equalled the field's alias.
    */
  def aliasItem(identifier: Identifier, request: SingleSearch): Option[Field] =
    if (
      identifier.functions.isEmpty && identifier.name.nonEmpty && !identifier.name.contains('.')
    ) {
      val fields = request.select.fields
      if (fields.exists(_.identifier.identifierName == identifier.name)) None
      else fields.find(_.fieldAlias.exists(_.alias == identifier.name))
    } else None
}

case class Bucket(
  identifier: Identifier,
  size: Option[Int] = None,
  unresolvedOrdinal: Option[UnresolvedOrdinal] = None,
  resolved: Boolean = false,
  substitution: Option[Bucket.Substitution] = None
) extends Updateable
    with PainlessScript {
  def tableAlias: Option[String] = identifier.tableAlias
  def table: Option[String] = identifier.table

  /** A SUBSTITUTED bucket re-emits the alias it was resolved through; every other bucket renders as
    * its identifier. See [[Bucket.Substitution]] for why, and for the measured corruption that
    * forced it.
    */
  override def sql: String = substitution.flatMap(_.alias).getOrElse(s"$identifier")

  /** The 1-based SELECT position this bucket names, when it is an ordinal `GROUP BY <n>` at all --
    * decided by [[Bucket.ordinalOf]] on the AST, never by a regex on the rendered name.
    *
    * 🔴 The `resolved` latch is what makes resolution IDEMPOTENT, and it is load-bearing: a bare
    * literal in the SELECT list has EXACTLY the ordinal's AST shape (`GROUP BY 2` resolving to `2
    * AS COL2` yields a bucket whose identifier is itself `("", List(LongValue(2)))`), so a second
    * `update()` would read the substituted literal back as position 2 and silently re-point the
    * bucket at a different SELECT item. `SingleSearch.update()` really is called twice on the same
    * statement -- `Table.mergeWithSearch` (`schema/package.scala`) re-updates an already-parsed
    * search with a schema, and so does the search path in `core`. Measured: without the latch,
    * `SELECT 2 AS COL2, SUM(amount) AS s FROM t GROUP BY COL2` re-resolved onto `SUM(amount)`.
    */
  lazy val ordinal: Option[Long] = if (resolved) None else Bucket.ordinalOf(identifier)

  def update(request: SingleSearch): Bucket = {
    val bucketSize = request.limit.map(_.limit).orElse(Some(Bucket.DefaultSize))
    ordinal match {
      case Some(position) =>
        Bucket.selectItem(position, request) match {
          case Some(selected) =>
            // `.update(request)` is required, not decorative: `SingleSearch.update` computes
            // `select.update(from)` and `groupBy.map(_.update(from))` against the SAME `from`, so
            // the SELECT item handed back here is the RAW one. Without this call a qualified item
            // (`t.category`) bucketed on the literal name "t.category" and the statement was then
            // rejected by the non-aggregated-field check with an unrelated message.
            this.copy(
              identifier = selected.identifier.update(request),
              size = bucketSize,
              resolved = true,
              substitution = Some(Bucket.Substitution.of(selected))
            )
          case None =>
            // Recorded, never thrown: `Parser.apply` must stay total, and a `Left` from
            // `validate()` carries a message naming the position instead of an index crash.
            this.copy(
              size = bucketSize,
              unresolvedOrdinal = Some(UnresolvedOrdinal(position, request.select.fields.size)),
              resolved = true
            )
        }
      case None =>
        // The alias arm needs no latch: once resolved, the identifier either names a projected
        // column (which `aliasItem`'s first rule hands back as a column) or carries functions
        // (which makes it ineligible), so re-running it is a no-op.
        Bucket.aliasItem(identifier, request) match {
          case Some(aliased) =>
            this.copy(
              identifier = aliased.identifier.update(request),
              size = bucketSize,
              resolved = true,
              substitution = Some(Bucket.Substitution.of(aliased))
            )
          case None =>
            this.copy(identifier = identifier.update(request), size = bucketSize, resolved = true)
        }
    }
  }

  override def validate(): Either[String, Unit] =
    unresolvedOrdinal match {
      case Some(u) =>
        Left(
          s"GROUP BY position ${u.position} is out of range: the SELECT list has ${u.selectSize} " +
          s"item(s), so positions 1 to ${u.selectSize} are valid"
        )
      case None if identifier.functions == List(ParamValue) =>
        // A bucket over an unbound `?` is scripted as `params.paramValue`, and nothing binds that
        // key -- Painless reads a missing param as null, so Elasticsearch answers ZERO groups with
        // HTTP 200. Scripting every nameless bucket is the right rule (it is what a terms
        // aggregation needs), but this one shape has no value to script, so reject it rather than
        // return a silent empty result. Consistent with story 20.9's ruling on a nested `?`.
        Left(
          "GROUP BY on a query parameter (?) is not supported: the parameter is never bound, so " +
          "the grouping would silently match nothing"
        )
      case None if substitution.exists(_.alias.isEmpty) && identifier.name.isEmpty =>
        // A SUBSTITUTED bucket whose resolved identifier has no field name of its own, and no
        // alias either, cannot be re-emitted: `SELECT 3 FROM t GROUP BY 1` would render
        // `GROUP BY 3` and re-parse as position 3, `SELECT 2.5 AS d ... GROUP BY 1` (unaliased)
        // would render something the grammar rejects outright. A bucket that was NOT substituted
        // is untouched -- `SELECT UPPER(country) FROM t GROUP BY UPPER(country)` renders itself and
        // re-parses -- and a substituted PLAIN COLUMN has a name to render, so
        // `SELECT country FROM t GROUP BY 1` still renders `GROUP BY country`.
        Left(
          s"GROUP BY on the expression ${identifier.sql} requires a SELECT alias to name it: " +
          s"write `SELECT ${identifier.sql} AS <name> ... GROUP BY <name>`"
        )
      case None => Right(())
    }

  lazy val sourceBucket: String =
    if (identifier.nested) {
      tableAlias
        .map(a => s"$a.")
        .getOrElse("") + identifier.name.split("\\.").tail.mkString(".")
    } else {
      identifier.name
    }
  lazy val nested: Boolean = nestedElement.isDefined
  lazy val nestedElement: Option[NestedElement] = identifier.nestedElement
  lazy val nestedBucket: Option[String] =
    identifier.nestedElement.map(_.innerHitsName)

  lazy val name: String = identifier.fieldAlias.getOrElse(path)

  lazy val path: String = sourceBucket

  lazy val nestedPath: String = {
    identifier.nestedElement match {
      case Some(ne) => ne.nestedPath
      case None     => "" // Root level
    }
  }

  override def out: SQLType = identifier.out

  /** A bucket with NO FIELD NAME must be scripted, or nothing describes it.
    *
    * An Elasticsearch `terms` aggregation must specify a `field` or a `script`. A bucket resolved
    * onto an expression or a constant has an EMPTY `identifier.name`, so there is no field to give;
    * if nothing in its function chain reports `shouldBeScripted` either, the emitted aggregation
    * carries NEITHER and Elasticsearch rejects the request outright. MEASURED before this line:
    * `SELECT SUM(1) AS COL, 2 AS COL2 FROM t GROUP BY 2` emitted `"terms": {"size": 65536,
    * "min_doc_count": 1}`, and so did `GROUP BY p` over `? AS p` and `GROUP BY r` over `RANDOM AS
    * r`.
    *
    * 🔴 The condition is `identifier.name.isEmpty`, NOT "the identifier is a row-invariant
    * literal". Every nameless `Value` hits the same hole -- `ParamValue` (a JDBC
    * `PreparedStatement` parameter, aliased and grouped, is the realistic route), `RandomValue`,
    * `IdValue`, `IngestTimestampValue`, `Values` -- because `Value` inherits `shouldBeScripted =
    * false`. A rule justified by correctness applies to every shape that triggers it, with no
    * "except" (`feedback_constant_uniform_justification`); scoping it to the row-invariant subset
    * left a fieldless, scriptless `terms` for all the others.
    *
    * Deliberately scoped to the BUCKET: `Value.shouldBeScripted` stays `false`, because that flag
    * also drives SELECT-list script fields and sorts, where a bare literal is handled elsewhere.
    */
  override def shouldBeScripted: Boolean =
    identifier.shouldBeScripted || identifier.name.isEmpty

  override def hasAggregation: Boolean = identifier.hasAggregation

  def isBucketScript: Boolean = !identifier.isAggregation && hasAggregation

  /** Generate painless script for this token
    *
    * @param context
    *   the painless context
    * @return
    *   the painless script
    */
  override def painless(context: Option[PainlessContext]): String =
    identifier.painless(context)

  def isObject: Boolean = identifier.isObject
}

case class BucketPath(buckets: Seq[Bucket]) {

  lazy val path: String =
    buckets.foldLeft("")((acc, b) => {
      if (acc.isEmpty) {
        s"${b.path}"
      } else {
        s"$acc>${b.path}"
      }
    })

  override def toString: String = path
}

object MetricSelectorScript {

  def metricSelector(expr: Criteria): String = expr match {
    case Predicate(left, op, right, maybeNot, group) =>
      val leftStr = metricSelector(left)
      val rightStr = metricSelector(right)

      // Filtering all "1 == 1"
      if (leftStr == "1 == 1" && rightStr == "1 == 1") {
        "1 == 1"
      } else if (leftStr == "1 == 1") {
        rightStr
      } else if (rightStr == "1 == 1") {
        leftStr
      } else {
        val opStr = op match {
          case AND | OR => op.painless(None)
          case _        => throw new IllegalArgumentException(s"Unsupported logical operator: $op")
        }
        // `A AND NOT B`: the parser attaches the NOT to the RIGHT criteria (`Predicate.sql` and
        // `asFilter` agree); this used to prefix the LEFT one -- the exact complement of what was
        // asked. The negation is pushed INTO the right-hand expression when it is a single one
        // (`NOT MAX(x) > 45` renders `(params.max_x == null ? false : (params.max_x <= 45))`), so a
        // bucket whose metric is missing still fails the test -- `!(guard ? false : ...)` would let
        // it through, against SQL's three-valued NOT and the AC 4b contract. A compound right side
        // falls back to `!( ... )`.
        maybeNot match {
          case Some(_) =>
            negated(right) match {
              case Some(n) => s"($leftStr) $opStr ${metricSelector(n)}"
              // Grammar-unreachable today (`NOT (A AND B)` in HAVING is a parse rejection); kept
              // as the total fallback for a compound right side.
              case None => s"($leftStr) $opStr !($rightStr)"
            }
          case None if group => s"($leftStr) $opStr ($rightStr)"
          case None          => s"$leftStr $opStr $rightStr"
        }
      }

    case relation: ElasticRelation => metricSelector(relation.criteria)

    case _: MultiMatchCriteria => "1 == 1"

    case e: Expression if e.isAggregation || e.referencesBucketMetric =>
      // NO FILTERING: the script is generated for all metrics. The context-free rendering of an
      // aggregate predicate IS the bucket-pipeline rendering (`Expression.bucketPipelinePainless`):
      // `params.<metric>` reads, null-guarded, one parenthesised expression, temporal literal
      // already converted to epoch millis. It used to be converted HERE by appending
      // `.toInstant().toEpochMilli()` to the rendered predicate -- which only reached the literal
      // because the predicate happened to end with it.
      e.painless(None)
    case _ => "1 == 1"
  }

  /** The single expression `c` with its own NOT toggled, when `c` is one that carries a NOT. */
  private def negated(c: Criteria): Option[Criteria] = {
    def toggle(not: Option[NOT.type]): Option[NOT.type] = if (not.isDefined) None else Some(NOT)
    c match {
      case e: GenericExpression => Some(e.copy(maybeNot = toggle(e.maybeNot)))
      case e: Comparison        => Some(e.copy(maybeNot = toggle(e.maybeNot)))
      case e: BetweenExpr       => Some(e.copy(maybeNot = toggle(e.maybeNot)))
      case e: InExpr[_, _]      => Some(e.copy(maybeNot = toggle(e.maybeNot)))
      case _                    => None
    }
  }
}

case class BucketIncludesExcludes(values: Set[String] = Set.empty, regex: Option[String] = None)

/** Tree structure representing buckets and their hierarchy */
case class BucketNode(
  bucket: Bucket,
  children: Seq[BucketNode] = Seq.empty,
  private val parent: Option[BucketNode] =
    None // to track parent node in order to build bucket path
) {
  def identifier: String = bucket.path

  def findNode(id: String): Option[BucketNode] = {
    if (this.identifier == id) Some(this)
    else children.flatMap(_.findNode(id)).headOption
  }

  def depth: Int = 1 + (if (children.isEmpty) 0 else children.map(_.depth).max)

  def level: Int = {
    parent match {
      case Some(p) => 1 + p.level
      case None    => 0
    }
  }

  // Check if the node is a leaf
  def isLeaf: Boolean = children.isEmpty

  def bucketPath: String = {
    parent match {
      case Some(p) => s"${p.bucketPath}>$identifier"
      case None    => identifier
    }
  }

  def parentBucketPath: Option[String] = {
    parent.map(_.bucketPath)
  }

  def root: BucketNode = {
    parent match {
      case Some(p) => p.root
      case None    => this
    }
  }
}

case class BucketTree(
  roots: Seq[BucketNode] = Seq.empty
) {

  private def filterNode(predicate: BucketNode => Boolean, node: BucketNode): Seq[BucketNode] = {
    if (predicate(node)) {
      node +: node.children.flatMap(child => filterNode(predicate, child))
    } else {
      node.children.flatMap(child => filterNode(predicate, child))
    }
  }

  /** Filter the bucket trees based on a predicate
    * @param predicate
    *   the predicate to filter the buckets
    * @return
    *   trees of buckets that satisfy the predicate
    */
  def filter(predicate: BucketNode => Boolean): Seq[Seq[BucketNode]] = {
    roots.map(root => filterNode(predicate, root))
  }

  /** Filter the bucket trees based on a negated predicate
    * @param predicate
    *   the predicate to filter the buckets
    * @return
    *   trees of buckets that do not satisfy the predicate
    */
  def filterNot(predicate: BucketNode => Boolean): Seq[Seq[BucketNode]] = {
    roots.map(root => filterNode(node => !predicate(node), root))
  }

  /** Find a bucket node by its identifier
    *
    * @param id
    *   the identifier of the bucket
    * @return
    *   an option of the bucket node
    */
  def findNode(id: String): Option[BucketNode] = {
    roots.flatMap(_.findNode(id)).headOption
  }

  /** Find a bucket by its identifier
    *
    * @param id
    *   the identifier of the bucket
    * @return
    *   an option of the bucket
    */
  def find(id: String): Option[Bucket] = {
    findNode(id).map(_.bucket)
  }

  /** Get the total number of nodes in the bucket trees
    *
    * @return
    *   the total number of nodes
    */
  def size: Int = roots.map(countNodes).sum

  private def countNodes(node: BucketNode): Int = {
    1 + node.children.map(countNodes).sum
  }

  def maxDepth: Int = {
    if (roots.isEmpty) 0 else roots.map(_.depth).max
  }

  /** Get all bucket trees as sequences of nodes
    *
    * @return
    *   all node trees
    */
  def allTrees: Seq[Seq[BucketNode]] = roots.flatMap(collectTrees)

  /** Get all bucket trees as sequences of buckets
    *
    * @return
    *   all bucket trees
    */
  def allBuckets: Seq[Seq[Bucket]] = allTrees.map(_.map(_.bucket))

  private def collectTrees(node: BucketNode): Seq[Seq[BucketNode]] = {
    if (node.isLeaf) {
      Seq(Seq(node))
    } else {
      node.children.flatMap { child =>
        collectTrees(child).map(path => node +: path)
      }
    }
  }

  override def toString: String = {
    roots.flatMap(root => printNode(root, "", isLast = true)).mkString("\n")
  }

  private def printNode(
    node: BucketNode,
    prefix: String,
    isLast: Boolean,
    acc: Seq[String] = Seq.empty
  ): Seq[String] = {
    val connector = if (isLast) "└── " else "├── "

    val childPrefix = prefix + (if (isLast) "    " else "│   ")

    (acc :+ s"$prefix$connector${node.identifier} (path: ${node.bucketPath})") ++ node.children.zipWithIndex
      .flatMap { case (child, idx) =>
        printNode(child, childPrefix, idx == node.children.size - 1)
      }
  }
}

object BucketTree {

  def fromBuckets(buckets: Seq[Seq[Bucket]]): BucketTree = {
    if (buckets.isEmpty || buckets.forall(_.isEmpty)) {
      return BucketTree(Seq.empty)
    }

    val validBuckets = buckets.filter(_.nonEmpty)

    // Group by root bucket path
    val groupedByRoot = validBuckets.groupBy(_.head.path)

    val roots = groupedByRoot
      .map { case (_, pathsWithSameRoot) =>
        buildNode(pathsWithSameRoot)
      }
      .toSeq
      .sortBy(_.identifier)

    BucketTree(roots)
  }

  private def buildNode(paths: Seq[Seq[Bucket]], parent: Option[BucketNode] = None): BucketNode = {
    val currentBucket = paths.head.head

    val childPaths = paths
      .filter(_.size > 1)
      .map(_.tail)

    val node = BucketNode(currentBucket, parent = parent)

    val children = if (childPaths.isEmpty) {
      Seq.empty
    } else {
      childPaths
        .groupBy(_.head.path)
        .map { case (_, childPathGroup) => buildNode(childPathGroup, Some(node)) }
        .toSeq
        .sortBy(_.identifier)
    }

    BucketNode(currentBucket, children, parent)
  }
}
