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
import scala.util.Try
import scala.util.matching.Regex
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

/** What a `HAVING` criterion contributes to a bucket pipeline. THREE values, because `""` used to
  * mean two things and the caller could not tell them apart (issue #389): "this level has nothing
  * to filter" and "I cannot express this filter" both came back as the empty string, and the second
  * was read as the first -- so a predicate the engine could not emit was silently DROPPED and every
  * group came back with HTTP 200.
  */
sealed trait MetricSelector

object MetricSelector {

  /** Nothing for the bucket pipeline to do at this level. Legitimate and common: a predicate over
    * the bucket KEY is honoured by the `terms` `include` / `exclude` instead (`HAVING status =
    * 'A'`), and a condition reading another level's metric belongs to that level.
    */
  case object NoFilter extends MetricSelector

  /** A `bucket_selector` `script.source`: ONE boolean expression reading published metrics. */
  case class Filter(script: String) extends MetricSelector

  /** The predicate references an aggregate and the engine cannot express it as a `bucket_selector`.
    * It is REFUSED by name at `Having.validate()`, never dropped.
    */
  case class Unrepresentable(expression: String, reason: String) extends MetricSelector {

    /** 🔴 The remedy is "compare the aggregate itself", NOT "alias it in SELECT". The sibling rule
      * on inline arithmetic (`HAVING MAX(x) - MIN(x) > 10`) does say to alias it, and that advice
      * does NOT transfer: MEASURED, `SELECT ABS(COUNT(*)) AS a ... GROUP BY city` is itself
      * rejected ("Non-aggregated fields ... cannot be selected when GROUP BY is present") for every
      * member of this family, so the borrowed wording would send the user to a dead end.
      */
    def message: String =
      s"HAVING cannot be applied to $expression: $reason. " +
      "Compare the aggregate itself (HAVING SUM(x) > 10 rather than HAVING ROUND(SUM(x), 2) > 10), " +
      "or apply the function to the result outside the query."
  }
}

object MetricSelectorScript {

  import MetricSelector._

  /** The bucket-pipeline script of a `HAVING` criterion, or `"1 == 1"` when there is nothing to
    * filter at this level.
    *
    * 🔴 It THROWS on an [[MetricSelector.Unrepresentable]] criterion, and that is the point:
    * returning `""` is what let a predicate vanish. `Having.validate()` refuses every such
    * statement inside `Parser.apply`, so no venue can reach this throw with a parsed statement --
    * it is the invariant's second line of defence, for a `SingleSearch` assembled in code and
    * emitted without validation.
    */
  def metricSelector(expr: Criteria): String = selector(expr) match {
    case NoFilter           => "1 == 1"
    case Filter(script)     => script
    case u: Unrepresentable => throw new IllegalStateException(u.message)
  }

  /** Every `HAVING` criterion of this tree the engine cannot express, in statement order. Read by
    * `Having.validate()`; empty for every criterion tree [[metricSelector]] can render.
    */
  def unrepresentable(expr: Criteria): Seq[MetricSelector.Unrepresentable] = expr match {
    case Predicate(left, _, right, maybeNot, _) =>
      unrepresentable(left) ++ (maybeNot match {
        case Some(_) => right.negated.map(unrepresentable).getOrElse(unrepresentable(right))
        case None    => unrepresentable(right)
      })
    case relation: ElasticRelation => unrepresentable(relation.criteria)
    case _ =>
      selector(expr) match {
        case u: Unrepresentable => Seq(u)
        case _                  => Nil
      }
  }

  private def selector(expr: Criteria): MetricSelector = expr match {
    case Predicate(left, op, right, maybeNot, group) =>
      // The RIGHT criterion as it is really rendered: a `NOT` on the predicate is pushed INTO it
      // when it can carry one, so the verdict must be taken on the SAME node the script is built
      // from -- otherwise a refusal could be decided on a criterion nothing emits.
      val effectiveRight = maybeNot.flatMap(_ => right.negated).getOrElse(right)
      val leftSel = selector(left)
      val rightSel = selector(effectiveRight)
      (leftSel, rightSel) match {
        case (u: Unrepresentable, _) => u
        case (_, u: Unrepresentable) => u
        // Filtering all "1 == 1"
        case (NoFilter, NoFilter) => NoFilter
        case (NoFilter, r)        => r
        case (l, NoFilter)        => l
        case (Filter(leftStr), Filter(rightStr)) =>
          val opStr = op match {
            case AND | OR => op.painless(None)
            case _ => throw new IllegalArgumentException(s"Unsupported logical operator: $op")
          }
          // `A AND NOT B`: the parser attaches the NOT to the RIGHT criteria (`Predicate.sql` and
          // `asFilter` agree); this used to prefix the LEFT one -- the exact complement of what was
          // asked. The negation is pushed INTO the right-hand expression when it is a single one
          // (`NOT MAX(x) > 45` renders `(params.max_x == null ? false : (params.max_x <= 45))`), so
          // a bucket whose metric is missing still fails the test -- `!(guard ? false : ...)` would
          // let it through, against SQL's three-valued NOT and the AC 4b contract. A compound right
          // side falls back to `!( ... )`.
          Filter(maybeNot match {
            case Some(_) if right.negated.isDefined => s"($leftStr) $opStr $rightStr"
            // Grammar-unreachable today (`NOT (A AND B)` in HAVING is a parse rejection); kept
            // as the total fallback for a compound right side.
            case Some(_)       => s"($leftStr) $opStr !($rightStr)"
            case None if group => s"($leftStr) $opStr ($rightStr)"
            case None          => s"$leftStr $opStr $rightStr"
          })
      }

    case relation: ElasticRelation => selector(relation.criteria)

    case _: MultiMatchCriteria => NoFilter

    case e: Expression if e.isAggregation || e.referencesBucketMetric =>
      // NO FILTERING: the script is generated for all metrics. The context-free rendering of an
      // aggregate predicate IS the bucket-pipeline rendering (`Expression.bucketPipelinePainless`):
      // `params.<metric>` reads, null-guarded, one parenthesised expression, temporal literal
      // already converted to epoch millis. It used to be converted HERE by appending
      // `.toInstant().toEpochMilli()` to the rendered predicate -- which only reached the literal
      // because the predicate happened to end with it.
      Filter(e.painless(None))

    // 🔴 Issue #389 -- the predicate reads an aggregate through a FUNCTION (`ABS(COUNT(*)) > 1`,
    // `COALESCE(COUNT(*), 0) > 1`, `1 < ABS(COUNT(*))`). Neither flag above sees it, because a
    // function never looks inside its own arguments, so it fell to the `1 == 1` catch-all below
    // and the whole condition VANISHED. Emitted when the rendering is provably a single boolean
    // expression; refused BY NAME when it is not. Never dropped.
    case e: Expression if e.bucketMetrics.nonEmpty =>
      representable(e) match {
        case Left(reason)  => Unrepresentable(e.sql, reason)
        case Right(script) => Filter(script)
      }

    case _ => NoFilter
  }

  /** This predicate's `bucket_selector` script, or the reason the engine cannot express it.
    *
    * ONE rendering: the verdict and the script it authorises come out of the same call, so the
    * emission can never be the second render of a tree whose FIRST render was the one vetted.
    *
    * 🔴 The verdict is taken on the RENDERING, never on a list of function names. A name list is a
    * fourth derivation of what the renderer does and would disagree with it the first time a
    * rendering changed (`project_type_derivations_runtime_declared_reported`); the rendering is the
    * thing Elasticsearch actually compiles. And it is a POSITIVE proof -- everything this method
    * cannot account for is refused, because a `HAVING` that cannot be expressed must fail, never
    * silently return every group (`feedback_nonsense_input_fails_loudly`).
    *
    * The four disqualifiers, each MEASURED on a real rendering:
    *   1. it cannot be rendered at all without a document context -- `CASE WHEN ... END` throws; 2.
    *      it carries a STATEMENT, not an expression -- `ROUND(MAX(x), 2)` renders `def arg1 =
    *      Math.pow(10, 2); ...`, and a `bucket_selector` source is one expression; 3. it can
    *      evaluate to NULL -- `NULLIF(COUNT(*), 0) > 1` renders `(params.c) == 0 ? null :
    *      (params.c) > 1`, which hands Elasticsearch a `null` where a boolean is required. `? null
    *      :` is the renderer's OWN null-producing idiom and is already the marker the repo's
    *      null-safety guard keys on; 4. its rendering BOXES a number -- `ABS(COUNT(*)) > 1` renders
    *      `Double.valueOf(Math.abs(params.c)) > 1`, and the bucket-pipeline script context does not
    *      compile it; 5. it reads a local nothing binds -- `NULLIF(c, 0) > 1` over a SELECT alias
    *      renders `arg0 == 0 ? null : arg0 > 1`, and `arg0` is a `PainlessContext` parameter that
    *      no bucket pipeline declares. Every free lower-case word is refused unless it is a
    *      Painless literal (`null` / `true` / `false`) or the `params` map itself: a Painless
    *      expression has no free functions, so a method call is always preceded by a `.` and a
    *      class is capitalised.
    *
    * 🔴 Rule 4 is the one NO amount of reading could have produced, and it is why the acceptance
    * bar for this issue was EXECUTION and not a byte pin
    * (`feedback_assert_the_mechanism_not_a_proxy`
    * -- PARSES != RENDERS != RUNS). The spec asserted that `Double.valueOf(Math.abs(params.c)) > 1`
    * was "legal as a bucket_selector source"; MEASURED on a real Elasticsearch 8.18.3 it is a
    * `class_cast_exception: Cannot cast from [java.lang.Double] to [int]` at COMPILE time, and the
    * whole search fails. The rule is DERIVED, not guessed: eighteen shapes were rendered and
    * executed, and `.valueOf(` separates the nine that run from the nine that do not, exactly. The
    * same call compiles in the QUERY script context (`WHERE ABS(x) > 1` runs), so this is a
    * property of the bucket-pipeline context, not of the rendering alone -- which is precisely why
    * a syntactic gate needs a measured rule here and cannot derive one.
    *
    * Rule 5's complement also proves rule 3 is not the only null route, and rules 2-5 are decided
    * on the rendering with every STRING LITERAL blanked, so a `;`, a `?` or a word inside one is
    * never read as code.
    */
  private[query] def representable(e: Expression): Either[String, String] =
    Try(e.functionBucketPipelinePainless).toOption match {
      case None =>
        Left("it cannot be rendered without a document, and a bucket pipeline has none")
      case Some(rendering) => disqualifyRendering(rendering).toLeft(rendering)
    }

  /** Rules 2-6 of [[representable]], asked of a RENDERING rather than of an expression.
    *
    * Exposed as its own function because it is a pure function of a string and is therefore the
    * only surface on which each rule can be falsified INDIVIDUALLY
    * (`feedback_assert_the_mechanism_not_a_proxy`): the rules overlap on real SQL -- every shape
    * that trips the top-level-conditional rule also renders a `? null :` -- so a mutation matrix
    * driven by statements alone reports a live rule as unguarded. The end-to-end rows stay; these
    * are what keep each rule honest.
    */
  private[query] def disqualifyRendering(rendering: String): Option[String] =
    disqualify(blankStringLiterals(rendering))

  private def disqualify(code: String): Option[String] = {
    if (code.contains(";") || code.contains("def "))
      Some(
        "its rendering needs a local declaration, and a bucket_selector source is one expression"
      )
    else if (code.contains("? null :"))
      Some("its rendering can evaluate to NULL, and a bucket_selector source must be a boolean")
    else if (conditionalAtTopLevel(code))
      Some(
        "its outermost operator is a conditional rather than a comparison, so it does not render a boolean"
      )
    else if (code.contains(".valueOf("))
      Some(
        "its rendering boxes a number, which the bucket-pipeline script context does not compile"
      )
    else
      freeLocal(code).map(name =>
        s"its rendering reads '$name', which nothing binds in a bucket pipeline"
      )
  }

  /** The same string with the CONTENT of every Painless string literal replaced by spaces, so the
    * scanners above read code and only code. Length-preserving, so an index into the result is an
    * index into the original.
    */
  private def blankStringLiterals(s: String): String = {
    val out = new StringBuilder(s)
    var i = 0
    while (i < s.length) {
      val c = s.charAt(i)
      if (c == '"' || c == '\'') {
        var j = i + 1
        while (j < s.length && s.charAt(j) != c) {
          if (s.charAt(j) == '\\') { out.setCharAt(j, ' '); j += 1 }
          if (j < s.length) { out.setCharAt(j, ' '); j += 1 }
        }
        i = j + 1
      } else i += 1
    }
    out.toString
  }

  /** Is there a `?` outside every parenthesis? Then the comparison is not the outermost operator
    * and the expression's value is whatever the branches are -- not necessarily a boolean.
    */
  private def conditionalAtTopLevel(code: String): Boolean = {
    var depth = 0
    var i = 0
    while (i < code.length) {
      code.charAt(i) match {
        case '(' => depth += 1
        case ')' => depth -= 1
        case '?' => if (depth <= 0) return true
        case _   =>
      }
      i += 1
    }
    false
  }

  private val FreeWord: Regex = """(?<![\w.$])([a-z_][A-Za-z0-9_]*)""".r

  /** The first lower-case word the rendering uses as a value of its own: not a method (those follow
    * a `.`), not a call, not a Painless literal, not the `params` map.
    */
  private def freeLocal(code: String): Option[String] =
    FreeWord
      .findAllMatchIn(code)
      .collectFirst {
        case m
            if !PainlessLiterals.contains(m.group(1)) &&
              m.group(1) != "params" &&
              !(m.end < code.length && code.charAt(m.end) == '(') =>
          m.group(1)
      }

  private val PainlessLiterals: Set[String] = Set("null", "true", "false")

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
