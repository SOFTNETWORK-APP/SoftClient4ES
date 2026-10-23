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

import app.softnetwork.elastic.sql.`type`.{
  SQLBigInt,
  SQLBool,
  SQLChar,
  SQLDouble,
  SQLInt,
  SQLReal,
  SQLSmallInt,
  SQLTemporal,
  SQLTinyInt,
  SQLType,
  SQLTypes,
  SQLVarchar
}
import app.softnetwork.elastic.sql.function.FunctionUtils
import app.softnetwork.elastic.sql.function.aggregate._
import app.softnetwork.elastic.sql.function.cond.{functionsOf, Case, NumericReducer}
import app.softnetwork.elastic.sql.function.convert.Conversion
import app.softnetwork.elastic.sql.function.time.{DateTrunc, FormatsDateDocValue}
import app.softnetwork.elastic.sql.operator._
import app.softnetwork.elastic.sql.time.TimeUnit
import scala.util.{DynamicVariable, Try}
import scala.util.matching.Regex
import app.softnetwork.elastic.sql.function.{
  Function,
  FunctionChain,
  FunctionN,
  FunctionWithIdentifier
}
import app.softnetwork.elastic.sql.{
  quoteIdentifier,
  renderedTypeOf,
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

  /** Whether `text` is a number Elasticsearch reads as exactly one value of `valueType` (`long`: a
    * whole number within its range, `double`: any decimal number) -- see
    * [[Bucket.scriptedKeyValueTypeBeside]].
    */
  def isExactNumberOf(valueType: String)(text: String): Boolean =
    Try(BigDecimal(text.trim)).toOption.exists { number =>
      valueType match {
        case "long"   => number.isWhole && number.isValidLong
        case "double" => true
        case _        => false
      }
    }

  /** The `DATE_TRUNC` units a `date_histogram` has a calendar interval for. A level is a
    * `date_histogram` only with one of them: Elasticsearch refuses one without an interval.
    */
  val DateHistogramUnits: Set[TimeUnit] = Set(
    TimeUnit.YEARS,
    TimeUnit.QUARTERS,
    TimeUnit.MONTHS,
    TimeUnit.WEEKS,
    TimeUnit.DAYS,
    TimeUnit.HOURS,
    TimeUnit.MINUTES,
    TimeUnit.SECONDS
  )

  /** A column's OWN type, without the functions a key applies to it. */
  private[query] def columnType(column: Identifier): SQLType = column.withFunctions(Nil).baseType

  /** The mapped types of a FIELD level whose key filter is proven exact
    * ([[Bucket.isExactKeyKind]]).
    */
  private[query] def exactFieldType(t: SQLType): Boolean = t match {
    case SQLTypes.Keyword                                                         => true
    case _: SQLTinyInt | _: SQLSmallInt | _: SQLInt | _: SQLBigInt | _: SQLDouble => true
    case _                                                                        => false
  }

  /** The column types a computed key of a proven kind may read ([[Bucket.isExactKeyKind]]): the
    * whole numbers, DOUBLE, the strings, the dates and BOOLEAN. Never REAL or NUMERIC, which
    * Painless computes with in double, and never a type the mapping did not give.
    */
  private[query] def exactSourceType(t: SQLType): Boolean = t match {
    case _: SQLReal                                                               => false
    case _: SQLTinyInt | _: SQLSmallInt | _: SQLInt | _: SQLBigInt | _: SQLDouble => true
    case _: SQLVarchar | _: SQLChar | _: SQLTemporal | _: SQLBool                 => true
    case _                                                                        => false
  }

  /** A string type: the type of a text column, or of a value a function renders as text. */
  private[query] def stringType(t: SQLType): Boolean =
    t.isInstanceOf[SQLVarchar] || t.isInstanceOf[SQLChar]

  /** Every text a Java number EQUAL to `number` prints, for [[Bucket.keyFilterTexts]]:
    *   - a whole value: its integer text and its `Double` text (`5` and `5.0`, `1.0E7` for ten
    *     million), and for zero both `0.0` and `-0.0`;
    *   - a fractional value: its `Double` text, which no integral key prints;
    *   - with `real`, also its `Float` text;
    *   - NaN and the infinities: their own text.
    *
    * Two finite numbers print the same Java text only if they are equal, so no text here matches a
    * key the comparison does not hold for.
    */
  private[query] def numberTexts(number: java.lang.Number, real: Boolean): Set[String] = {
    val own = number.toString
    val asDouble = number.doubleValue
    if (asDouble.isNaN || asDouble.isInfinite) Set(own)
    else
      Try(BigDecimal(own)).toOption match {
        case None => Set(own)
        case Some(exact) =>
          val texts =
            if (!exact.isWhole) Set(java.lang.Double.toString(asDouble))
            else if (asDouble == 0d) Set(exact.toBigInt.toString, "0.0", "-0.0")
            else Set(exact.toBigInt.toString, java.lang.Double.toString(asDouble))
          if (real) texts + java.lang.Float.toString(number.floatValue) else texts
      }
  }

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
    // `LIMIT n OFFSET m` answers the n groups after the first m: each level holds n + m
    // (`GroupedRowOrder` skips the m)
    val bucketSize = request.limit
      .map(l =>
        scala.math
          .min(l.limit.toLong + l.offset.map(_.offset.max(0)).getOrElse(0), Int.MaxValue.toLong)
          .toInt
      )
      .orElse(Some(Bucket.DefaultSize))
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

  /** Story IDENT-4 -- do two buckets group on the SAME key? The ONE comparison every "which level
    * does this HAVING leaf address" question reads: the three include sites of `Where.scala`
    * (`Expression`, `InExpr`, `MatchCriteria`) and `SingleSearch.keyBucketOf`.
    *
    * It used to be `name == other.name`, and [[name]] is the DISPLAY name -- the SELECT alias, else
    * the column. Three ways that is not an identity:
    *   - a SELF-JOIN's two levels (`GROUP BY o.category, p.category`) are both named `category`
    *     after one resolution, so a condition on one correlation filtered BOTH levels;
    *   - two FUNCTIONS of one column (`GROUP BY YEAR(d), MONTH(d)`) are both named `d` when the
    *     SELECT does not project them, so `HAVING MONTH(d) = 1` also filtered the YEAR level;
    *   - a copy recorded before resolution (`a.category`) never equalled its resolved level
    *     (`category`), which is why the qualified spelling was refused.
    *
    * So the key is the display name AND the correlation AND the grouped expression -- strings only,
    * never `Identifier.equals`, which compares a doc access, not a grouping key.
    *
    * A NESTED level keeps the display-name comparison: its attached copy is recorded from the
    * parsed spelling (no alias, no nested element) while the level is resolved, and only the
    * display name agrees between them (`sourceBucket`).
    */
  def sameKeyAs(other: Bucket): Boolean =
    name == other.name && (nested || other.nested || (
      identifier.tableAlias == other.identifier.tableAlias &&
      identifier.identifierName == other.identifier.identifierName
    ))

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

  /** The calendar unit of this level's `date_histogram`: the unit of a key that is ONE `DATE_TRUNC`
    * of a unit Elasticsearch has an interval for, and `None` for every other key.
    */
  lazy val dateHistogramUnit: Option[TimeUnit] =
    identifier.functions match {
      case (trunc: DateTrunc) :: Nil if Bucket.DateHistogramUnits.contains(trunc.unit) =>
        Some(trunc.unit)
      case _ => None
    }

  /** Story IDENT-4 -- does this level reach Elasticsearch as a `date_histogram`? The ONE derivation
    * both bridges' emission AND the HAVING classifier read (#292): a `date_histogram` carries no
    * include / exclude, so a key filter addressed to this level could not be applied. It is a
    * `date_histogram` rather than a `terms` aggregation only for a key that has an interval
    * ([[dateHistogramUnit]]).
    *
    * 🔴 It reads the KEY EXPRESSION, never `out`. `out` is the COLUMN's type, so with the mapping
    * attached every key over a date column read as temporal and was emitted as a `date_histogram`,
    * with no interval unless the key was one `DATE_TRUNC`: `GROUP BY d`, `GROUP BY YEAR(d)`, `GROUP
    * BY s, d` all failed with HTTP 400 ("Invalid interval specified" on Elasticsearch 8, "Zero or
    * negative time interval not supported" on 6.8). A concrete index always resolved the mapping;
    * since an index pattern does too, the same statements failed there, where 0.23.0 answered them
    * with a `terms` level.
    */
  def isDateHistogram: Boolean = dateHistogramUnit.isDefined

  /** Whether this level's key is the key expression's SCRIPT rather than a field.
    *
    * A key with no field name must be scripted ([[shouldBeScripted]]). So must a key that applies a
    * FUNCTION to a column, unless it is a `date_histogram` (whose interval is the function): a
    * function whose own `shouldBeScripted` is false (`ISNULL`, `DATE_FORMAT`, `WEEKDAY`,
    * `LAST_DAY`, ...) left the level a `terms` on the bare field, so the groups were the column's
    * raw values, with HTTP 200 (`GROUP BY ISNULL(s)` answered one group per value of `s`).
    *
    * Scoped to the level: `shouldBeScripted` keeps its meaning for every other reader.
    */
  def isScriptedKey: Boolean =
    !isBucketScript && (shouldBeScripted || (!isDateHistogram && identifier.functions.nonEmpty))

  /** Whether this level builds a bucket for the documents whose key is NULL. It does not: no bridge
    * sets the `missing` option of a `terms` or `date_histogram` level (the only `missing` either
    * bridge writes is a SORT option), so a document with no key value -- or whose key script
    * returns null -- falls in no bucket of the level. So on every bucket it builds, `<key> IS NOT
    * NULL` is TRUE, and `<key> IS NULL` has no bucket to keep.
    *
    * The ONE statement of that premise: the HAVING rule that reads it
    * (`SingleSearch.keepsEveryGroup`) follows it if a level ever emits the NULL group.
    */
  def hasNullBucket: Boolean = false

  /** Whether a key filter on this level would compare a DATE, TIME or TIMESTAMP with the
    * condition's TEXT: a scripted key ([[isScriptedKey]]) whose expression renders a temporal type
    * (`renderedTypeOf`, the type every rule on the resolved statement reads).
    *
    * A scripted `terms` key is a string `terms`: Elasticsearch compares each `include` / `exclude`
    * value with the text of the value the script returns, which is whatever the Java object prints
    * -- `"2024-01-31T00:00Z"` for `LAST_DAY(d)` on Elasticsearch 8, never the `'2024-01-31'` the
    * condition spells (MEASURED: `=` / `IN` kept no group and `<>` kept every group, HTTP 200). No
    * function of this layer owns that text, so the filter is refused by the key's output type,
    * never by its rendering.
    *
    * 🔴 A different fact from [[isDateHistogram]], the level TYPE both bridges emit: this one is
    * whether the key filter the HAVING classifier lets through can MATCH. It is read by the
    * classifier alone (`SingleSearch.temporalScriptLevelOf`), at parse and on the resolved
    * statement; no bridge reads it.
    */
  def isTemporalScriptKey: Boolean = isScriptedKey && renderedTypeOf(identifier).isTemporal

  /** The `value_type` a scripted `terms` level is sent with: `long` for a key that reports a whole
    * number, `double` for one that reports another number, nothing for any other key.
    *
    * Without it a scripted `terms` level is a STRING `terms`: each key comes back as the text the
    * script's value prints and the level orders those texts. MEASURED on Elasticsearch 8.18.3:
    * `GROUP BY n + 0 ORDER BY kk DESC` answered `'900', '7', ..., '0', '-900', '-4'`, where the
    * field key `GROUP BY n` answers the numbers in numeric order. With it, Elasticsearch builds a
    * numeric `terms` (MEASURED with a script on 8.18.3 and 6.8.23), so the keys come back as
    * numbers and sort as numbers.
    *
    * Read by both bridges through [[scriptedKeyValueTypeBeside]]: a HAVING key filter is written as
    * the TEXTS a string `terms` matches ([[keyFilterTexts]]), which a numeric level parses as its
    * numbers. A temporal key is left as it is (`date_histogram` or its own text).
    */
  def scriptedKeyValueType: Option[String] =
    if (!isScriptedKey || isDateHistogram) None
    else
      renderedTypeOf(identifier) match {
        case _: SQLTinyInt | _: SQLSmallInt | _: SQLInt | _: SQLBigInt => Some("long")
        case _: SQLDouble | _: SQLReal                                 => Some("double")
        case SQLTypes.Numeric                                          => Some("double")
        case _                                                         => None
      }

  /** [[scriptedKeyValueType]] for a level whose HAVING key filter is `include` / `exclude`: the
    * type when the filter is exact values only, each an exact number of that type, which a numeric
    * `terms` parses as the very numbers its keys are; `None` -- a string `terms`, its keys texts --
    * beside a pattern, or a value that is not one. MEASURED on Elasticsearch 8.18.3 and 6.8.23: a
    * scripted `terms` with `value_type: long` keeps the groups `include: ["5", "7"]` names, drops
    * the one `exclude: ["5"]` names, and answers its keys as numbers in numeric order -- but it
    * reads `"5.5"` as 5, which is why an inexact value keeps the string `terms`.
    *
    * 🔴 Without the filter's values a scripted numeric key with a key filter (`GROUP BY n + 0
    * HAVING kk <> 5 ORDER BY kk DESC`) answered its keys as texts in text order: `'900', '7', '0',
    * '-900', '-4'`, where DuckDB answers 900, 7, 0, -4, -900.
    */
  def scriptedKeyValueTypeBeside(
    include: BucketIncludesExcludes,
    exclude: BucketIncludesExcludes
  ): Option[String] =
    if (Seq(include, exclude).exists(_.regex.exists(_.nonEmpty))) None
    else
      scriptedKeyValueType.filter { valueType =>
        (include.values ++ exclude.values).forall(Bucket.isExactNumberOf(valueType))
      }

  /** The columns this level's key reads -- through its functions and its arithmetic, a CASE's
    * branches and its conditions -- each with the functions the key applies to it.
    */
  def keyColumns: Seq[Identifier] =
    (FunctionUtils.funIdentifiers(identifier) ++
      Case
        .conditionsOf(identifier)
        .flatMap(_.referencedIdentifiers)
        .flatMap(FunctionUtils.funIdentifiers(_)))
      .filter(_.name.nonEmpty)

  /** Whether the mapping typed every column this level's key reads. With no mapping every column is
    * `Any`: nothing is known of the key's value or of the text its script prints.
    */
  def keyTypesKnown: Boolean = keyColumns.forall(c => !Bucket.columnType(c).isUnknown)

  /** Is this level's key of a kind whose key filter Elasticsearch is PROVEN to match exactly as the
    * HAVING rules model it -- the ONE predicate that says so (#292), read by the exact decider
    * (`SingleSearch.keyFilterExactly`), the nameless-key gate
    * (`SingleSearch.namelessKeyRouteReason`), the `<key> IS NOT NULL` rule
    * (`SingleSearch.keepsEveryGroup`), [[keyFilterTexts]] and [[keyOf]]. A level of any other kind
    * is judged as before: by the shape rules and their texts.
    *
    * A FIELD level, of a mapped type whose include / exclude values Elasticsearch 8.18.3 parses as
    * [[keyOf]] does (`TermsAggregatorFactory`, `IncludeExclude`, `DocValueFormat`):
    *   - `keyword`: a values source of bytes in the RAW format, whose values are compared as the
    *     bytes they are;
    *   - `byte`, `short`, `integer`, `long`: a numeric source that is not floating-point, read
    *     through `convertToLongFilter(DocValueFormat.RAW)`, that is `Long.parseLong`, else the
    *     floor of `Double.parseDouble`;
    *   - `double`: a floating-point source, read through `convertToDoubleFilter`, that is
    *     `Double.parseDouble`, compared bit for bit with the key.
    *
    * Not proven, so not decided: `float` (the same `Double.parseDouble`, against a key that is the
    * float widened to a double), `half_float`, `scaled_float` and `unsigned_long` (no SQL type, or
    * `NUMERIC`), every date type and `ip` (parsed by their own format, so two spellings name one
    * key), `boolean`, `text`, and any column the mapping does not type.
    *
    * A COMPUTED level is a string `terms`: the script's values have no ordinals, so the level runs
    * in the `map` mode (`TermsAggregatorFactory.bytesSupplier`), each value is the text of the
    * script's value (`ScriptBytesValues`: `toString()`), and the include / exclude is the
    * `SetAndRegexStringFilter` of `convertToStringFilter(RAW)`: membership of the values as the
    * bytes they are, and a full match of the pattern -- the set a `keyword` field's
    * `SetAndRegexOrdinalsFilter` accepts, by the same values and automaton. Proven:
    *   - a key whose value is the number SQL computes and prints as a `Long`, an `Integer`, a
    *     `Short`, a `Byte` or a `Double`, every text of which [[keyFilterTexts]] sends: it renders
    *     a number other than REAL, and reads only columns of a mapped type that is a whole number,
    *     a DOUBLE, a string, a date or a BOOLEAN (a date part, a LENGTH, an integer cast of a
    *     string are exact integers of their argument). Not proven: a key reading a REAL or a
    *     NUMERIC column, or converting anything to REAL or NUMERIC, anywhere in its expression
    *     (Painless computes it in double, DuckDB in REAL or DECIMAL). A `ROUND` to decimal places
    *     is the DOUBLE DuckDB computes (`Round`), and prints as one;
    *   - a key that renders a string computed from strings alone (`UPPER(k)`, `SUBSTRING(k, 1, 3)`,
    *     `CONCAT(k1, k2)`, `TRIM(k)`): every column it reads is a string, and every function and
    *     operand in its expression renders a string, so nothing converts a number, a date or a
    *     BOOLEAN to a text (`CAST(n AS VARCHAR)`, `CONCAT(k, 1)`, `CAST(LENGTH(k) AS VARCHAR)` are
    *     not proven). Never proven: a key that renders a BOOLEAN or a date, and a key reading a
    *     column the mapping does not type.
    */
  def isExactKeyKind: Boolean =
    if (isScriptedKey) isExactComputedKey
    else
      !isBucketScript && !isDateHistogram && identifier.functions.isEmpty &&
      Bucket.exactFieldType(Bucket.columnType(identifier))

  private[this] def isExactComputedKey: Boolean = {
    val rendered = renderedTypeOf(identifier)
    val functions = functionsOf(identifier) ++ keyColumns.flatMap(functionsOf)
    if (Bucket.stringType(rendered))
      keyColumns.forall(c => Bucket.stringType(Bucket.columnType(c))) &&
      functions.forall(f => Bucket.stringType(renderedTypeOf(f)))
    else
      rendered.isNumber && !rendered.isInstanceOf[SQLReal] &&
      keyColumns.forall(c => Bucket.exactSourceType(Bucket.columnType(c))) &&
      !functions.exists {
        case conversion: Conversion =>
          conversion.targetType.isInstanceOf[SQLReal] || conversion.targetType == SQLTypes.Numeric
        case _ => false
      }
  }

  /** The texts a HAVING key filter sends on THIS level for the literal `value`: the ONE writer of a
    * key-filter VALUE (#292). `Expression.includes` (its `=` and `<>` arms) and `InExpr.includes`
    * call it; both bridges emit the lists it fills, and every rule that judges the key filter reads
    * those same lists.
    *
    * A scripted `terms` level is a string `terms`: Elasticsearch compares each value with the TEXT
    * the key's script value prints, which is whatever its Java class prints -- a Long `5`, a Double
    * `5.0`, an int date part `1`. That class is decided by the emitter (a cast's coercion, a
    * function's integer or `Math` form, the column's doc value), and nothing that reads the key
    * reads it: `renderedTypeOf` is not the printed class (`CAST(n AS DOUBLE)` without a mapping
    * prints the raw Long). So the literal was sent in its OWN spelling, and the match depended on
    * that spelling and on the key's runtime class, never on SQL equality. MEASURED on Elasticsearch
    * 8.18.3: `GROUP BY CONVERT(n, DOUBLE) HAVING CONVERT(n, DOUBLE) IN (5, 1)` over an index
    * pattern sent `["5","1"]` against a key printing `5.0` and answered no group, and `GROUP BY
    * MONTH(d) HAVING MONTH(d) IN (1.0, 3.0)` sent `["1.0","3.0"]` against a key printing `1`.
    *
    * A NUMBER on a scripted key that renders neither a string, a BOOLEAN nor a temporal type (a
    * temporal one is refused, [[isTemporalScriptKey]]) is therefore sent in EVERY text a Java
    * number of that value prints ([[Bucket.numberTexts]]), and the filter matches the key however
    * its script prints it. So is a QUOTED number on a key that renders a number, when it is a
    * number of the key's type (`ABS(x) = '5'`): DuckDB casts the string to the key's type, and the
    * key prints `5.0`. That holds on a key of a proven kind ([[isExactKeyKind]]), and on a key
    * whose columns no mapping typed, whose printed class is unknown (on a key that renders REAL
    * there, the `Float` text is sent too). A key the mapping shows to be of another kind -- a REAL
    * computed in double -- keeps the literal's own text, as before: no spelling is known to match
    * its value, and the nameless-key gate judges that text as it did. Everything else is sent as
    * its own text too: Elasticsearch parses a FIELD level's values itself, and a string or a
    * boolean prints as itself.
    *
    * Called once per literal while the request is built.
    */
  def keyFilterTexts(value: Any): Set[String] =
    if (!isScriptedKey || (keyTypesKnown && !isExactKeyKind)) Set(value.toString)
    else
      value match {
        case number: java.lang.Number =>
          val rendered = renderedTypeOf(identifier)
          if (rendered.isText || rendered.isBoolean || rendered.isTemporal) Set(number.toString)
          else Bucket.numberTexts(number, real = rendered.isInstanceOf[SQLReal])
        case quoted: String =>
          val rendered = renderedTypeOf(identifier)
          Try(BigDecimal(quoted)).toOption
            .filter(number =>
              rendered.isNumber && (number.isWhole || !NumericReducer.wholeNumber(rendered))
            )
            .map(number => Bucket.numberTexts(number, real = rendered.isInstanceOf[SQLReal]))
            .getOrElse(Set(quoted))
        case other => Set(other.toString)
      }

  /** The KEY a key-filter value names on this level, as Elasticsearch reads it -- the ONE
    * numeric-identity function of key-filter values, read by `SingleSearch.keyFilterExactly`. It
    * reads the type knowledge [[isExactKeyKind]] proves.
    *
    * A numeric FIELD level of a proven kind is a numeric `terms` aggregation: Elasticsearch parses
    * each include / exclude value as a number, so `"5"` and `"5.0"` name ONE key, the number 5. An
    * integer field reads a value with `DocValueFormat.RAW.parseLong` (`Long.parseLong`, else the
    * floor of `Double.parseDouble`), a `double` field with `Double.parseDouble`. Every other level
    * is read as texts: a value is its own key. A value no parse reads is its own key too
    * (Elasticsearch fails on it).
    */
  def keyOf(value: String): String =
    if (!comparesNumbers) value
    else if (NumericReducer.wholeNumber(Bucket.columnType(identifier)))
      Try(java.lang.Long.parseLong(value))
        .orElse(
          Try(java.lang.Math.round(java.lang.Math.floor(java.lang.Double.parseDouble(value))))
        )
        .map(_.toString)
        .getOrElse(value)
    else Try(java.lang.Double.toString(java.lang.Double.parseDouble(value))).getOrElse(value)

  /** Whether this level is a numeric FIELD level of a proven kind, whose key filter values name
    * numbers ([[keyOf]]).
    */
  def comparesNumbers: Boolean =
    !isScriptedKey && isExactKeyKind && Bucket.columnType(identifier).isNumber

  /** The function of this level's key that formats a date column's RAW doc value (`DATE_FORMAT(d,
    * '%Y-%m')`, `DATETIME_FORMAT(ts, …)` over a mapped date column), which Elasticsearch 6.8 cannot
    * run: see [[app.softnetwork.elastic.sql.function.time.FormatsDateDocValue]].
    */
  lazy val rawDateDocValueFormat: Option[FormatsDateDocValue] =
    identifier.functions.collectFirst {
      case f: FormatsDateDocValue if f.formatsRawDateDocValue => f
    }

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

// Extends the function type the synthetic companion of a case class extends, and keeps its
// `toString`, so that declaring this companion leaves `BucketPath`'s binary shape as it was.
object BucketPath extends scala.runtime.AbstractFunction1[Seq[Bucket], BucketPath] {

  final override def toString: String = "BucketPath"

  /** The order a request EMITS a sequence of buckets in -- a GROUP BY's, or a window's PARTITION BY
    * -- which every path and every tree built from that sequence must follow: the root keys first,
    * then the nested ones, each part in its written order.
    *
    * The request nests every nested level inside the root buckets, never a root bucket inside a
    * nested one, so `GROUP BY i.qty, t.category` (an UNNEST key before a root key) is emitted as
    * `GROUP BY t.category, i.qty` is. Read in the WRITTEN order, the path of `MIN(i.qty)` named
    * `items.qty>category`, a node the request never carries: the aggregate was left out of the
    * request and answered NULL on every group, and a root aggregate lost the UNNEST bucket. In this
    * order the statement emits, and answers, what its root-first spelling does.
    *
    * The identity whenever no nested key precedes a root key. ONE function for every reader that
    * builds a path or a tree from a bucket sequence (`BucketPath.path`, `BucketTree.fromBuckets`);
    * the written GROUP BY (`GroupBy.sql`) keeps its order.
    */
  def emissionOrder(buckets: Seq[Bucket]): Seq[Bucket] = {
    val firstNested = buckets.indexWhere(_.nested)
    if (firstNested < 0 || buckets.drop(firstNested).forall(_.nested)) buckets
    else buckets.filterNot(_.nested) ++ buckets.filter(_.nested)
  }
}

case class BucketPath(buckets: Seq[Bucket]) {

  /** Every level's path, joined by `>`, exactly as `BucketNode.bucketPath` joins the levels of the
    * tree the request is built from: the bridges attach an aggregate to the level whose node path
    * EQUALS this one.
    *
    * 🔴 A level whose key is an expression has an EMPTY path (no field name), and it must keep its
    * place. The path used to skip an empty FIRST level, so `GROUP BY <CASE ...> AS kk, g1` gave
    * `COUNT(*)` the path `g1` while the leaf node was `>g1`: no level matched, the count was left
    * out of the request and every group answered NULL, with HTTP 200 -- with or without a HAVING.
    */
  lazy val path: String =
    BucketPath
      .emissionOrder(buckets)
      .map(_.path)
      .mkString(">")

  override def toString: String = path
}

/** The order and the cut the ROWS of a GROUP BY of several levels take once they are flattened, by
  * the names the rows carry them under -- what each level's own bucket order and size cannot give.
  *
  * A level orders, and is cut to the statement's LIMIT, INSIDE its parent bucket only. So:
  *   - `GROUP BY a, b LIMIT n` answered up to n rows per parent (n x n rows for two levels): the
  *     flattened rows are cut to n ([[limit]]);
  *   - `GROUP BY a, b ORDER BY b` (any ORDER BY that is not the levels' own keys in their order, an
  *     aggregate included) answered the rows ordered inside each `a` only: the flattened rows are
  *     sorted by the ORDER BY ([[sortKeys]]) and then cut. A NULL aggregate sorts where the same
  *     ORDER BY over one level puts it, the order Elasticsearch gives a group with no value: below
  *     every value for a `MAX` ([[lowNulls]], its -Infinity), above every value otherwise -- so last
  *     ascending and first descending, the engine's default (the statement cannot say otherwise
  *     under a GROUP BY). A level is then sent with every bucket (`Bucket.DefaultSize`): a LIMIT
  *     cut inside a parent would keep groups the global order does not.
  *
  * `LIMIT n OFFSET m` skips the first m groups, then answers n, as DuckDB does ([[offset]]): every
  * level is sent with n + m buckets (`Bucket.update`), and the rows -- in the order above, or in
  * the levels' own order -- drop m before they are cut to n. With one level too: its bucket order
  * is the statement's, only the skip is left to the rows.
  *
  * `None` when the levels already answer the rows as the statement asks: one level and no OFFSET,
  * or an ORDER BY that is the levels' keys in their order and no LIMIT or OFFSET. Also `None`, the
  * rows kept as they come, when a sort key is not a column of the rows (an aggregate the SELECT
  * does not project) -- and an OFFSET is then refused ([[offsetRefusal]]).
  */
case class GroupedRowOrder(
  sortKeys: Seq[(String, SortOrder)],
  limit: Option[Int],
  offset: Option[Int] = None,
  lowNulls: Set[String] = Set.empty
) {
  def sorts: Boolean = sortKeys.nonEmpty
}

object GroupedRowOrder {

  /** The OFFSET of `request`, when it skips at least one group. */
  private def offsetOf(request: SingleSearch): Option[Int] =
    request.limit.flatMap(_.offset).map(_.offset).filter(_ > 0)

  private def isMax(aggregation: SQLAggregation): Boolean = aggregation.aggType match {
    case MAX | _: MaxAgg => true
    case _               => false
  }

  def of(request: SingleSearch): Option[GroupedRowOrder] = {
    val buckets = BucketPath.emissionOrder(request.groupBy.map(_.buckets).getOrElse(Nil))
    val levels = buckets.filterNot(_.nested)
    // the groups of a key over an UNNEST element are flattened from inside each document: an
    // OFFSET is not skipped there (refused, `offsetRefusal`)
    val offset = offsetOf(request).filter(_ => levels.size == buckets.size)
    if (request.windowFunctions.exists(_.isWindowing)) None
    else if (levels.size < 2)
      offset
        .filter(_ => levels.nonEmpty)
        .map(m => GroupedRowOrder(Nil, request.limit.map(_.limit), Some(m)))
    else {
      val sorts = request.orderBy.map(_.sorts).getOrElse(Nil)
      def levelOf(sort: FieldSort): Int =
        levels.indexWhere(b => b.identifier.identifierName == sort.name || b.name == sort.name)
      val honoured = sorts.zipWithIndex.forall { case (sort, i) => levelOf(sort) == i }
      // a sort key as the rows carry it: (its column, its NULL below every value)
      def columnOf(sort: FieldSort): Option[(String, Boolean)] =
        levels
          .find(b => b.identifier.identifierName == sort.name || b.name == sort.name)
          .map(b => (b.name, false))
          .orElse(
            request.sqlAggregations.values
              .find(a => !a.auxiliary && (a.field == sort.name || a.aggName == sort.name))
              .map(a => (a.field, isMax(a)))
          )
      val limit = request.limit.map(_.limit)
      if (honoured)
        if (limit.isEmpty && offset.isEmpty) None else Some(GroupedRowOrder(Nil, limit, offset))
      else {
        val keys = sorts.map(sort => columnOf(sort).map(_ -> sort.direction))
        if (keys.exists(_.isEmpty)) None
        else {
          val resolved = keys.flatten
          Some(
            GroupedRowOrder(
              resolved.map { case ((column, _), direction) => column -> direction },
              limit,
              offset,
              lowNulls = resolved.collect { case ((column, true), _) => column }.toSet
            )
          )
        }
      }
    }
  }

  /** Why the OFFSET of a GROUP BY `request` cannot be honoured -- its rows are not skipped in the
    * order the statement asks for ([[of]]) -- or `None`: no GROUP BY, no OFFSET, or one the rows
    * skip.
    */
  def offsetRefusal(request: SingleSearch): Option[String] =
    if (request.groupBy.isEmpty || offsetOf(request).isEmpty) None
    else if (of(request).exists(_.offset.isDefined)) None
    else {
      val why =
        if (request.windowFunctions.exists(_.isWindowing)) "beside a window function"
        else if (request.groupBy.exists(_.buckets.exists(_.nested)))
          "over a GROUP BY key of an UNNEST element"
        else "with an ORDER BY over an aggregate the SELECT does not return"
      Some(
        s"OFFSET is not supported $why: the groups cannot be skipped in the order the statement " +
        "asks for. Remove the OFFSET, or page by key ranges."
      )
    }

  /** The size every level of `request` is sent with when its rows are sorted after they are
    * flattened: every bucket. `None` otherwise -- each level keeps its own (`Bucket.size`).
    */
  def levelSize(request: SingleSearch): Option[Int] =
    of(request).filter(_.sorts).map(_ => Bucket.DefaultSize)
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

  /** The `bucket_selector` source of a `HAVING` criterion that reads every aggregate exactly as
    * SELECT returns it, or `None` when there is nothing to filter. Throws exactly where
    * [[metricSelector]] does.
    *
    * 🔴 ONE function for every consumer (#292): the bridge's `metricSelectorForBucket` (both
    * bridges) and `Having.script` -- which a materialized view's transform `bucket_selector` is
    * built from -- read it, so a group filter and a view's filter cannot disagree about the value
    * of an empty group's aggregate.
    *
    * 🔴 Why [[selectorScript]] is not enough (F1). In a group where NO document has the aggregated
    * column, SELECT answers NULL for every aggregate of `ClientAggregation.nullOverEmptyInput`
    * (rule #336: `MIN`, `MAX`, `AVG`, the percentiles, the STDDEV / VARIANCE family, `FIRST_VALUE`
    * / `LAST_VALUE`, and `bucket_script` arithmetic over them), but a `bucket_selector` never
    * receives that NULL: under the default gap policy Elasticsearch hands it `NaN` in place of the
    * metric's `-Infinity` / `+Infinity` / `NaN` sentinel. So the `params.x == null` guards never
    * fired and every comparison ran on `NaN`. MEASURED on Elasticsearch 8.18.3: `MAX(v) <> 1`, `NOT
    * MAX(v) = 1`, `MAX(v) NOT IN (1, 2)` and `ISNOTNULL(MAX(v))` KEPT the group, `ISNULL(MAX(v))`
    * dropped it, and so did `COALESCE(MAX(v), 5) > 1`.
    *
    * Every read of such a metric therefore becomes [[nullAwareRead]]: NULL when the value is null,
    * `NaN` or infinite. Exact, not a heuristic: Elasticsearch 6.8 to 9.0 index only finite numbers,
    * so no group's real aggregate is `NaN` or infinite, and SELECT answers NULL for exactly those
    * values. COUNT and SUM are read as before -- 0 and `0.0` over no value, never NULL. Nothing is
    * added to the request: no hidden aggregation, and no `buckets_path` variable -- the read uses
    * the metric's own `params.<name>`, so a consumer that derives its `buckets_path` from the
    * clause (the materialized view's `extractAggregatePaths`) stays valid.
    *
    * Everything else is the existing rendering: a comparison's guard collapses the UNKNOWN to
    * `false` OUTSIDE its `NOT` (`<operands non-null> && [!](<comparison>)`), `ISNULL` / `ISNOTNULL`
    * test the value, and a `COALESCE`, a `GREATEST` or a `LEAST` is guarded on its result, never on
    * its arguments (`Expression.bucketPipelineGuard`). The read is substituted on the RENDERING,
    * after every rule has been decided on the unchanged one: the representability gate, the guards
    * and their placement are exactly those of [[selectorScript]], and a criterion with no such
    * aggregate is returned byte for byte.
    */
  def nullAwareSelectorScript(expr: Criteria): Option[String] =
    selectorScript(expr).map { script =>
      val nullable = expr.bucketMetrics.filter(nullOverEmptyInput).map(_.metricPathKey).toSet
      if (nullable.isEmpty) script else readAsSelectReturns(script, nullable)
    }

  /** How the selector reads the metric it is handed as `params.<name>` when SELECT answers NULL for
    * that metric over a group with no value: NULL when the value is null, `NaN` or infinite.
    *
    * The `(def)` cast is load-bearing: without it Painless types the conditional from its position,
    * and `Math.max((... ? null : params.max_v), 0)` fails to compile (`Cannot cast null to a
    * primitive type [double]`, MEASURED). `Double.isNaN` and `Double.isInfinite` are whitelisted on
    * every supported major (6.8 to 9.0), and the null test comes first: unboxing a null throws.
    */
  private def nullAwareRead(name: String): String = {
    val param = s"params.$name"
    s"((def) ($param == null || Double.isNaN($param) || Double.isInfinite($param) ? null : $param))"
  }

  /** Does SELECT answer NULL for this metric over a group with none of its values? F1's list: the
    * rule of `ClientAggregation.nullOverEmptyInput`, restated over the parsed aggregate because
    * this module cannot see the client one -- the core suite asserts that the two agree for every
    * aggregation type.
    */
  private[query] def nullOverEmptyInput(metric: Identifier): Boolean =
    metric.aggregateFunction match {
      case Some(af) => nullOverEmptyInput(af)
      // `Identifier.bucketMetrics`' second arm: the alias of a SELECT `bucket_script` item,
      // arithmetic over aggregates -- NULL whenever an operand is
      case None => metric.hasAggregation
    }

  /** Deliberately a total match, with no default arm: a new aggregate must be classified here. */
  private[query] def nullOverEmptyInput(af: AggregateFunction): Boolean = af match {
    // 0 and 0.0 over no value (#336), never NULL
    case COUNT | SUM | _: CountAgg | _: SumAgg => false
    // multi-valued: no comparison reads them
    case _: ArrayAgg | _: RankingWindow                      => false
    case MIN | MAX | AVG | _: MinAgg | _: MaxAgg | _: AvgAgg => true
    case STDDEV | STDDEV_POP | STDDEV_SAMP | VARIANCE | VAR_POP | VAR_SAMP | _: ExtendedStatsAgg =>
      true
    case PERCENTILE_CONT | PERCENTILE_DISC | _: PercentileAgg => true
    case _: FirstValue | _: LastValue                         => true
    case _: BucketScriptAggregation                           => true
  }

  /** `script` with every read of a `nullable` metric turned into its [[nullAwareRead]]. String
    * literals are skipped.
    */
  private def readAsSelectReturns(script: String, nullable: Set[String]): String = {
    val out = new java.lang.StringBuilder(script.length * 2)
    var last = 0
    MetricRead.findAllMatchIn(blankStringLiterals(script)).foreach { m =>
      if (nullable.contains(m.group(1))) {
        out.append(script, last, m.start).append(nullAwareRead(m.group(1)))
        last = m.end
      }
    }
    out.append(script, last, script.length).toString
  }

  /** One `params.<name>` read, the name taken whole. */
  private val MetricRead: Regex = """(?<![\w.$])params\.([A-Za-z_][A-Za-z0-9_]*)(?![\w$])""".r

  /** The metric names a bucket-pipeline `script` reads, string literals skipped -- every
    * `params.<name>` but `__now__`, the opaque script parameter a temporal literal is rendered
    * against (see the bridge's `metricSelectorForBucket`).
    */
  private[query] def metricsRead(script: String): Set[String] =
    paramsRead(script).toSet - "__now__"

  /** EVERY parameter a bucket-pipeline `script` reads (`params.<name>`, string literals skipped),
    * in the order it first reads them: the metrics its `buckets_path` binds AND the script
    * parameters its caller binds, such as `__now__` (`CURRENT_DATE`, `NOW()`, ...).
    */
  private[query] def paramsRead(script: String): Seq[String] =
    MetricRead.findAllMatchIn(blankStringLiterals(script)).map(_.group(1)).toList.distinct

  /** The bucket-pipeline script of a `HAVING` criterion, or `"1 == 1"` when there is nothing to
    * filter at this level: [[nullAwareSelectorScript]] with its placeholder, so it reads every
    * aggregate exactly as the bridge's `bucket_selector` and `Having.script` do (#292).
    *
    * 🔴 It THROWS on an [[MetricSelector.Unrepresentable]] criterion, and that is the point:
    * returning `""` is what let a predicate vanish. `Having.validate()` refuses every such
    * statement inside `Parser.apply`, so no venue can reach this throw with a parsed statement --
    * it is the invariant's second line of defence, for a `SingleSearch` assembled in code and
    * emitted without validation.
    */
  def metricSelector(expr: Criteria): String = nullAwareSelectorScript(expr).getOrElse("1 == 1")

  /** The selector script BEFORE the null-aware read [[nullAwareSelectorScript]] substitutes into
    * it, or `None` when there is nothing to filter at this level. Throws exactly where
    * [[metricSelector]] does.
    *
    * 🔴 The emission must never look for the placeholder INSIDE a script. It used to strip it with
    * `replaceAll("1 == 1", "")`, and a rendered comparison can hold that very text: `MAX(c1) = 1`
    * renders `params.max_c1 == 1`, which the strip turned into `params.max_c` -- a parameter no
    * aggregation publishes, and the search failed -- and `MAX(c1) = 10` into `params.max_c0`.
    */
  def selectorScript(expr: Criteria): Option[String] = selector(expr) match {
    case NoFilter           => None
    case Filter(script)     => Some(script)
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
          //
          // The script must evaluate the TREE. Painless, like SQL, binds `&&` tighter than `||`, so
          // an operand joined by OR under an AND is parenthesised whatever its `group` flag -- a
          // written group renders `(a) || (b)` for itself and nothing around it, and its parent used
          // to read `(a) || (b) && c` as `a || (b && c)`.
          val l = if (PredicatePrecedence.orUnderAnd(left, op)) s"($leftStr)" else leftStr
          val r =
            if (PredicatePrecedence.orUnderAnd(effectiveRight, op)) s"($rightStr)" else rightStr
          Filter(maybeNot match {
            case Some(_) if right.negated.isDefined => s"($leftStr) $opStr $rightStr"
            // Grammar-unreachable today (`NOT (A AND B)` in HAVING is a parse rejection); kept
            // as the total fallback for a compound right side.
            case Some(_)       => s"($leftStr) $opStr !($rightStr)"
            case None if group => s"($leftStr) $opStr ($rightStr)"
            case None          => s"$l $opStr $r"
          })
      }

    case relation: ElasticRelation => selector(relation.criteria)

    case _: MultiMatchCriteria => NoFilter

    // The context-free rendering of an aggregate predicate IS the bucket-pipeline rendering
    // (`Expression.bucketPipelinePainless`): `params.<metric>` reads, null-guarded, one
    // parenthesised expression, temporal literal already converted to epoch millis.
    //
    // 🔴 It goes through the SAME gate as the arm below, and that is not symmetry for its own sake.
    // This arm keys on the LEFT operand being an aggregate and says nothing about the RIGHT one, so
    // every disqualifier refused below was EMITTED here. MEASURED and EXECUTED on Elasticsearch
    // 8.18.3, all four with a bare `COUNT(*)` on the left:
    //   `> ROUND(MAX(a), 2)`      -> `invalid sequence of tokens near ['def']`   HTTP 400
    //   `> NULLIF(MAX(a), 0)`     -> `Cannot cast from [boolean] to [int]`       HTTP 400
    //   `BETWEEN 1 AND ABS(MAX(a))` -> `Unknown call [ABS] with [1] arguments`   HTTP 400
    //                                  (the BOUND leaked RAW SQL into the Painless source)
    //   `> CASE WHEN MAX(a) > 1 …`  -> `painless(None)` THREW inside `validate()`, surfacing
    //                                  `Internal parser error: …` to the user (issue #250's family)
    case e: Expression if e.isAggregation || e.referencesBucketMetric =>
      representable(e) match {
        case Left(reason)  => Unrepresentable(e.sql, reason)
        case Right(script) => Filter(script)
      }

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
    Try(groupFilter.withValue(true)(e.functionBucketPipelinePainless)).toOption match {
      case None =>
        Left("it cannot be rendered without a document, and a bucket pipeline has none")
      case Some(rendering) => disqualifyRendering(rendering).toLeft(rendering)
    }

  /** `true` while [[representable]] renders a `HAVING` leaf -- the ONE rendering every group filter
    * is built from (both bridges' `bucket_selector`, `Having.script`, the view rules) and the
    * verdict is taken on -- and nothing else.
    *
    * Read by `GREATEST` / `LEAST` ([[rendersGroupFilter]]), which skip a NULL argument and must
    * know that a metric argument can be one HERE: a group filter reads every aggregate as SELECT
    * returns it, NULL over a group with none of its values ([[nullAwareRead]]). The SELECT list's
    * `bucket_script` renders the same function context-free, and there the gap policy skips such a
    * group before the script runs, so its script does not move.
    *
    * A thread-scoped carrier, as `Having.aliasScope`, for the same reason: the function sits at any
    * depth of an operand and is reached only through the rendering of every function of the
    * dialect, which takes nothing but its context. Rendering is synchronous.
    */
  private[this] val groupFilter: DynamicVariable[Boolean] = new DynamicVariable[Boolean](false)

  /** Is a group filter being rendered ([[groupFilter]])? */
  private[sql] def rendersGroupFilter: Boolean = groupFilter.value

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
    else if (code.contains(".compareTo(\"") && code.contains("params."))
      // 🔴 A metric arrives from `buckets_path` as a NUMBER -- a date metric as epoch millis -- so a
      // rendering that compares it as a STRING cannot run. MEASURED on Elasticsearch 8.18.3:
      // `HAVING COALESCE(MAX(d), '2020-01-01') > '2019-01-01'` renders
      // `(params.max_d != null ? params.max_d : "2020-01-01").compareTo("2019-01-01") > 0` and
      // fails with `cannot explicitly cast def [java.lang.String] to java.lang.Double`.
      //
      // The RENDERER defect is pre-existing and wider than this issue -- the bare
      // `HAVING MAX(d) > '2019-01-01'` renders byte-identically on the control and fails the same
      // way -- but a raw Painless class-cast reaching the user is not "refused by name", so the
      // shape is refused here until the rendering is fixed.
      Some(
        "its rendering compares a metric as text, and a bucket pipeline reads every metric as a " +
        "number (a date metric as epoch millis)"
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

    // each path in the order the request emits it, as `BucketPath.path` builds it
    val validBuckets = buckets.filter(_.nonEmpty).map(BucketPath.emissionOrder)

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

/** The aggregate shapes Elasticsearch cannot answer as SQL does, named so that the statement's
  * validation refuses them before any request (`SingleSearch.validate`), instead of answering a
  * different number.
  */
object UnanswerableAggregate {

  /** The aggregates of `request` -- in its SELECT, its HAVING, its ORDER BY, or inside a per-group
    * calculation -- with every identifier they hold, as the request reads them
    * (`FunctionUtils.funIdentifiers`, the walk the calculation's operands are found by).
    */
  private def identifiersOf(request: SingleSearch): Seq[Identifier] =
    (request.select.fields ++ request.aggregates)
      .map(_.identifier)
      .flatMap(id => FunctionUtils.funIdentifiers(id))
      .distinct

  /** Does Elasticsearch answer something else than `af(DISTINCT x)` once the DISTINCT is dropped?
    * Every aggregation but a count is computed over every value: a COUNT is a `cardinality` and an
    * ARRAY_AGG keeps each value once, so they honour it; MIN, MAX, FIRST_VALUE and LAST_VALUE
    * answer the same with or without it (DuckDB 1.5.5 agrees on each, MEASURED).
    *
    * Deliberately a total match, with no default arm: a new aggregate must be classified here.
    */
  private def dropsDistinct(af: AggregateFunction): Boolean = af match {
    case COUNT | _: CountAgg | _: ArrayAgg             => false
    case MIN | MAX | _: MinAgg | _: MaxAgg             => false
    case _: FirstValue | _: LastValue                  => false
    case _: RankingWindow | _: BucketScriptAggregation => false
    case AVG | SUM | _: AvgAgg | _: SumAgg             => true
    case STDDEV | STDDEV_POP | STDDEV_SAMP | VARIANCE | VAR_POP | VAR_SAMP | _: ExtendedStatsAgg =>
      true
    case PERCENTILE_CONT | PERCENTILE_DISC | _: PercentileAgg => true
  }

  /** `AVG(DISTINCT x)` and every aggregate whose DISTINCT Elasticsearch would drop
    * ([[dropsDistinct]]): `SUM`, the STDDEV / VARIANCE family and the percentiles, plain or over a
    * window. Each was computed over every value, as if DISTINCT were not written, on every release.
    */
  def droppedDistinct(request: SingleSearch): Option[String] =
    identifiersOf(request)
      .find(id => id.distinct && id.aggregateFunction.exists(dropsDistinct))
      .map { id =>
        s"${id.sql} is not supported: Elasticsearch computes this aggregate over every value of " +
        "its column, not over its distinct values, so DISTINCT would be ignored. Only " +
        "COUNT(DISTINCT ...) counts distinct values (MIN and MAX answer the same with or without " +
        "DISTINCT). Remove DISTINCT, or compute the aggregate over the distinct values outside " +
        "the query."
      }

  /** The `GREATEST` / `LEAST` calls of `f`, at any depth. */
  private def reducersIn(f: Function, depth: Int = 0): Seq[NumericReducer] =
    if (depth > 32) Nil
    else
      f match {
        case r: NumericReducer =>
          r +: r.values.collect { case g: Function => g }.flatMap(reducersIn(_, depth + 1))
        case id: Identifier => id.functions.flatMap(reducersIn(_, depth + 1))
        case fn: FunctionN[_, _] =>
          fn.args.collect { case g: Function => g }.flatMap(reducersIn(_, depth + 1))
        case fc: FunctionChain           => fc.functions.flatMap(reducersIn(_, depth + 1))
        case fwi: FunctionWithIdentifier => reducersIn(fwi.identifier, depth + 1)
        case _                           => Nil
      }

  /** The first aggregate an argument of `reducer` reads that is NULL in a group with no value of
    * its column (`MetricSelectorScript.nullOverEmptyInput`: MIN, MAX, AVG, the STDDEV / VARIANCE
    * family, the percentiles). A COUNT or a SUM is never NULL there (0 and 0.0 over no value).
    */
  private def nullableArgument(reducer: NumericReducer): Option[Identifier] =
    reducer.values.iterator
      .collect { case g: Function => g }
      .flatMap(g => FunctionUtils.funIdentifiers(g))
      .find(_.aggregateFunction.exists {
        // an aggregate over a window is computed apart, never by the group's calculation; an
        // argument `MAX(n)` parses as a window function with no window (`MaxAgg`), and is one of
        // the group's aggregates
        case w: WindowFunction if w.isWindowing => false
        case af                                 => MetricSelectorScript.nullOverEmptyInput(af)
      })

  /** `GREATEST` / `LEAST` over an aggregate that can be NULL, in a calculation over aggregates of
    * the SELECT -- computed per group, or in the one whole-table group, by a `bucket_script`.
    *
    * Elasticsearch skips the calculation of a group where one of its operands has no value (its gap
    * policy, `keep_values` included -- MEASURED on 8.18.3), so the group answered NULL, while
    * GREATEST and LEAST skip a NULL argument: `GREATEST(MAX(n), 2)` is 2 in DuckDB 1.5.5 where `n`
    * has no value. Every other calculation is NULL when one of its operands is, as SQL is, and
    * stays answered; so does GREATEST over aggregates that cannot be NULL (a COUNT, a SUM), and in
    * a HAVING, which reads each aggregate as SELECT does and skips a NULL one.
    */
  def nullSkippingOverNullable(request: SingleSearch): Option[String] =
    request.select.fields.iterator
      .filter(_.isBucketScript)
      .flatMap { field =>
        reducersIn(field.identifier).iterator.flatMap(r =>
          nullableArgument(r).map(metric => (field, r, metric))
        )
      }
      .toSeq
      .headOption
      .map { case (field, reducer, metric) =>
        val name = reducer.operator.sql
        val wins = if (name == "GREATEST") "largest" else "smallest"
        s"$name over ${metric.sql} is not supported in a calculation over aggregates " +
        s"(${field.identifier.sql}): ${metric.sql} is NULL in a group with no value of its " +
        "column, and Elasticsearch then skips the calculation, so the group would answer NULL " +
        s"where SQL answers the $wins of the other arguments. Select the aggregate on its own and " +
        s"apply $name to the result outside the query; in a HAVING, $name over aggregates is " +
        "answered."
      }
}
