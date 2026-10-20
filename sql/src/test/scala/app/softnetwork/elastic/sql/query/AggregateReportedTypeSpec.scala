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

import app.softnetwork.elastic.sql.{ComparisonRule, Identifier}
import app.softnetwork.elastic.sql.function.aggregate.AggregateFunction
import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.schema.{Column, Table => SchemaTable}
import app.softnetwork.elastic.sql.`type`.{SQLType, SQLTypeUtils, SQLTypes}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.util.Try

/** An aggregate REPORTS the type it produces, not the type of the column it aggregates.
  *
  * 🔴 `reportedType` is what a consumer that DECLARES a column reads: `Table.mergeWithSearch`
  * writes it into a CTAS target's `_meta.columns.<c>.data_type` and, through
  * `SQLTypeUtils.elasticType`, into its mapping; a materialized view declares its columns from it.
  * Since #395 an aggregate reported its LEAF, so `CREATE TABLE t2 AS SELECT COUNT(DISTINCT b) AS c
  * FROM t` declared `c` BOOLEAN and mapped it `boolean` (on ES 6.8 every row of it then failed to
  * index), `AVG(n)` an INTEGER, `VAR_POP(n)` an INTEGER. Comparisons and set operations already
  * read the right answer, `AggregateFunction.outputTypeOf`, through an arm of their own; now there
  * is one derivation (#292) and every consumer reads it.
  *
  * 🔴 The oracle is DuckDB 1.5.5's answer for each aggregate over each column kind, transcribed
  * here in core's type names (DuckDB's VARCHAR is KEYWORD / TEXT, its FLOAT is REAL, its DATETIME
  * is an alias of TIMESTAMP), and NOT derived from `outputTypeOf`. Each cell is one of:
  *
  *   - [[Pinned]]: DuckDB answers a type and core agrees, so the cell pins it;
  *   - [[Agreed]]: core's answer differs from DuckDB's for a PRE-EXISTING reason, or the type is an
  *     open decision; the comment names it, and the cell asserts only what holds whichever way it
  *     is resolved: every consumer reads the same type, plus the cell's own property;
  *   - [[Refused]]: DuckDB refuses the input (SUM of a string, AVG of a BOOLEAN, VAR of a date…).
  *     It is not part of the type rule, so the cell is left out.
  *
  * The matrix is every aggregate × every column kind, global and grouped; nested in a COALESCE, an
  * aggregate must read exactly like a COLUMN of the type it produces.
  */
class AggregateReportedTypeSpec extends AnyFlatSpec with Matchers {

  private sealed trait Family
  private case object Bool extends Family
  private case object WholeNumber extends Family
  private case object FractionalNumber extends Family
  private case object Characters extends Family
  private case object Temporal extends Family

  /** A column kind: its name in the schema, its SQL type, the Elasticsearch type a CTAS maps, and
    * the family that decides what DuckDB answers over it.
    */
  private case class Kind(name: String, sqlType: SQLType, es: String, family: Family)

  private val kinds: List[Kind] = List(
    Kind("b", SQLTypes.Boolean, "boolean", Bool),
    Kind("i8", SQLTypes.TinyInt, "byte", WholeNumber),
    Kind("i16", SQLTypes.SmallInt, "short", WholeNumber),
    Kind("n", SQLTypes.Int, "integer", WholeNumber),
    Kind("l", SQLTypes.BigInt, "long", WholeNumber),
    Kind("r", SQLTypes.Real, "float", FractionalNumber),
    Kind("x", SQLTypes.Double, "double", FractionalNumber),
    Kind("k", SQLTypes.Keyword, "keyword", Characters),
    Kind("t", SQLTypes.Text, "text", Characters),
    Kind("d", SQLTypes.Date, "date", Temporal),
    Kind("ts", SQLTypes.Timestamp, "date", Temporal),
    Kind("dt", SQLTypes.DateTime, "date", Temporal),
    Kind("tm", SQLTypes.Time, "date", Temporal)
  )

  private val schema: SchemaTable = SchemaTable(
    "t",
    columns = kinds.map(k => Column(k.name, k.sqlType)) ++ List(
      Column("g", SQLTypes.Keyword),
      Column("de", SQLTypes.Date)
    )
  )

  private def kind(name: String): Kind =
    kinds.find(_.name == name).getOrElse(fail(s"no column kind `$name`"))

  private def kindOf(sqlType: SQLType): Kind =
    kinds.find(_.sqlType == sqlType).getOrElse(fail(s"no column kind of type $sqlType"))

  private sealed trait Cell

  /** DuckDB answers this type, and core agrees: the cell pins it. */
  private final case class Pinned(sqlType: SQLType) extends Cell

  /** Core differs from DuckDB for the pre-existing reason `why`, or the type is an open decision:
    * the cell asserts only that every consumer reads one type, and that this type `holds`.
    */
  private final case class Agreed(why: String, holds: SQLType => Unit = _ => ()) extends Cell

  /** DuckDB refuses the input: it is not part of the type rule, so the cell is left out. */
  private case object Refused extends Cell

  private val wholeWidth: Map[SQLType, Int] =
    Map(SQLTypes.TinyInt -> 1, SQLTypes.SmallInt -> 2, SQLTypes.Int -> 3, SQLTypes.BigInt -> 4)

  /** A whole SUM stays whole, at least as wide as the summed column. */
  private def wholeAtLeast(column: SQLType): SQLType => Unit = produced =>
    withClue(s"a SUM of $column is $produced: ") {
      wholeWidth.get(produced).exists(_ >= wholeWidth(column)) shouldBe true
    }

  /** A fractional SUM stays fractional. */
  private val fractional: SQLType => Unit = produced =>
    withClue(s"a SUM of a fractional column is $produced: ") {
      Set[SQLType](SQLTypes.Real, SQLTypes.Double) should contain(produced)
    }

  // 🔴 Open: core keeps the summed column's width, DuckDB widens a whole sum to HUGEINT and a REAL
  // sum to DOUBLE. Which one core follows is still to be decided, so no cell pins a SUM's width.
  private val sumWidthIsOpen =
    "the width of a SUM is an open decision (core keeps the column's, DuckDB widens)"

  private val count: Kind => Cell = _ => Pinned(SQLTypes.BigInt)

  private val sum: Kind => Cell = k =>
    k.family match {
      case WholeNumber      => Agreed(sumWidthIsOpen, wholeAtLeast(k.sqlType))
      case FractionalNumber => Agreed(sumWidthIsOpen, fractional)
      // DuckDB counts the trues as a HUGEINT; core declares the DOUBLE Elasticsearch's sum answers
      case Bool => Agreed("SUM of a BOOLEAN: DuckDB HUGEINT, core DOUBLE")
      case _    => Refused
    }

  private val avg: Kind => Cell = k =>
    k.family match {
      case WholeNumber | FractionalNumber => Pinned(SQLTypes.Double)
      // DuckDB averages a temporal into a TIMESTAMP (a TIME for a TIME); core declares the DOUBLE it
      // answers, the averaged epoch millis Elasticsearch returns
      case Temporal => Agreed("AVG of a temporal: DuckDB TIMESTAMP / TIME, core DOUBLE")
      case _        => Refused
    }

  private val minMax: Kind => Cell = k => Pinned(k.sqlType)

  private val statistic: Kind => Cell = k =>
    k.family match {
      case WholeNumber | FractionalNumber => Pinned(SQLTypes.Double)
      case _                              => Refused
    }

  private val percentileCont: Kind => Cell = k =>
    k.family match {
      // DuckDB keeps a REAL (its FLOAT); core declares the DOUBLE Elasticsearch's percentiles compute
      case FractionalNumber if k.sqlType == SQLTypes.Real =>
        Agreed("PERCENTILE_CONT of a REAL: DuckDB FLOAT, core DOUBLE")
      case WholeNumber | FractionalNumber => Pinned(SQLTypes.Double)
      // DuckDB interpolates a temporal into a TIMESTAMP (a TIME for a TIME); core declares the
      // DOUBLE it answers, epoch millis
      case Temporal => Agreed("PERCENTILE_CONT of a temporal: DuckDB TIMESTAMP / TIME, core DOUBLE")
      case _        => Refused
    }

  // DuckDB answers the argument's type, a member of the set. Core computes PERCENTILE_DISC exactly
  // like PERCENTILE_CONT, with Elasticsearch's `percentiles` (TDigest, interpolated), so its value
  // is a Double that need not be a member of the set, and it declares DOUBLE. Pre-existing.
  private val percentileDisc: Kind => Cell = k =>
    if (k.sqlType == SQLTypes.Double) Pinned(SQLTypes.Double)
    else Agreed("PERCENTILE_DISC: DuckDB the argument's type, core DOUBLE")

  /** Every aggregate, with DuckDB's answer over each column kind. */
  private val aggregates: List[(String, Kind => Cell)] = List(
    "COUNT(%s)"                                       -> count,
    "COUNT(DISTINCT %s)"                              -> count,
    "SUM(%s)"                                         -> sum,
    "SUM(DISTINCT %s)"                                -> sum,
    "AVG(%s)"                                         -> avg,
    "AVG(DISTINCT %s)"                                -> avg,
    "MIN(%s)"                                         -> minMax,
    "MAX(%s)"                                         -> minMax,
    "VAR_POP(%s)"                                     -> statistic,
    "VAR_SAMP(%s)"                                    -> statistic,
    "VARIANCE(%s)"                                    -> statistic,
    "STDDEV(%s)"                                      -> statistic,
    "STDDEV_POP(%s)"                                  -> statistic,
    "STDDEV_SAMP(%s)"                                 -> statistic,
    "PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY %s)" -> percentileCont,
    "PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY %s)" -> percentileDisc
  )

  /** The statement, and the identifier of its column `a` resolved against `against`. */
  private def resolved(sql: String, against: SchemaTable = schema): (SingleSearch, Identifier) =
    Parser(sql) match {
      case Right(search: SingleSearch) =>
        val field = search
          .update(Some(against))
          .select
          .fields
          .find(_.fieldAlias.map(_.alias).contains("a"))
          .getOrElse(fail(s"no column `a` in [$sql]"))
        (search, field.identifier)
      case other => fail(s"[$sql] is not one SELECT: $other")
    }

  /** The column `a` of `CREATE TABLE … AS <search>`. */
  private def ctas(search: SingleSearch, against: SchemaTable = schema): Column =
    against
      .mergeWithSearch(search)
      .columns
      .find(_.name == "a")
      .getOrElse(fail(s"no CTAS column `a` for [${search.sql}]"))

  /** Every consumer reads one derivation: `outputTypeOf`, `reportedType`, `declaredTypeOf` and the
    * CTAS column agree. Returns that type and the CTAS column.
    */
  private def assertOneType(sql: String): (SQLType, Column) = {
    val (search, id) = resolved(sql)
    withClue(s"[$sql] ") {
      id.isAggregation shouldBe true
      val produced = AggregateFunction.outputTypeOf(id).getOrElse(fail("aggregates nothing"))
      id.reportedType shouldBe produced
      ComparisonRule.declaredTypeOf(id) shouldBe produced
      val column = ctas(search)
      column.dataType shouldBe produced
      (produced, column)
    }
  }

  private def assertCell(sql: String, cell: Cell): Unit =
    cell match {
      case Pinned(expected) =>
        val (produced, column) = assertOneType(sql)
        withClue(s"[$sql] ") {
          produced shouldBe expected
          SQLTypeUtils.elasticType(column.dataType) shouldBe kindOf(expected).es
        }
      case Agreed(_, holds) => holds(assertOneType(sql)._1)
      case Refused          => ()
    }

  /** An aggregate nested in a function reads like a COLUMN of the type it produces (the aggregate's
    * own type is the global cell's question; this one is about what a function over it sees).
    */
  private def assertNestsLikeAColumn(aggregate: String): Unit = {
    val produced = AggregateFunction
      .outputTypeOf(resolved(s"SELECT $aggregate AS a FROM t")._2)
      .getOrElse(fail(s"$aggregate aggregates nothing"))
    val twinSchema = schema.copy(columns = schema.columns :+ Column("z", produced))
    val sql = s"SELECT COALESCE($aggregate, 0) AS a FROM t"
    val twinSql = "SELECT COALESCE(z, 0) AS a FROM t"
    val (search, id) = resolved(sql)
    val (twinSearch, twin) = resolved(twinSql, twinSchema)
    withClue(s"[$sql] vs [$twinSql] over z $produced ") {
      id.reportedType shouldBe twin.reportedType
      ComparisonRule.declaredTypeOf(id) shouldBe id.reportedType
      ctas(search).dataType shouldBe ctas(twinSearch, twinSchema).dataType
    }
  }

  /** Runs one check per column kind DuckDB answers over, and fails once, naming every kind that
    * failed.
    */
  private def overEveryKind(cellOf: Kind => Cell)(check: (Kind, Cell) => Unit): Unit = {
    val asserted = kinds.filterNot(k => cellOf(k) == Refused)
    val failed = asserted.flatMap { k =>
      Try(check(k, cellOf(k))).failed.toOption.map(e =>
        s"${k.name} (${k.sqlType}): ${e.getMessage}"
      )
    }
    withClue(
      s"${failed.size} of ${asserted.size} column kinds " +
      s"(${kinds.size - asserted.size} refused by DuckDB, left out): ${failed.mkString(" | ")}"
    ) {
      failed shouldBe empty
    }
  }

  behavior of "an aggregate's reported type"

  for ((aggregate, cellOf) <- aggregates) {
    it should s"be the type ${aggregate.format("<c>")} produces, over every column kind (global)" in {
      overEveryKind(cellOf) { (k, cell) =>
        assertCell(s"SELECT ${aggregate.format(k.name)} AS a FROM t", cell)
      }
    }

    it should s"be the type ${aggregate.format("<c>")} produces, over every column kind (grouped)" in {
      overEveryKind(cellOf) { (k, cell) =>
        assertCell(s"SELECT g, ${aggregate.format(k.name)} AS a FROM t GROUP BY g", cell)
      }
    }

    it should s"read ${aggregate.format("<c>")} in a COALESCE like a column of the type it produces" in {
      overEveryKind(cellOf) { (k, _) =>
        assertNestsLikeAColumn(aggregate.format(k.name))
      }
    }
  }

  private val windows: List[(String, Cell)] = List(
    "SELECT COUNT(n) OVER (PARTITION BY g) AS a FROM t" -> Pinned(SQLTypes.BigInt),
    "SELECT SUM(i8) OVER (PARTITION BY g) AS a FROM t" -> Agreed(
      sumWidthIsOpen,
      wholeAtLeast(SQLTypes.TinyInt)
    ),
    "SELECT AVG(n) OVER (PARTITION BY g) AS a FROM t"                    -> Pinned(SQLTypes.Double),
    "SELECT MAX(n) OVER (PARTITION BY g) AS a FROM t"                    -> Pinned(SQLTypes.Int),
    "SELECT FIRST_VALUE(d) OVER (PARTITION BY g ORDER BY n) AS a FROM t" -> Pinned(SQLTypes.Date),
    "SELECT LAST_VALUE(k) OVER (PARTITION BY g ORDER BY n) AS a FROM t" -> Pinned(
      SQLTypes.Keyword
    ),
    // DuckDB answers VARCHAR[], a list; core reports the element's type (which is also how
    // Elasticsearch maps an array). Pre-existing.
    "SELECT ARRAY_AGG(k) OVER (PARTITION BY g) AS a FROM t" -> Agreed(
      "ARRAY_AGG: DuckDB VARCHAR[], core the element type"
    ),
    "SELECT ROW_NUMBER() OVER (PARTITION BY g ORDER BY n) AS a FROM t" -> Pinned(SQLTypes.BigInt),
    "SELECT RANK() OVER (PARTITION BY g ORDER BY n) AS a FROM t"       -> Pinned(SQLTypes.BigInt)
  )

  for ((sql, cell) <- windows) {
    it should s"be the type a window produces: $sql" in {
      assertCell(sql, cell)
    }
  }

  behavior of "an aggregate's reported type, the named cases"

  // Not pinned to a value: DuckDB answers INTEGER here, the argument's type. Core computes
  // PERCENTILE_DISC like PERCENTILE_CONT, with Elasticsearch's `percentiles` (TDigest,
  // interpolated), so its value is a Double that need not be a member of the set. That disagreement
  // is pre-existing; what holds either way is one type in every consumer.
  it should "declare PERCENTILE_DISC with one type in every consumer" in {
    assertCell(
      "SELECT PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY n) AS a FROM t",
      percentileDisc(kind("n"))
    )
  }

  // Not pinned to a value: DuckDB averages a DATE into a TIMESTAMP, while Elasticsearch averages a
  // date field as epoch millis and core answers that number. Pre-existing; one type either way.
  it should "declare AVG over a DATE with one type in every consumer" in {
    assertCell("SELECT AVG(d) AS a FROM t", avg(kind("d")))
  }

  // MIN / MAX return one of the values they aggregate, so they report what their chain reports.
  // Those functions report NUMERIC in core, where DuckDB answers BIGINT (DOUBLE for ROUND): that is
  // the functions' own type, pre-existing, so this is an equality and pins no value.
  it should "declare MIN / MAX over YEAR, MONTH, EXTRACT, ROUND or DATEDIFF as that function" in {
    for (
      (aggregate, chain) <- List(
        "MAX(YEAR(d))"              -> "YEAR(d)",
        "MIN(MONTH(d))"             -> "MONTH(d)",
        "MAX(EXTRACT(YEAR FROM d))" -> "EXTRACT(YEAR FROM d)",
        "MAX(ROUND(x, 2))"          -> "ROUND(x, 2)",
        "MAX(DATEDIFF(d, de))"      -> "DATEDIFF(d, de)"
      )
    ) {
      val (produced, column) = assertOneType(s"SELECT $aggregate AS a FROM t")
      val (chainSearch, chainId) = resolved(s"SELECT $chain AS a FROM t")
      withClue(s"[$aggregate] vs [$chain] ") {
        produced shouldBe chainId.reportedType
        column.dataType shouldBe ctas(chainSearch).dataType
      }
    }
  }

  // Not pinned to a width (open decision, see `sumWidthIsOpen`): what holds either way is that a
  // whole SUM stays whole and at least as wide as its column, and a fractional SUM stays fractional.
  it should "keep a SUM whole over a whole column and fractional over a fractional one" in {
    for (name <- List("i8", "i16", "n", "l", "r", "x"))
      assertCell(s"SELECT SUM($name) AS a FROM t", sum(kind(name)))
  }

  // COALESCE takes the super type of what its arms REPORT, so the variance arm must report DOUBLE
  // (it reported TINYINT, and the COALESCE a BIGINT). DuckDB: DOUBLE.
  it should "declare COALESCE(VAR_POP(i8), 0) a DOUBLE" in {
    val (search, id) = resolved("SELECT COALESCE(VAR_POP(i8), 0) AS a FROM t")
    id.reportedType shouldBe SQLTypes.Double
    ComparisonRule.declaredTypeOf(id) shouldBe SQLTypes.Double
    ctas(search).dataType shouldBe SQLTypes.Double
  }

  // NULLIF answers its first argument's type, which is the count's BIGINT, not the counted
  // column's BOOLEAN. DuckDB: BIGINT.
  it should "declare NULLIF(COUNT(DISTINCT b), 0) a BIGINT" in {
    val (search, id) = resolved("SELECT NULLIF(COUNT(DISTINCT b), 0) AS a FROM t")
    id.reportedType shouldBe SQLTypes.BigInt
    ComparisonRule.declaredTypeOf(id) shouldBe SQLTypes.BigInt
    ctas(search).dataType shouldBe SQLTypes.BigInt
  }
}
