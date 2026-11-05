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

import app.softnetwork.elastic.sql.{PainlessContext, PainlessContextType}
import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.schema.{Column, Table => SchemaTable}
import app.softnetwork.elastic.sql.`type`.{SQLType, SQLTypeUtils, SQLTypes}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.util.Try

/** SCHEMA-6 (D7, A-27) -- an `ARRAY<X>` column used where Elasticsearch works per ELEMENT reads as
  * its element X: a term or range query matches any element, a metric aggregates the elements, a
  * terms aggregation buckets them, and Painless reads ONE (`doc['f'].value`).
  *
  * The oracle is a TWIN: the same statement over a column of the scalar type X. Over every P2
  * statement and every element type, the `ARRAY<X>` column must get the twin's verdict, the twin's
  * Painless (projection and predicate), and -- for every item that is not the bare column itself (a
  * computed item, an aggregate, a GROUP BY key) -- the twin's type. So no computed item over a list
  * column reports `ARRAY` (which would make the scripted reader wrap ONE element in a list:
  * `NULLIF(arr, 0)` on `[5, 9]` answering `List(5)`), and no comparison is refused for the column
  * being a list.
  *
  * On 0.24.0 every array column read `ANY`; typing it `ARRAY<X>` without this view refused or
  * re-typed exactly these cells.
  */
class ElementViewTwinSpec extends AnyFlatSpec with Matchers {

  /** `extra`: items typed from their operand outside the shared argument helpers -- `ABS` and
    * `DATE_ADD` / `DATE_SUB` read their operand's own type, so each needs its element read too.
    */
  private final case class Element(
    x: SQLType,
    v: String,
    w: String,
    fn: String,
    arith: Option[String],
    extra: Seq[String] = Nil
  )

  /** SCHEMA-5's operand-typed numeric sites, read through the element too: ROUND takes its
    * operand's type (to tens, hundreds, … a narrower integer is a BIGINT, a BIGINT tests its
    * range), FLOOR and CEIL are BIGINT, and every consumer of ROUND's type follows (a metric, a
    * conditional, arithmetic, a comparison whose quoted number is read beside it).
    */
  private val rounding: Seq[String] = Seq(
    "ROUND(c)",
    "ROUND(c, 1)",
    "ROUND(c, -1)",
    "ROUND(c, -2)",
    "FLOOR(c)",
    "CEIL(c)",
    "MAX(ROUND(c, -1))",
    "SUM(ROUND(c, -1))",
    "MIN(ROUND(c, 1))",
    "ROUND(c, -1) + 1",
    "COALESCE(ROUND(c, -1), 0)",
    "NULLIF(ROUND(c, -1), 10)",
    "CASE WHEN b THEN ROUND(c, -1) ELSE 0 END"
  )

  private val elements: Seq[Element] = Seq(
    Element(SQLTypes.Int, "1", "5", "ABS(c)", Some("c + 1"), rounding),
    Element(SQLTypes.BigInt, "1", "5", "ABS(c)", Some("c + 1"), rounding),
    Element(SQLTypes.TinyInt, "1", "5", "ABS(c)", Some("c + 1"), rounding),
    Element(SQLTypes.Double, "1.5", "9.5", "ABS(c)", Some("c + 1"), rounding),
    Element(SQLTypes.Real, "1.5", "9.5", "ABS(c)", Some("c + 1"), rounding),
    Element(SQLTypes.Keyword, "'a'", "'z'", "UPPER(c)", None),
    Element(
      SQLTypes.Date,
      "'2024-01-01'",
      "'2024-12-31'",
      "YEAR(c)",
      Some("c + 1"),
      Seq(
        "DATE_ADD(c, INTERVAL 1 DAY)",
        "DATE_SUB(c, INTERVAL 1 DAY)",
        // SCHEMA-5: DATE_TRUNC over a date reports TIMESTAMP in a query, DATEDIFF a BIGINT
        "DATE_TRUNC(c, DAY)",
        "DATEDIFF(c, '2024-01-01')",
        "c + DATEDIFF(c, '2024-01-01')"
      )
    ),
    Element(SQLTypes.Boolean, "TRUE", "FALSE", "CAST(c AS VARCHAR)", None)
  )

  private def statements(e: Element): Seq[String] =
    Seq(
      s"SELECT g FROM t WHERE c = ${e.v}",
      s"SELECT g FROM t WHERE c IN (${e.v}, ${e.w})",
      s"SELECT g FROM t WHERE c BETWEEN ${e.v} AND ${e.w}",
      s"SELECT g FROM t WHERE c > ${e.v}",
      "SELECT g FROM t WHERE c LIKE 'a%'",
      "SELECT g FROM t WHERE c IS NULL",
      s"SELECT g FROM t WHERE COALESCE(c, c) = ${e.v}",
      s"SELECT ${e.fn} AS r FROM t",
      "SELECT CAST(c AS VARCHAR) AS r FROM t",
      "SELECT c, COUNT(*) AS n FROM t GROUP BY c",
      "SELECT g FROM t ORDER BY c",
      "SELECT MIN(c) AS r FROM t",
      "SELECT MAX(c) AS r FROM t",
      "SELECT SUM(c) AS r FROM t",
      "SELECT COUNT(c) AS r FROM t",
      "SELECT g, MAX(c) AS r FROM t GROUP BY g",
      s"SELECT NULLIF(c, ${e.v}) AS r FROM t",
      "SELECT COALESCE(c, c) AS r FROM t",
      "SELECT CASE WHEN b THEN c END AS r FROM t"
    ) ++ e.arith.map(a => s"SELECT $a AS r FROM t") ++ e.extra.map(x => s"SELECT $x AS r FROM t")

  private def schema(c: SQLType): SchemaTable =
    SchemaTable(
      "t",
      columns = List(Column("c", c), Column("b", SQLTypes.Boolean), Column("g", SQLTypes.Keyword))
    )

  private final case class Outcome(
    accepted: Boolean,
    painless: Seq[Option[String]],
    types: Seq[Option[MultiSearch.SetOperationType]],
    reported: Seq[Option[SQLType]]
  )

  private def outcome(sql: String, against: SchemaTable): Outcome =
    Parser(sql) match {
      case Right(single: SingleSearch) =>
        val resolved = single.update(Some(against))
        val accepted = resolved.validateResolved().isRight
        val fields = resolved.select.fieldsWithComputedAliases
        // the bare column item is declared `ARRAY<X>` on purpose; every other item is compared
        def compared(f: Field): Boolean =
          f.identifier.functions.nonEmpty || f.isAggregation ||
          resolved.groupBy.exists(
            _.buckets.exists(_.identifier.identifierName == f.identifier.identifierName)
          )
        val painless = fields.map { f =>
          Try {
            val ctx = PainlessContext(PainlessContextType.Query)
            val body = f.painless(Some(ctx))
            s"$ctx$body"
          }.toOption
        } ++ Seq(
          resolved.where.flatMap(_.criteria).flatMap { criteria =>
            Try {
              val ctx = PainlessContext(PainlessContextType.Query)
              val body = criteria.painless(Some(ctx))
              s"$ctx$body"
            }.toOption
          }
        )
        val types =
          fields.map(f => if (compared(f)) Some(MultiSearch.fieldType(f, resolved)) else None)
        // a GROUP BY key keeps its column's declared type as its REPORTED type (CTAS declares it,
        // A-27); its position is typed by its key ([[MultiSearch.fieldType]], compared above)
        def groupKey(f: Field): Boolean =
          !f.isAggregation && f.identifier.functions.isEmpty &&
          resolved.groupBy.exists(
            _.buckets.exists(_.identifier.identifierName == f.identifier.identifierName)
          )
        val reported =
          fields.map(f =>
            if (compared(f) && !groupKey(f)) Some(f.identifier.reportedType) else None
          )
        Outcome(accepted, painless, types, reported)
      case Right(other) => fail(s"[$sql] is not one SELECT: $other")
      case Left(error)  => Outcome(accepted = false, Nil, Nil, Nil)
    }

  behavior of "an ARRAY<X> column used per element"

  for (e <- elements) {
    it should s"read as its twin X column for X = ${e.x.typeId} (D7, A-27)" in {
      val failures = statements(e).flatMap { sql =>
        val list = outcome(sql, schema(SQLTypes.Array(e.x)))
        val twin = outcome(sql, schema(e.x))
        Seq(
          if (list.accepted != twin.accepted)
            Some(s"[$sql] verdict: ARRAY ${list.accepted}, twin ${twin.accepted}")
          else None,
          if (list.painless != twin.painless)
            Some(s"[$sql] Painless: ARRAY ${list.painless}, twin ${twin.painless}")
          else None,
          if (list.types != twin.types)
            Some(s"[$sql] type: ARRAY ${list.types}, twin ${twin.types}")
          else None,
          if (list.reported != twin.reported)
            Some(s"[$sql] reported: ARRAY ${list.reported}, twin ${twin.reported}")
          else None
        ).flatten
      }
      withClue(
        s"${failures.size} differences over ${statements(e).size} statements:\n" +
        failures.mkString("\n")
      ) {
        failures shouldBe empty
      }
    }
  }

  it should "keep the declared ARRAY<X> where a column is DECLARED: the bare item, CTAS, FIRST_VALUE" in {
    val t = schema(SQLTypes.Array(SQLTypes.Int))
    val bare = Parser("SELECT c FROM t") match {
      case Right(s: SingleSearch) => s.update(Some(t))
      case other                  => fail(s"$other")
    }
    MultiSearch.fieldType(bare.select.fields.head, bare).sqlType shouldBe
    SQLTypes.Array(SQLTypes.Int)
    t.mergeWithSearch(bare).columns.find(_.name == "c").map(_.dataType) shouldBe
    Some(SQLTypes.Array(SQLTypes.Int))
    val first = Parser("SELECT FIRST_VALUE(c) OVER (PARTITION BY g ORDER BY b) AS r FROM t") match {
      case Right(s: SingleSearch) => s.update(Some(t))
      case other                  => fail(s"$other")
    }
    first.select.fields.head.identifier.reportedType shouldBe SQLTypes.Array(SQLTypes.Int)
  }

  it should "type a window PARTITION BY over an ARRAY<X> column as 0.24.0 read it (A-28)" in {
    val t = schema(SQLTypes.Array(SQLTypes.Int))
    val windowed = Parser("SELECT g, COUNT(*) OVER (PARTITION BY c) AS n FROM t") match {
      case Right(s: SingleSearch) => s.update(Some(t))
      case other                  => fail(s"$other")
    }
    val level = windowed.windowFunctions.flatMap(_.buckets).head
    MultiSearch.partitionType(level).sqlType shouldBe SQLTypes.Any
    MultiSearch.levelType(windowed, level).sqlType shouldBe SQLTypes.Any
    MultiSearch.bucketType(level).sqlType shouldBe SQLTypes.Int
    val grouped = Parser("SELECT c, COUNT(*) AS n FROM t GROUP BY c") match {
      case Right(s: SingleSearch) => s.update(Some(t))
      case other                  => fail(s"$other")
    }
    val key = grouped.groupBy.get.buckets.head
    MultiSearch.levelType(grouped, grouped.buckets.head).sqlType shouldBe SQLTypes.Int
    MultiSearch.bucketType(key).sqlType shouldBe SQLTypes.Int
  }

  private def resolve(sql: String, t: SchemaTable): SingleSearch =
    Parser(sql) match {
      case Right(s: SingleSearch) => s.update(Some(t))
      case other                  => fail(s"[$sql] $other")
    }

  /** What Elasticsearch is SENT for the statement's quoted numbers: a bare column's term, range,
    * terms and BETWEEN bounds, and each GROUP BY level's key filter for `'1.5'`.
    */
  private def sent(sql: String, t: SchemaTable): Seq[Any] = {
    val s = resolve(sql, t)
    (s.where.flatMap(_.criteria) match {
      case Some(e: GenericExpression) => Seq(e.queryText(e.value))
      case Some(in: InExpr[_, _])     => in.queryValues
      case Some(between: BetweenExpr) =>
        Seq(between.boundQueryText(between.fromTo.from), between.boundQueryText(between.fromTo.to))
      case None  => Nil
      case other => fail(s"[$sql] $other")
    }) ++ s.groupBy.toSeq.flatMap(_.buckets).map(_.keyFilterTexts("1.5"))
  }

  // SCHEMA-5 (R-54) x D7: a quoted number beside a whole column is the whole number it rounds to,
  // and that number is what Elasticsearch is sent -- a term, a range, a terms query, a BETWEEN, a
  // HAVING key filter. An `ARRAY<X>` column is matched per element, so it sends what its X twin
  // sends: sent as written, `'1.5'` matched no element of an integer field (term) or the group 1
  // (terms include, the floor of 1.5) where the twin keeps 2
  it should "send a quoted fractional number to Elasticsearch as its twin does (R-54)" in {
    for (x <- Seq(SQLTypes.TinyInt, SQLTypes.SmallInt, SQLTypes.Int, SQLTypes.BigInt)) {
      for (
        sql <- Seq(
          "SELECT g FROM t WHERE c = '1.5'",
          "SELECT g FROM t WHERE c > '1.5'",
          "SELECT g FROM t WHERE c IN ('1.5', '3')",
          "SELECT g FROM t WHERE c BETWEEN '1.5' AND '4.5'",
          "SELECT c, COUNT(*) AS n FROM t GROUP BY c"
        )
      ) withClue(s"[${x.typeId}] [$sql] ") {
        val twin = sent(sql, schema(x))
        twin should not be empty
        sent(sql, schema(SQLTypes.Array(x))) shouldBe twin
      }
    }
    val ints = schema(SQLTypes.Array(SQLTypes.Int))
    sent("SELECT g FROM t WHERE c = '1.5'", ints) shouldBe Seq(Some("2"))
    sent("SELECT c, COUNT(*) AS n FROM t GROUP BY c", ints) shouldBe Seq(Set("2"))
  }

  // R-66 -- outside a query the element view is off: an ingest processor and a transform read the
  // whole value (`ctx.<c>`), and the validation of a computed column judges that processor. So
  // ROUND over an `ARRAY<X>` column renders and reports there what it does over an untyped column,
  // the reading 0.24.0 gave every list column
  it should "keep ROUND over an ARRAY<X> column untyped outside a query (R-66)" in {
    val processor = Some(PainlessContext(context = PainlessContextType.Processor))
    for (
      x <- Seq(SQLTypes.Int, SQLTypes.BigInt, SQLTypes.Real, SQLTypes.Double);
      e <- Seq("ROUND(c)", "ROUND(c, 1)", "ROUND(c, -1)")
    ) {
      val sql = s"SELECT $e AS r FROM t"
      def item(t: SchemaTable): Field = resolve(sql, t).select.fields.head
      withClue(s"[${x.typeId}] [$e] ") {
        val list = item(schema(SQLTypes.Array(x)))
        val untyped = item(schema(SQLTypes.Any))
        val listContext = PainlessContext(context = PainlessContextType.Processor)
        val untypedContext = PainlessContext(context = PainlessContextType.Processor)
        s"$listContext${list.painless(Some(listContext))}" shouldBe
        s"$untypedContext${untyped.painless(Some(untypedContext))}"
        def inProcessor(f: Field): SQLType =
          SQLTypeUtils.renderingIn(processor)(f.identifier.reportedType)
        inProcessor(item(schema(SQLTypes.Array(x)))) shouldBe inProcessor(untyped)
        SQLTypeUtils.validatingComputedColumns(
          item(schema(SQLTypes.Array(x))).identifier.reportedType
        ) shouldBe SQLTypes.Numeric
        // in a query, the twin's
        item(schema(SQLTypes.Array(x))).identifier.reportedType shouldBe
        item(schema(x)).identifier.reportedType
      }
    }
  }
}
